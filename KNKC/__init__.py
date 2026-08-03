from __future__ import annotations
import json
import re
from http.cookiejar import CookieJar
from typing import Optional
import click
from lxml import html as lxml_html
from unshackle.core.credential import Credential
from unshackle.core.manifests import HLS
from unshackle.core.service import Service
from unshackle.core.titles import Episode, Movie, Movies, Series, Title_T, Titles_T
from unshackle.core.tracks import Chapters, Subtitle, Tracks


class KNKC(Service):
    """
    Service code for Knowledge Kids (https://www.knowledgekids.ca)
    www.nostalgic.cc
    Authorization: None
    """

    ALIASES = ("KNKC", "knowledgekids")
    GEOFENCE = ("CA",)

    TITLE_RE = (
        r"^(?:https?://(?:www\.)?knowledgekids\.ca)?/?(?:videos/)?"
        r"(?P<show>[a-z0-9-]+)"
        r"(?:/(?:s(?P<season>\d+)/)?e(?P<episode>\d+)/(?P<slug>[a-z0-9-]+)"
        r"|/(?P<special>[a-z0-9-]+))?/?$"
    )
    JWPLAYER_ID_RE = re.compile(r'"jwPlayerId"\s*:\s*"([A-Za-z0-9]{6,12})"')
    NODE_ID_RE = re.compile(r'"currentPath"\s*:\s*"node\\?/(\d+)"')
    EPISODE_PATH_RE = re.compile(r"^/videos/(?P<show>[a-z0-9-]+)/(?:s(?P<season>\d+)/)?e(?P<episode>\d+)/")

    @staticmethod
    @click.command(name="KNKC", short_help="https://www.knowledgekids.ca")
    @click.argument("title", type=str)
    @click.pass_context
    def cli(ctx, **kwargs):
        return KNKC(ctx, **kwargs)

    def __init__(self, ctx, title: str):
        self.title = title
        self._pages: dict[str, str] = {}
        super().__init__(ctx)

        m = re.match(self.TITLE_RE, self.title.strip(), re.IGNORECASE)
        if not m:
            self.log.error(f" - Could not parse a Knowledge Kids URL from: {self.title!r}")
            raise SystemExit(1)
        self.show = m.group("show")
        self.is_show = not (m.group("slug") or m.group("special"))
        tail = "/".join(p for p in (m.group("show"), m.group("season") and f"s{m.group('season')}",
                                    m.group("episode") and f"e{m.group('episode')}",
                                    m.group("slug") or m.group("special")) if p)
        self.path = f"/videos/{tail}"

    def authenticate(self, cookies: Optional[CookieJar] = None, credential: Optional[Credential] = None) -> None:
        super().authenticate(cookies, credential)
        if cookies:
            self.session.cookies.update(cookies)
        self.session.headers.update({
            "User-Agent": self.config["user_agent"],
            "Referer": f"{self.config['endpoints']['base']}/",
        })

    def _page(self, path: str) -> str:
        if path not in self._pages:
            resp = self.session.get(f"{self.config['endpoints']['base']}{path}")
            if resp.status_code != 200:
                self.log.error(f" - Could not load {path} (HTTP {resp.status_code}).")
                raise SystemExit(1)
            self._pages[path] = resp.text
        return self._pages[path]

    def get_titles(self) -> Titles_T:
        if self.is_show:
            return self._series_titles()

        html = self._page(self.path)
        m = self.EPISODE_PATH_RE.match(self.path)
        name, series = self._page_titles(html)
        if not m:
            return Movies([Movie(
                id_=self.path, service=self.__class__,
                name=name or series or self.show, language="en", data={"path": self.path},
            )])
        return Series([Episode(
            id_=self.path, service=self.__class__,
            title=series or self.show.replace("-", " ").title(),
            season=int(m.group("season") or 0),
            number=int(m.group("episode")),
            name=name, language="en", data={"path": self.path},
        )])

    def _series_titles(self) -> Series:
        tree = lxml_html.fromstring(self._page(self.path))
        episodes: list[Episode] = []
        seen: set[str] = set()

        for article in tree.xpath('//article[@data-type="episode"]'):
            href = next((h for h in article.xpath('.//a/@href') if self.EPISODE_PATH_RE.match(h)), None)
            if not href or href in seen:
                continue
            seen.add(href)
            m = self.EPISODE_PATH_RE.match(href)
            episodes.append(Episode(
                id_=href,
                service=self.__class__,
                title=self._series_from_data_name(article.get("data-name")) or self.show.replace("-", " ").title(),
                season=int(m.group("season") or 0),
                number=int(m.group("episode")),
                name=self._episode_from_data_name(article.get("data-name")),
                language="en",
                data={"path": href},
            ))

        if not episodes:
            self.log.error(f" - No episodes found on {self.path}.")
            raise SystemExit(1)
        return Series(episodes)

    @staticmethod
    def _series_from_data_name(data_name: Optional[str]) -> Optional[str]:
        return (data_name or "").split(" - ")[0].strip() or None

    @staticmethod
    def _episode_from_data_name(data_name: Optional[str]) -> Optional[str]:
        parts = [p.strip() for p in (data_name or "").split(" - ")]
        return parts[-1] if len(parts) > 1 else None

    def _page_titles(self, html: str) -> tuple[Optional[str], Optional[str]]:
        meta = re.search(r'"metaTitle"\s*:\s*"([^"]+)"', html)
        program = re.search(r'"programTitle"\s*:\s*"([^"]+)"', html)
        name = None
        if meta:
            parts = [p.strip() for p in meta.group(1).split(" - ")]
            name = parts[-1] if len(parts) > 1 else parts[0]
        return name, (program.group(1) if program else None)

    def get_tracks(self, title: Title_T) -> Tracks:
        path = (title.data or {}).get("path") or str(title.id)
        html = self._page(path)

        subtitles: list[Subtitle] = []
        media_id = self._jwplayer_id(html)
        if media_id:
            manifest = self.config["endpoints"]["jw_manifest"].format(media_id=media_id)
        else:
            manifest, media_id, subtitles = self._streaming_access(html, path)

        self.log.info(f" + JW media {media_id}")
        tracks = HLS.from_url(manifest, self.session).to_tracks(title.language or "en")
        for subtitle in subtitles:
            tracks.add(subtitle)
        return tracks

    def _jwplayer_id(self, html: str) -> Optional[str]:
        m = self.JWPLAYER_ID_RE.search(html)
        return m.group(1) if m else None

    def _streaming_access(self, html: str, path: str) -> tuple[str, str, list[Subtitle]]:
        node = self.NODE_ID_RE.search(html)
        if not node:
            self.log.error(f" - No jwPlayerId and no node ID on {path}.")
            raise SystemExit(1)

        url = self.config["endpoints"]["streaming_access"].format(node_id=node.group(1))
        resp = self.session.get(url, headers={"X-Requested-With": "XMLHttpRequest"})
        if resp.status_code != 200:
            self.log.error(f" - streaming-access for node {node.group(1)} returned HTTP {resp.status_code}.")
            raise SystemExit(1)

        try:
            inner = resp.json().get("response") or ""
        except ValueError:
            inner = resp.text

        config = self._jwplayer_setup(inner)
        if not config:
            message = " ".join(lxml_html.fromstring(inner).text_content().split()) if inner.strip() else ""
            self.log.error(f" - Knowledge Kids won't serve this title: {message[:200] or 'empty response'}")
            raise SystemExit(1)

        item = next(iter(config.get("playlist") or []), {})
        media_id = item.get("mediaid")
        source = next((s.get("file") for s in item.get("sources") or [] if s.get("type") == "hls"), None)
        if not source and media_id:
            source = self.config["endpoints"]["jw_manifest"].format(media_id=media_id)
        if not source:
            self.log.error(f" - streaming-access returned no playable source for node {node.group(1)}.")
            raise SystemExit(1)

        self._warn_on_substitution(html, item.get("title"))

        subtitles = [
            Subtitle(
                id_=str(track.get("file", "")).rsplit("/", 1)[-1].split(".")[0],
                url=track["file"],
                codec=Subtitle.Codec.from_mime("vtt"),
                language=track.get("label") or "en",
                forced=False,
                sdh=False,
            )
            for track in item.get("tracks") or []
            if track.get("kind") == "captions" and track.get("file")
        ]
        return source, media_id or "?", subtitles

    def _warn_on_substitution(self, html: str, returned_title: Optional[str]) -> None:
        _, series = self._page_titles(html)
        if series and returned_title and series.lower() not in returned_title.lower():
            self.log.warning(
                f" - streaming-access returned {returned_title!r}, which doesn't match {series!r}. "
                "This title may be unavailable."
            )

    @staticmethod
    def _jwplayer_setup(inner: str) -> Optional[dict]:
        start = inner.find(".setup(")
        if start < 0:
            return None
        brace = inner.find("{", start)
        if brace < 0:
            return None
        try:
            config, _ = json.JSONDecoder().raw_decode(inner[brace:])
        except ValueError:
            return None
        return config if isinstance(config, dict) else None

    def get_chapters(self, title: Title_T) -> Chapters:
        return Chapters()