from __future__ import annotations
import base64
import json
import re
import time
import uuid
from http.cookiejar import CookieJar
from typing import Any, Optional
from urllib.parse import urljoin
import click
from unshackle.core.credential import Credential
from unshackle.core.manifests import HLS
from unshackle.core.service import Service
from unshackle.core.titles import Episode, Movie, Movies, Series, Title_T, Titles_T
from unshackle.core.tracks import Chapters, Tracks

class ANGL(Service):
    """
    Service code for Angel (https://www.angel.com)
    www.nostalgic.cc
    Authorization: Cookies
    Security: None
    """

    ALIASES = ("angel", "angelstudios", "ANGEL")

    TITLE_RE = (
        r"^(?:https?://(?:www\.)?angel\.com/watch/)?"
        r"(?P<slug>[a-z0-9-]+)"
        r"(?:/episode/(?P<guid>[0-9a-f-]{36})"
        r"(?:/season-(?P<season>\d+))?(?:/episode-(?P<number>\d+))?"
        r"(?:/(?P<episode_slug>[a-z0-9-]+))?)?/?$"
    )

    EPISODE_RE = re.compile(
        r'\{"description":"(?P<description>(?:[^"\\]|\\.)*)",'
        r'"episodeNumber":(?P<number>\d+),'
        r'"name":"(?P<name>(?:[^"\\]|\\.)*)",'
        r'"url":"(?P<url>[^"]+)"\}'
    )
    EPISODE_URL_RE = re.compile(r"/episode/(?P<guid>[0-9a-f-]{36})/season-(?P<season>\d+)/episode-(?P<number>\d+)")
    AUDIO_BITRATE_RE = re.compile(r"(\d+)kbps")
    CONTENT_ID_RE = re.compile(r'"contentId"\s*:\s*"(?P<content_id>[0-9a-f-]{36})"')
    MASTER_RE = re.compile(
        r"https://api\.angelstudios\.com/streaming/[0-9a-f-]{36}/master\.m3u8\?[A-Za-z0-9\-._~:/?#@!$&*+,;=%]+"
    )

    @staticmethod
    @click.command(name="ANGL", short_help="https://www.angel.com", help=__doc__)
    @click.argument("title", type=str)
    @click.option("-m", "--movie", is_flag=True, default=False, help="Treat title as Movie.")
    @click.pass_context
    def cli(ctx: click.Context, **kwargs: Any) -> "ANGL":
        return ANGL(ctx, **kwargs)

    def __init__(self, ctx: click.Context, title: str, movie: bool = False) -> None:
        self.title = title
        self.forced_movie = movie
        super().__init__(ctx)

        self.token: Optional[str] = None
        self.profile_id: Optional[str] = None
        self._page_cache: dict[str, str] = {}
        self._raw_cache: dict[str, str] = {}
        self._master_cache: dict[str, str] = {}

        self.session.headers.update(
            {"User-Agent": self.config["user_agent"], "Referer": self.config["base_url"] + "/"}
        )

    def _url(self, key: str, **kwargs: Any) -> str:
        return self.config["endpoints"][key].format(
            base=self.config["base_url"],
            api=self.config["api_url"],
            playback=self.config["playback_url"],
            **kwargs,
        )

    def _page(self, path: str) -> str:
        url = urljoin(self.config["base_url"], path)
        if url in self._page_cache:
            return self._page_cache[url]

        response = self.session.get(url)
        if response.status_code == 404:
            raise click.ClickException(f"Angel has no page at {path}")
        response.raise_for_status()

        self._raw_cache[url] = response.text
        payload = response.text.replace('\\"', '"').replace("\\u0026", "&").replace("\\/", "/")
        self._page_cache[url] = payload
        return payload

    def _raw(self, path: str) -> str:
        url = urljoin(self.config["base_url"], path)
        if url not in self._raw_cache:
            self._page(path)
        return self._raw_cache.get(url, "")

    @staticmethod
    def _unescape(value: str) -> str:
        value = re.sub(r"\\(?=[&<>])", "", value)
        try:
            return json.loads('"' + value + '"')
        except ValueError:
            return value

    def _json_ld(self, path: str) -> dict:
        merged: dict = {}
        for match in re.finditer(r'<script type="application/ld\+json"[^>]*>(.*?)</script>', self._raw(path), re.S):
            try:
                data = json.loads(match.group(1))
            except ValueError:
                continue
            for item in data if isinstance(data, list) else [data]:
                if not isinstance(item, dict) or item.get("@type") not in ("Movie", "TVSeries", "TVEpisode"):
                    continue
                for key, value in item.items():
                    if value and (key not in merged or not merged[key]):
                        merged[key] = value
        return merged

    @staticmethod
    def _year(meta: dict) -> Optional[int]:
        match = re.match(r"(\d{4})", str(meta.get("datePublished") or meta.get("dateCreated") or ""))
        return int(match.group(1)) if match else None

    def authenticate(self, cookies: Optional[CookieJar] = None, credential: Optional[Credential] = None) -> None:
        super().authenticate(cookies, credential)

        if credential and not cookies:
            self.log.warning(" - ANGL signs in with cookies.")

        token = self._token_from_cookies()
        if not token:
            self.log.warning(
                " - No angel_jwt_v3 cookie, so only free titles will resolve."
            )
            return

        expires = self._claims(token).get("exp")
        if expires and expires < time.time():
            self.log.warning(" - The angel_jwt_v3 cookie has expired.")
            return

        self.token = token
        self.profile_id = self._subject(token) or str(uuid.uuid4())
        self.log.info(" + Signed in from the cookie jar")

    def _token_from_cookies(self) -> Optional[str]:
        for cookie in self.session.cookies:
            if cookie.name == "angel_jwt_v3" and cookie.value:
                return cookie.value
        return None

    @staticmethod
    def _claims(token: str) -> dict:
        try:
            payload = token.split(".")[1]
            payload += "=" * (-len(payload) % 4)
            return json.loads(base64.urlsafe_b64decode(payload))
        except Exception:
            return {}

    def _subject(self, token: str) -> Optional[str]:
        subject = self._claims(token).get("sub") or ""
        return subject.split("|")[-1] or None

    def get_titles(self) -> Titles_T:
        match = re.match(self.TITLE_RE, self.title.strip())
        if not match:
            raise click.ClickException(f"Could not parse an Angel title from {self.title!r}")

        slug = match.group("slug")
        page = self._page(f"/watch/{slug}")
        meta = self._json_ld(f"/watch/{slug}")
        name = meta.get("name") or slug.replace("-", " ").title()

        if match.group("guid"):
            guid = match.group("guid")
            listed = [e for e in self._episodes(page, slug, name) if e.id == guid]
            if listed:
                return Series(listed)

            data = {"slug": slug, "page_path": f"/watch/{slug}"}
            if match.group("season") and match.group("number"):
                return Series(
                    [
                        Episode(
                            id_=guid,
                            service=self.__class__,
                            title=name,
                            season=int(match.group("season")),
                            number=int(match.group("number")),
                            name=self._unescape(match.group("episode_slug") or "").replace("-", " ").title() or None,
                            language="en",
                            data=data,
                        )
                    ]
                )

            year = self._year(meta)
            self.log.info(f" + Title: {name} ({year or 'n/a'}) [{guid}]")
            return Movies(
                [
                    Movie(
                        id_=guid,
                        service=self.__class__,
                        name=name,
                        year=year,
                        language="en",
                        description=meta.get("description"),
                        data=data,
                    )
                ]
            )

        if meta.get("@type") == "TVSeries" and not self.forced_movie:
            episodes = self._episodes(page, slug, name)
            if not episodes:
                raise click.ClickException(f"No episodes listed on /watch/{slug}")
            self.log.info(f" + Series: {name} ({len(episodes)} episodes)")
            return Series(episodes)

        content_id = self._feature_content_id(page)
        if not content_id:
            raise click.ClickException(f"Could not find a content id on /watch/{slug}")
        year = self._year(meta)
        self.log.info(f" + Movie: {name} ({year or 'n/a'})")
        return Movies(
            [
                Movie(
                    id_=content_id,
                    service=self.__class__,
                    name=name,
                    year=year,
                    language="en",
                    description=meta.get("description"),
                    data={"slug": slug, "page_path": f"/watch/{slug}"},
                )
            ]
        )

    def _feature_content_id(self, page: str) -> Optional[str]:
        match = self.CONTENT_ID_RE.search(page)
        return match.group("content_id") if match else None

    def _episodes(self, page: str, slug: str, show_name: str) -> list[Episode]:
        episodes: list[Episode] = []
        seen: set[str] = set()

        for match in self.EPISODE_RE.finditer(page):
            url_match = self.EPISODE_URL_RE.search(match.group("url"))
            if not url_match:
                continue
            guid = url_match.group("guid")
            if guid in seen:
                continue
            seen.add(guid)

            episodes.append(
                Episode(
                    id_=guid,
                    service=self.__class__,
                    title=show_name,
                    season=int(url_match.group("season")),
                    number=int(url_match.group("number")),
                    name=self._unescape(match.group("name")),
                    language="en",
                    description=self._unescape(match.group("description")) or None,
                    data={"slug": slug, "page_path": match.group("url").replace(self.config["base_url"], "")},
                )
            )

        return episodes

    def get_tracks(self, title: Title_T) -> Tracks:
        master_url = self._master_url(title)
        self.log.debug(f"Master playlist: {master_url}")

        tracks = HLS.from_url(url=master_url, session=self.session).to_tracks(language=title.language)
        if not tracks.videos:
            raise click.ClickException("The master playlist carried no video tracks.")

        self._apply_audio_bitrates(tracks)
        return tracks

    def _apply_audio_bitrates(self, tracks: Tracks) -> None:
        for audio in tracks.audio:
            if audio.bitrate:
                continue
            media = ((audio.data or {}).get("hls") or {}).get("media")
            match = self.AUDIO_BITRATE_RE.search(getattr(media, "group_id", "") or "")
            if match:
                audio.bitrate = int(match.group(1)) * 1000

    def _master_url(self, title: Title_T) -> str:
        content_id = str(title.id)
        if content_id in self._master_cache:
            return self._master_cache[content_id]

        master = self._session_master(content_id) if self.token else None
        if not master:
            master = self._page_master(title, content_id)

        self._master_cache[content_id] = master
        return master

    def _session_master(self, content_id: str) -> Optional[str]:
        client = self.config["client"]
        headers = {
            "Accept": "application/protobuf",
            "Authorization": self.token,
            "x-angel-app-name": client["app_name"],
            "x-angel-app-version": str(client["app_version"]),
            "x-accept-language": client["language"],
            "x-viewer-profile-id": self.profile_id,
            "x-rid": str(uuid.uuid4()),
            "x-segment-anon-id": str(uuid.uuid4()),
            **{k: str(v) for k, v in client["headers"].items()},
        }

        response = self.session.get(
            self._url("video"), params={"type": "episode", "id": content_id}, headers=headers
        )
        if response.status_code == 401:
            raise click.ClickException("Angel rejected the token.")
        if not response.ok:
            self.log.debug(f"Playback session failed ({response.status_code}) for {content_id}")
            return None

        match = self.MASTER_RE.search(response.content.decode("utf-8", "replace"))
        if not match:
            self.log.debug(f"No master playlist in the playback session for {content_id}")
            return None
        return match.group(0)

    def _page_master(self, title: Title_T, content_id: str) -> str:
        data = title.data or {}
        page = self._page(data.get("page_path") or f"/watch/{data.get('slug')}")
        candidates = {
            m.group(0).split("/streaming/")[1].split("/")[0]: m.group(0) for m in self.MASTER_RE.finditer(page)
        }

        if content_id in candidates:
            self.log.warning(" - Using the public page stream.")
            return candidates[content_id]

        if not self.token:
            raise click.ClickException(
                "This title needs a logged in session cookies."
            )
        raise click.ClickException(f"Angel published no playable stream for {content_id}")

    def get_chapters(self, title: Title_T) -> Chapters:
        return Chapters()