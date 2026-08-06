from __future__ import annotations
import re
from collections.abc import Generator
from http.cookiejar import CookieJar
from typing import Any, Optional
import click
from unshackle.core.credential import Credential
from unshackle.core.manifests import HLS
from unshackle.core.service import Service
from unshackle.core.titles import Episode, Movie, Movies, Series, Title_T, Titles_T
from unshackle.core.tracks import Chapter, Chapters, Tracks


class KNCA(Service):
    """
    Service code for Knowledge Network (https://www.knowledge.ca)
    www.nostalgic.cc
    Authorization: None
    """

    ALIASES = ("KNCA", "knowledge", "knowledgenetwork")
    GEOFENCE = ("CA",)

    TITLE_RE = (
        r"(?:https?://(?:www\.)?knowledge\.ca/)?"
        r"(?:(?P<kind>program|watch/program|watch|collection)/)?"
        r"(?P<id>[0-9a-zA-Z][0-9a-zA-Z\-]*)"
    )
    UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)

    @staticmethod
    @click.command(name="KNCA", short_help="https://www.knowledge.ca", help=__doc__)
    @click.argument("title", type=str)
    @click.option(
        "-a", "--all", "all_",
        is_flag=True,
        default=False,
        help="Return the series over a single episode.",
    )
    @click.pass_context
    def cli(ctx, **kwargs):
        return KNCA(ctx, **kwargs)

    def __init__(self, ctx, title: str, all_: bool = False):
        super().__init__(ctx)
        self.title = title
        self.want_all = all_

        m = re.fullmatch(self.TITLE_RE, title.strip().split("?")[0].rstrip("/"))
        if not m:
            self.log.error(" - Could not parse URL/ID.")
            raise SystemExit(1)

        self.kind = m.group("kind") or ""
        self.item_id = m.group("id")
        self.episode_id: Optional[str] = (
            self.item_id if self.kind == "watch" and self.UUID_RE.match(self.item_id) else None
        )

    def authenticate(self, cookies: Optional[CookieJar] = None, credential: Optional[Credential] = None) -> None:
        super().authenticate(cookies, credential)
        self.session.headers.update({
            "accept": "*/*",
            "origin": "https://www.knowledge.ca",
            "referer": "https://www.knowledge.ca/",
            "knowledge-agent": self.config.get("knowledge_agent", "web/3.0.34"),
        })
        if cookies or credential:
            self.log.info(" + Authenticated")

    def _api(self, path: str) -> dict:
        url = f"{self.config['endpoints']['api']}{path}"
        resp = self.session.get(url)
        if resp.status_code == 404:
            self.log.error(f" - Not found on Knowledge: {path}")
            raise SystemExit(1)
        if resp.status_code == 403:
            self.log.error(" - Knowledge returned 403.")
            raise SystemExit(1)
        if resp.status_code != 200:
            self.log.error(f" - Knowledge API error on {path}: {resp.status_code} {resp.text[:200]}")
            raise SystemExit(1)
        return resp.json()

    def _program(self, program_id: str) -> dict:
        return self._api(f"/programs/{program_id}")

    def get_titles(self) -> Titles_T:
        program_id = self.item_id
        if self.episode_id:
            episode = self._api(f"/episodes/{self.episode_id}")
            program_id = episode.get("program_id") or self.item_id

        program = self._program(program_id)
        entries = list(self._iter_episodes(program))
        if not entries:
            self.log.error(f" - No episodes found for '{program.get('title') or program_id}'.")
            raise SystemExit(1)

        if self.episode_id and not self.want_all:
            entries = [e for e in entries if e[2].get("id") == self.episode_id] or entries

        if str(program.get("type") or "").lower() == "series":
            return Series([self._build_episode(program, s_num, e_num, ep) for s_num, e_num, ep in entries])
        return Movies([self._build_movie(program, ep) for _s, _e, ep in entries])

    def _iter_episodes(self, program: dict) -> Generator[tuple[int, int, dict], None, None]:
        for s_idx, season in enumerate(program.get("seasons") or [], start=1):
            s_num = self._as_int(season.get("season_number"), s_idx)
            for e_idx, episode in enumerate(season.get("episodes") or [], start=1):
                if not episode.get("jwplayer_id"):
                    self.log.warning(
                        f" - Skipping '{episode.get('title')}' - no playable media."
                    )
                    continue
                e_num = self._as_int(episode.get("episode_number"), e_idx)
                yield self._as_int(episode.get("season_number"), s_num), e_num, episode

    def _build_episode(self, program: dict, season: int, number: int, ep: dict) -> Episode:
        return Episode(
            id_=ep["id"],
            service=self.__class__,
            title=program.get("title") or ep.get("program_title") or "Unknown",
            season=season,
            number=number,
            name=ep.get("title"),
            year=self._as_int(program.get("year"), None),
            language=self.config.get("language", "en"),
            description=ep.get("description") or None,
            data=self._episode_data(program, ep),
        )

    def _build_movie(self, program: dict, ep: dict) -> Movie:
        return Movie(
            id_=ep["id"],
            service=self.__class__,
            name=program.get("title") or ep.get("title") or "Unknown",
            year=self._as_int(program.get("year"), None),
            language=self.config.get("language", "en"),
            description=program.get("description") or ep.get("description") or None,
            data=self._episode_data(program, ep),
        )

    def _episode_data(self, program: dict, ep: dict) -> dict:
        return {
            "jwplayer_id": ep.get("jwplayer_id"),
            "jwplayer_site_id": ep.get("jwplayer_site_id"),
            "duration": ep.get("duration"),
            "rating": ep.get("rating"),
            "closed_captioned": bool(ep.get("closed_captioned")),
            "described_video": bool(ep.get("described_video")),
            "opening_credits_start": ep.get("opening_credits_start"),
            "opening_credits_end": ep.get("opening_credits_end"),
            "closing_credits_start": ep.get("closing_credits_start"),
            "closing_credits_end": ep.get("closing_credits_end"),
            "program_alias": program.get("alias"),
        }

    def get_tracks(self, title: Title_T) -> Tracks:
        data = title.data if isinstance(title.data, dict) else {}
        media_id = data.get("jwplayer_id")
        if not media_id:
            self.log.error(f" - No JW Player media id for '{title}'.")
            raise SystemExit(1)

        manifest = self.config["endpoints"]["manifest"].format(media_id=media_id)
        self.log.debug(f"HLS master: {manifest}")

        language = title.language or self.config.get("language", "en")
        tracks = HLS.from_url(url=manifest, session=self.session).to_tracks(language=language)

        for sub in tracks.subtitles:
            media = ((sub.data or {}).get("hls") or {}).get("media")
            characteristics = getattr(media, "characteristics", None) or ""
            if "transcribes-spoken-dialog" in characteristics:
                sub.cc = True

        return tracks

    def get_chapters(self, title: Title_T) -> Chapters:
        data = title.data if isinstance(title.data, dict) else {}
        marks: list[tuple[float, str]] = []

        opening_start = self._as_float(data.get("opening_credits_start"))
        opening_end = self._as_float(data.get("opening_credits_end"))
        closing_start = self._as_float(data.get("closing_credits_start"))

        if opening_start is not None and opening_start >= 0:
            marks.append((opening_start, "Opening Credits"))
        if opening_end is not None and opening_end > 0:
            marks.append((opening_end, "Main"))
        if closing_start is not None and closing_start > 0:
            marks.append((closing_start, "Closing Credits"))

        if not marks:
            return Chapters()

        marks.sort()
        if 0 < marks[0][0] <= 1.0:
            marks[0] = (0.0, marks[0][1])
        elif marks[0][0] > 1.0:
            marks.insert(0, (0.0, "Start"))

        chapters, seen = [], set()
        for ts, name in marks:
            key = round(ts, 3)
            if key in seen:
                continue
            seen.add(key)
            chapters.append(Chapter(timestamp=float(ts), name=name))
        return Chapters(chapters)

    def get_widevine_service_certificate(self, **_: Any) -> None:
        return None

    def get_widevine_license(self, **_: Any) -> None:
        return None

    @staticmethod
    def _as_int(value: Any, default: Optional[int]) -> Optional[int]:
        try:
            if value is None or value == "":
                return default
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _as_float(value: Any) -> Optional[float]:
        try:
            if value is None or value == "":
                return None
            return float(value)
        except (TypeError, ValueError):
            return None