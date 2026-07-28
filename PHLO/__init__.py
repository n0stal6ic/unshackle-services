from __future__ import annotations
import base64
import json
import re
import uuid
from http.cookiejar import CookieJar
from typing import Any, Optional, Union
import click
from unshackle.core.constants import AnyTrack
from unshackle.core.credential import Credential
from unshackle.core.manifests import DASH
from unshackle.core.service import Service
from unshackle.core.titles import Episode, Movie, Movies, Series, Title_T, Titles_T
from unshackle.core.tracks import Chapter, Chapters, Tracks


class PHLO(Service):
    """
    Service code for Philo (https://www.philo.com).
    www.nostalgic.cc
    Authorization: Cookies
    Security: FHD@L3
    """

    ALIASES = ("PHLO", "philo")

    TITLE_RE = (
        r"^(?:https?://(?:www\.)?philo\.com/(?:player/)*(?:show|movie|episode)/)?"
        r"(?P<id>[A-Za-z0-9_\-=]{10,})"
    )

    @staticmethod
    @click.command(name="PHLO", short_help="https://www.philo.com", help=__doc__)
    @click.argument("title", type=str)
    @click.option("--no-ads", is_flag=True, default=False,
                  help="Skip the ad-break chapter markers.")
    @click.pass_context
    def cli(ctx, **kwargs):
        return PHLO(ctx, **kwargs)

    def __init__(self, ctx, title: str, no_ads: bool):
        super().__init__(ctx)
        self.title = title
        self.no_ads = no_ads

        if not self.config:
            self.log.error(" - config.yaml is missing or empty")
            raise SystemExit(1)

        self.timeout = self.config.get("request_timeout") or 30
        self.ccextract = bool(int(self.config.get("ccextract") or 0))
        self.player_id: Optional[str] = None
        self.session_data: dict = {}
        self._session_cache: dict[str, dict] = {}

    @property
    def player_path(self):
        return self.cache_dir / "player.json"

    def authenticate(self, cookies: Optional[CookieJar] = None, credential: Optional[Credential] = None) -> None:
        super().authenticate(cookies, credential)
        if not cookies:
            self.log.error(" - Philo needs browser cookies.")
            raise SystemExit(1)

        self.session.headers.update(self.config.get("headers") or {})

        user = self.session.get(self.config["endpoints"]["user"], timeout=self.timeout)
        if user.status_code != 200:
            self.log.error(f" - Could not read the Philo account (HTTP {user.status_code}). "
                           "The cookies may be expired.")
            raise SystemExit(1)

        subscription = self._graphql("userSubscription", {})
        if subscription:
            has_access = subscription.get("hasContentAccess")
            self.log.info(f" + Subscription: {subscription.get('state', 'Unknown')} "
                          f"(Access: {'Yes' if has_access else 'No'})")
            if has_access is False:
                self.log.warning(" - This account reports no content access.")

        self.player_id = self._register_player()
        self.log.info(" + Authenticated with Philo")

    def _register_player(self) -> str:
        cached = {}
        if self.player_path.exists():
            try:
                cached = json.loads(self.player_path.read_text(encoding="utf-8"))
            except Exception as e:
                self.log.debug(f"Could not read cached player: {e}")

        device_ident = cached.get("deviceIdent") or str(uuid.uuid4())

        profile = dict(self.config.get("player") or {})
        profile["deviceIdent"] = device_ident

        data = self._graphql("registerPlayerV2", profile)
        player = (data or {}).get("player") or {}
        player_id = player.get("id")
        if not player_id:
            self.log.error(f" - registerPlayerV2 returned no player id: {json.dumps(data)[:300]}")
            raise SystemExit(1)

        try:
            self.player_path.parent.mkdir(parents=True, exist_ok=True)
            self.player_path.write_text(
                json.dumps({"deviceIdent": device_ident, "playerId": player_id}, indent=2),
                encoding="utf-8")
        except Exception as e:
            self.log.debug(f"Could not cache player: {e}")

        return player_id

    def _graphql(self, operation: str, variables: dict) -> Optional[dict]:
        queries = self.config.get("persisted_queries") or {}
        sha = queries.get(operation)
        if not sha:
            self.log.error(f" - config.yaml has no persisted_queries.{operation}")
            raise SystemExit(1)

        res = self.session.post(
            self.config["endpoints"]["graphql"],
            json=[{
                "operationName": operation,
                "variables": variables,
                "extensions": {"persistedQuery": {"version": 1, "sha256Hash": sha}},
            }],
            timeout=self.timeout,
        )
        if res.status_code != 200:
            self.log.error(f" - GraphQL {operation} failed: HTTP {res.status_code} {res.text[:200]}")
            raise SystemExit(1)

        try:
            payload = res.json()
        except Exception as e:
            self.log.error(f" - GraphQL {operation} returned non-JSON: {e}")
            raise SystemExit(1)

        entry = (payload[0] if isinstance(payload, list) and payload else payload) or {}

        for error in entry.get("errors") or []:
            code = ((error.get("extensions") or {}).get("code") or "").upper()
            if code == "PERSISTED_QUERY_NOT_FOUND":
                self.log.error(
                    f" - Philo no longer recognises the {operation} query hash.")
                raise SystemExit(1)
            self.log.error(f" - GraphQL {operation} error: {error.get('message') or error}")
            raise SystemExit(1)

        return (entry.get("data") or {}).get(operation)

    @staticmethod
    def _extract_id(title: str) -> Optional[str]:
        match = re.search(r"/(?:show|movie|episode)/([A-Za-z0-9_\-=]+)", title)
        if match:
            return match.group(1)
        if re.fullmatch(r"[A-Za-z0-9_\-=]{10,}", title.strip()):
            return title.strip()
        return None

    @staticmethod
    def _decode_node_id(node_id: str) -> str:
        try:
            padded = node_id + "=" * (-len(node_id) % 4)
            return base64.urlsafe_b64decode(padded.encode()).decode("utf-8", errors="ignore")
        except Exception:
            return ""

    def _playback_session(self, title_id: str) -> dict:
        if title_id in self._session_cache:
            return self._session_cache[title_id]

        data = self._graphql("createPlaybackSessionV2", {
            "id": title_id,
            "playerId": self.player_id,
            "idfa": None,
            "lat": None,
            "givn": None,
            "tileGroupId": None,
            "broadcastAt": None,
            "startAtOverride": None,
            "isPreload": False,
        })
        if not data:
            self.log.error(f" - No playback session for {title_id}. The title may not be in "
                           "your plan, or the id is wrong.")
            raise SystemExit(1)

        self._session_cache[title_id] = data
        return data

    def get_titles(self) -> Titles_T:
        title_id = self._extract_id(self.title)
        if not title_id:
            self.log.error(" - Could not find a Philo title id in that URL.")
            raise SystemExit(1)

        kind = self._decode_node_id(title_id).split(":")[0] or "?"
        self.log.debug(f" + Title id {title_id} ({kind})")

        session = self._playback_session(title_id)
        node = session.get("node") or {}
        show = node.get("show") or {}
        episode = node.get("episode")

        name = show.get("title") or "Unknown"
        description = show.get("longDescription") or show.get("shortDescription")
        year = self._year(show)

        if episode:
            return Series([self._episode(title_id, node, show, episode, description, year)])

        if (show.get("type") or "").upper() == "MOVIE" or not episode:
            return Movies([
                Movie(
                    id_=title_id,
                    service=self.__class__,
                    name=name,
                    year=year,
                    description=description,
                    language=self.LANGUAGE,
                    data={"session": session},
                )
            ])

        return Series([self._episode(title_id, node, show, episode, description, year)])

    def _episode(self, title_id: str, node: dict, show: dict, episode: dict,
                 description: Optional[str], year: Optional[int]) -> Episode:
        return Episode(
            id_=title_id,
            service=self.__class__,
            title=show.get("title") or "Unknown",
            season=int(episode.get("seasonNumber") or episode.get("season") or 0),
            number=int(episode.get("episodeNumber") or episode.get("number") or 0),
            name=episode.get("title") or episode.get("name"),
            description=episode.get("longDescription") or description,
            year=year,
            language=self.LANGUAGE,
            data={"session": {"node": node}},
        )

    @staticmethod
    def _year(show: dict) -> Optional[int]:
        for key in ("movieReleaseYear", "releaseYear", "year"):
            value = show.get(key)
            if not value:
                continue
            match = re.search(r"(\d{4})", str(value))
            if match:
                return int(match.group(1))
        return None

    LANGUAGE = "en"

    def get_tracks(self, title: Title_T) -> Tracks:
        session = self._playback_session(str(title.id))

        dash_url = session.get("dashURL")
        if not dash_url:
            self.log.error(" - The playback session carried no dashURL. Philo also returns "
                           f"hlsURL/dashJSONURL: {sorted(session)}")
            raise SystemExit(1)

        self.session_data = session.get("drmProvider") or {}
        self._save_manifest(dash_url)

        windows = self._ad_windows(session)
        ad_ids = self._ad_break_ids(session)
        dropped: list[str] = []
        seen: list[str] = []

        def period_filter(period) -> bool:
            period_id = (period.get("id") or "").strip()
            start = self._period_start(period)
            seen.append(f"{period_id or '?'}@{'?' if start is None else format(start, '.3f')}")
            
            if start is not None and any(s - 0.5 <= start < e - 0.5 for s, e in windows):
                dropped.append(period_id or f"@{start:.3f}")
                return True
            if period_id and "." in period_id and period_id.split(".")[0] in ad_ids:
                dropped.append(period_id)
                return True
            return False

        tracks = DASH.from_url(url=dash_url, session=self.session).to_tracks(
            language=title.language or self.LANGUAGE,
            period_filter=period_filter if windows or ad_ids else None,
        )

        if dropped:
            self.log.info(f" + Dropped {len(dropped)} ad periods ({len(ad_ids)} ad breaks)")
        elif ad_ids and len(seen) > 1:
            self.log.warning(f" - Philo reported {len(ad_ids)} ad breaks but none of the "
                             f"{len(seen)} periods matched.")
            self.log.warning(f" - Periods seen (id@start): {', '.join(seen[:40])}")
        else:
            self.log.debug(f"Single-period MPD. (Periods: {', '.join(seen)})")

        for track in tracks.audio:
            track.language = track.language or title.language or self.LANGUAGE

        if not self.ccextract:
            for track in tracks.videos:
                self._disable_ccextractor(track)
            self.log.info(" + Closed captions disabled.")

        return tracks

    def _disable_ccextractor(self, track) -> None:
        track.closed_captions = []

        def skipped(*_: Any, **__: Any) -> None:
            self.log.debug(f"ccextractor skipped for {getattr(track, 'id', '?')} ")
            return None

        track.ccextractor = skipped

    def _save_manifest(self, dash_url: str) -> None:
        try:
            res = self.session.get(dash_url, timeout=self.timeout)
            if res.status_code != 200:
                self.log.debug(f"Could not fetch the MPD for inspection: HTTP {res.status_code}")
                return
            path = self.cache_dir / "last_manifest.mpd"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(res.text, encoding="utf-8")
            self.log.debug(f" + Saved MPD to {path}")
        except Exception as e:
            self.log.debug(f"Could not save the MPD: {e}")

    @staticmethod
    def _ad_break_ids(session: dict) -> set:
        breaks = ((session.get("manifestMetadata") or {}).get("adBreaks")) or []
        return {str(b.get("id")) for b in breaks if b.get("id") is not None}

    @staticmethod
    def _ad_windows(session: dict) -> list:
        breaks = ((session.get("manifestMetadata") or {}).get("adBreaks")) or []
        windows = []
        for ad in breaks:
            start, end = ad.get("start"), ad.get("end")
            if start is None or end is None:
                continue
            windows.append((float(start), float(end)))
        return windows

    _ISO8601 = re.compile(
        r"^P(?:(?P<d>[\d.]+)D)?T?(?:(?P<h>[\d.]+)H)?(?:(?P<m>[\d.]+)M)?(?:(?P<s>[\d.]+)S)?$")

    @classmethod
    def _period_start(cls, period) -> Optional[float]:
        raw = (period.get("start") or "").strip()
        if not raw:
            return None
        match = cls._ISO8601.match(raw)
        if not match:
            return None
        parts = match.groupdict()
        if not any(parts.values()):
            return None
        return (float(parts["d"] or 0) * 86400 + float(parts["h"] or 0) * 3600
                + float(parts["m"] or 0) * 60 + float(parts["s"] or 0))

    def get_chapters(self, title: Title_T) -> Chapters:
        if self.no_ads:
            return Chapters()

        session = self._playback_session(str(title.id))
        breaks = ((session.get("manifestMetadata") or {}).get("adBreaks")) or []

        chapters = Chapters()
        removed = 0.0
        for index, ad in enumerate(breaks, 1):
            start, end = ad.get("start"), ad.get("end")
            if start is None:
                continue
            chapters.add(Chapter(timestamp=max(0.0, float(start) - removed),
                                 name=f"Ad Break {index}"))
            if end is not None:
                removed += float(end) - float(start)
        return chapters

    def get_widevine_service_certificate(self, **_: Any) -> Optional[str]:
        return self.config.get("certificate")

    def get_widevine_license(self, *, challenge: bytes, title: Title_T,
                             track: AnyTrack = None, **_) -> Optional[Union[bytes, str]]:
        drm = self.session_data or (self._playback_session(str(title.id)).get("drmProvider") or {})

        license_url = next(
            (s.get("licenseURL") for s in (drm.get("drmSystems") or [])
             if (s.get("system") or "").upper() == "WIDEVINE" and s.get("licenseURL")),
            self.config["endpoints"].get("widevine_license"),
        )
        auth_token = drm.get("authToken")
        if not auth_token:
            self.log.error(" - The playback session carried no DRMtoday auth token.")
            raise SystemExit(1)

        res = self.session.post(
            license_url,
            data=challenge,
            headers={"x-dt-auth-token": auth_token, "content-type": "application/octet-stream"},
            timeout=self.timeout,
        )
        if res.status_code != 200:
            raise ValueError(f"Widevine licence denied: HTTP {res.status_code} {res.text[:300]}")

        try:
            payload = res.json()
        except Exception:
            return res.content

        if payload.get("status") not in (None, "OK", "SUCCESS"):
            raise ValueError(f"Widevine licence denied by DRMtoday: {json.dumps(payload)[:300]}")
        if not payload.get("license"):
            raise ValueError(f"No licence in DRMtoday response: {json.dumps(payload)[:300]}")

        return payload["license"]