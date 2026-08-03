from __future__ import annotations
import base64
import json
import re
import time
import uuid
from http.cookiejar import CookieJar
from typing import Optional
from urllib.parse import unquote, urlsplit, urlunsplit
import click
from unshackle.core.cacher import Cacher
from unshackle.core.credential import Credential
from unshackle.core.manifests import HLS
from unshackle.core.service import Service
from unshackle.core.titles import Episode, Movie, Movies, Series, Title_T, Titles_T
from unshackle.core.tracks import Chapter, Chapters, Tracks


class FOXO(Service):
    """
    Service code for FOX One (https://www.fox.com)
    www.nostalgic.cc
    Authorization: Cookies
    """

    ALIASES = ("FOXO", "foxone")
    GEOFENCE = ("US",)

    TITLE_RE = (
        r"^(?:https?://(?:www\.)?fox\.com)?"
        r"(?:/(?:watch|detail)/(?P<kind>episode|series|season|epg|clip|movie)/)?"
        r"(?P<id>[A-Za-z0-9][\w.:-]*)"
    )
    ID_PREFIXES = {"SER": "series", "SEA": "season", "fmc-": "episode"}
    AUTH_COOKIE = "FOXKITAUTHN_WEB"

    @staticmethod
    @click.command(name="FOXO", short_help="https://www.fox.com", help=__doc__)
    @click.argument("title", type=str)
    @click.option("-wa", "--with-ads", is_flag=True, default=False,
                  help="Keep the inserted ad segments when only the ad-stitched manifest is "
                       "available. The direct manifest is preferred and is content-only.")
    @click.pass_context
    def cli(ctx, **kwargs):
        return FOXO(ctx, **kwargs)

    def __init__(self, ctx, title: str, with_ads: bool):
        self.title = title
        self.with_ads = with_ads
        self.profile = getattr(ctx.obj, "profile", None)
        self.token: Optional[str] = None
        self.token_expiry: float = 0.0
        self._playback: dict[str, dict] = {}
        super().__init__(ctx)

        m = re.match(self.TITLE_RE, self.title.strip())
        if not m:
            self.log.error(f" - Could not parse a FOX One URL: {self.title!r}")
            raise SystemExit(1)
        self.entity_id = m.group("id")
        self.kind = m.group("kind") or self._infer_kind(self.entity_id)

    def _infer_kind(self, entity_id: str) -> str:
        for prefix, kind in self.ID_PREFIXES.items():
            if entity_id.startswith(prefix):
                return kind
        return "epg" if "-program-" in entity_id else "clip"

    def authenticate(self, cookies: Optional[CookieJar] = None, credential: Optional[Credential] = None) -> None:
        super().authenticate(cookies, credential)
        if not cookies:
            raise EnvironmentError("FOX One requires cookies for authentication.")
        self.session.cookies.update(cookies)

        self.token = self._read_auth_cookie()
        self.session.headers.update({
            "User-Agent": self.config["user_agent"],
            "Accept": "application/json",
            "Origin": "https://www.fox.com",
            "Referer": "https://www.fox.com/",
        })

        try:
            self.log.debug(f"session/check: {self.session.get(self.config['endpoints']['session_check']).text}")
        except Exception as e:
            self.log.debug(f"session check failed: {e}")
        self.log.info(" + Authenticated with FOX One")

    def _read_auth_cookie(self) -> str:
        raw = next((c.value for c in self.session.cookies if c.name == self.AUTH_COOKIE and c.value), None)
        if not raw:
            self.log.error(
                f" - No '{self.AUTH_COOKIE}' cookie found. Export cookies from www.fox.com while "
                "signed in to FOX One."
            )
            raise SystemExit(1)

        if not raw.lstrip().startswith("{"):
            raw = unquote(raw)

        try:
            blob = json.loads(raw)
        except ValueError:
            self.log.error(
                f" - The '{self.AUTH_COOKIE}' cookie isn't valid JSON (Starts with {raw[:40]!r})."
            )
            raise SystemExit(1)

        token = blob.get("accessToken")
        if not token:
            self.log.error(f" - The '{self.AUTH_COOKIE}' cookie has no accessToken.")
            raise SystemExit(1)

        self.token_expiry = float(blob.get("tokenExpiration") or 0) / 1000
        if self.token_expiry and time.time() >= self.token_expiry:
            self.log.error(
                " - The access token in your cookies has expired. Reload "
                "and export cookies again."
            )
            raise SystemExit(1)
        return token

    @property
    def device_id(self) -> str:
        if getattr(self, "_device_id", None):
            return self._device_id
        xid = next((c.value for c in self.session.cookies if c.name == "xid" and c.value), None)
        if xid:
            self._device_id = xid
            return self._device_id
        cache = Cacher("FOXO").get(f"device_id_{self.profile or 'default'}")
        if cache and cache.data:
            self._device_id = cache.data
        else:
            self._device_id = str(uuid.uuid4())
            cache.set(self._device_id, int(time.time()) + 60 * 60 * 24 * 3650)
        return self._device_id

    def _api(self, path: str) -> dict:
        headers = {
            "x-fox-apikey": self.config["api_key"],
            "x-fox-client": self.config["client"],
            "x-fox-userauth": f"Bearer {self.token}",
            "x-fox-api-request-id": str(uuid.uuid4()),
            "Content-Type": "application/json",
        }
        headers.update(self.config.get("geo_headers") or {})

        url = path if path.startswith("http") else f"{self.config['endpoints']['api']}{path}"
        resp = self.session.get(url, headers=headers)
        if resp.status_code == 401:
            self.log.error(" - FOX rejected the access token.")
            raise SystemExit(1)
        if resp.status_code != 200:
            self.log.error(f" - FOX API error on {url.split('?')[0]}: {resp.status_code} {resp.text[:200]}")
            raise SystemExit(1)
        body = resp.json()
        if not body.get("status", True):
            self.log.error(f" - FOX API returned status=false for {url.split('?')[0]}")
            raise SystemExit(1)
        return body.get("data") or {}

    def get_titles(self) -> Titles_T:
        if self.kind == "series":
            return self._series_titles(self.entity_id)
        if self.kind == "season":
            return Series(self._episodes_for_season(self.entity_id))
        if self.kind == "episode":
            entity = self._api(self.config["endpoints"]["entity"].format(entity_id=self.entity_id))
            return Series([self._build_episode(entity)])
        return Movies([self._build_movie(self.entity_id)])

    def _series_titles(self, series_id: str) -> Series:
        page = self._api(self.config["endpoints"]["series"].format(entity_id=series_id))
        seasons_uri = next(
            (c.get("uri") for c in page.get("containers") or []
             if c.get("container_layout_type") == "D2C_LR_SEASON_WIDGET"),
            None,
        )
        if not seasons_uri:
            self.log.error(f" - No season list on the series page for {series_id}.")
            raise SystemExit(1)

        episodes: list[Episode] = []
        for season in self._api(seasons_uri).get("items") or []:
            episodes.extend(self._episodes_for_season(season.get("entity_id"), season.get("episodes_uri")))
        if not episodes:
            self.log.error(f" - No episodes found for {series_id}.")
            raise SystemExit(1)
        return Series(episodes)

    def _episodes_for_season(self, season_id: Optional[str], episodes_uri: Optional[str] = None) -> list[Episode]:
        if not season_id and not episodes_uri:
            return []
        uri = episodes_uri or self.config["endpoints"]["episodes"].format(season_id=season_id)
        data = self._api(uri)
        return [self._build_episode(item) for item in data.get("items") or [] if item.get("entity_id")]

    def _build_episode(self, item: dict) -> Episode:
        return Episode(
            id_=item["entity_id"],
            service=self.__class__,
            title=item.get("series_name"),
            season=item.get("season_number"),
            number=item.get("episode_number"),
            name=item.get("title"),
            year=self._year(item.get("original_air_date")),
            language="en",
            data=item,
        )

    def _build_movie(self, entity_id: str) -> Movie:
        try:
            item = self._api(self.config["endpoints"]["entity"].format(entity_id=entity_id))
        except SystemExit:
            item = {}
        asset = item or self._request_playback(entity_id, live=self.kind == "epg").get("asset") or {}
        return Movie(
            id_=entity_id,
            service=self.__class__,
            name=asset.get("title") or asset.get("name") or entity_id,
            year=self._year(asset.get("original_air_date") or asset.get("originalAirDate")),
            language="en",
            data=asset,
        )

    def _platform_location(self) -> str:
        location = self.config.get("platform_location") or {"country": "US"}
        return base64.b64encode(json.dumps(location, separators=(",", ":")).encode()).decode()

    def _request_playback(self, asset_id: str, live: bool = False) -> dict:
        if asset_id in self._playback:
            return self._playback[asset_id]

        device = self.config["device"]
        endpoint = self.config["endpoints"]["watchlive" if live else "watchvod"]
        resp = self.session.post(
            f"{self.config['endpoints']['playback']}{endpoint}",
            headers={
                "x-api-key": self.config["api_key"],
                "x-access-token": f"Bearer {self.token}",
                "x-device-capabilities": "drm/widevine",
                "x-platform-location": self._platform_location(),
                "Content-Type": "application/json",
                "Accept": "*/*",
            },
            json={
                "asset": {"id": asset_id},
                "stream": {"type": "live" if live else "vod"},
                "device": {
                    "capabilities": ["drm/widevine"],
                    "model": device["model"],
                    "width": device["width"],
                    "height": device["height"],
                    "os": device["os"],
                    "osv": device["osv"],
                },
                "ad": {
                    "did": self.device_id,
                    "customParams": {"xid": self.device_id, "isNationSku": False, "isNationSub": False},
                    "capabilities": ["ssai"],
                },
                "debug": {"traceId": ""},
                "privacy": {"us": "1NNY", "lat": True},
            },
        )
        if resp.status_code == 403:
            self.log.error(f" - Playback refused for {asset_id} (403).")
            raise SystemExit(1)
        if resp.status_code != 200:
            self.log.error(f" - Playback request failed: {resp.status_code} {resp.text[:250]}")
            raise SystemExit(1)

        playback = resp.json()
        if not (playback.get("stream") or {}).get("playbackUrl"):
            self.log.error(f" - Playback response for {asset_id} carried no playbackUrl.")
            raise SystemExit(1)
        self._playback[asset_id] = playback
        return playback

    def get_tracks(self, title: Title_T) -> Tracks:
        live = self.kind == "epg"
        playback = self._request_playback(str(title.id), live=live)
        stream = playback["stream"]

        self.log.info(f" + Stream: {stream.get('maxRes', '?')} via {stream.get('cdn', '?')} ({stream.get('platform', '?')})")

        manifest_url = self._direct_manifest(stream["playbackUrl"])
        stitched = manifest_url == stream["playbackUrl"]
        tracks = HLS.from_url(manifest_url, self.session).to_tracks(title.language or "en")

        if stitched and not self.with_ads:
            for track in tracks:
                track.OnSegmentFilter = self._is_ad_segment

        return tracks

    def _direct_manifest(self, url: str) -> str:
        parts = urlsplit(url)
        direct = re.sub(r"/vod/foxone/\d+/", "/vod-dr/foxone/", parts.path)
        if direct == parts.path:
            return url

        candidate = urlunsplit((parts.scheme, parts.netloc, direct, "", ""))
        try:
            resp = self.session.get(candidate, timeout=20)
            if resp.ok and "#EXT-X-STREAM-INF" in resp.text:
                self.log.info(" + Using the direct unstitched manifest")
                return candidate
            self.log.debug(f"Direct manifest returned {resp.status_code}, falling back.")
        except Exception as e:
            self.log.debug(f"Direct manifest probe failed: {e}")

        self.log.warning(" - No direct manifest.")
        return url

    def _is_ad_segment(self, segment) -> bool:
        uri = getattr(segment, "uri", "") or ""
        return any(marker in uri for marker in self.config.get("ad_segment_markers") or ["/creatives/"])

    def get_chapters(self, title: Title_T) -> Chapters:
        asset = (self._playback.get(str(title.id)) or {}).get("asset") or {}
        credits_at = asset.get("creditCuePoint")
        if not credits_at:
            return Chapters()
        return Chapters([Chapter(timestamp=0.0, name="Chapter 1"),
                         Chapter(timestamp=float(credits_at), name="Credits")])

    @staticmethod
    def _year(value: Optional[str]) -> Optional[int]:
        m = re.match(r"(\d{4})", str(value or ""))
        return int(m.group(1)) if m else None