from __future__ import annotations
import base64
import json
import re
import time
from http.cookiejar import CookieJar
from pathlib import Path
from typing import Any, Optional
import click
from unshackle.core.credential import Credential
from unshackle.core.manifests import DASH
from unshackle.core.music import MusicTrackOption
from unshackle.core.service import Service
from unshackle.core.titles import Music, Song, Titles_T
from unshackle.core.tracks import Audio, Chapters, Tracks
from unshackle.core.tracks.track import Track


class _ApiError(Exception):
    """Raised on API failure."""


class _AuthError(Exception):
    """Raised on OAuth failure."""


class TIDL(Service):
    """
    Service code for TIDAL (https://tidal.com)
    www.nostalgic.cc
    Authorization: Credentials
    Security: FLAC@L3/SL2K/None
    """

    ALIASES = ("TIDL", "tidal")
    GROUP_AUDIO_DOWNLOADS = True

    TITLE_RE = (
        r"(?:https?://(?:www\.|listen\.|desktop\.)?tidal\.com/(?:browse/)?)?"
        r"(?P<type>track|album|playlist|artist|mix)/(?P<id>[0-9a-zA-Z\-]+)"
    )

    FORMATS = {
        "LOW": ("AAC", False, False, None, "AAC 96 kb/s"),
        "HIGH": ("AAC", False, False, None, "AAC 320 kb/s"),
        "LOSSLESS": ("FLAC", True, False, "LOSSLESS", "FLAC 16-bit/44.1kHz"),
        "HI_RES_LOSSLESS": ("FLAC", True, True, "HIRES_LOSSLESS", "FLAC 24-bit ≤192kHz"),
    }
    QUALITY_MAP = {
        "LOW": "LOW", "96": "LOW",
        "HIGH": "HIGH", "320": "HIGH", "AAC": "HIGH",
        "LOSSLESS": "LOSSLESS", "FLAC": "LOSSLESS", "CD": "LOSSLESS", "16": "LOSSLESS",
        "HI_RES_LOSSLESS": "HI_RES_LOSSLESS", "HI_RES": "HI_RES_LOSSLESS",
        "HIRES": "HI_RES_LOSSLESS", "MAX": "HI_RES_LOSSLESS", "24": "HI_RES_LOSSLESS",
    }
    FALLBACK_ORDER = ["HI_RES_LOSSLESS", "LOSSLESS", "HIGH", "LOW"]
    BITRATES = {"LOW": 96000, "HIGH": 320000}
    BIT_DEPTHS = {"LOSSLESS": (16, 44100), "HI_RES_LOSSLESS": (24, None)}
    ATMOS_TAG = "DOLBY_ATMOS"
    ATMOS_PARAM = "immersiveAudio"
    ATMOS_CODEC = "EC3"
    ATMOS_CHANNELS = 6.0
    ATMOS_LABEL = "Dolby Atmos"
    PLAYBACK_INFO = "/tracks/{track_id}/playbackinfopostpaywall"
    PLAYBACK_MODE = "STREAM"
    ASSET_PRESENTATION = "FULL"
    PAGE_LIMIT = 100
    COVER_URL = "https://resources.tidal.com/images/{path}/{variant}.jpg"
    COVER_VARIANT = "origin"
    COVER_SIZES = (80, 160, 320, 640, 1280)
    MANIFEST_BTS = "application/vnd.tidal.bts"
    MANIFEST_DASH = "application/dash+xml"
    OAUTH_SCOPE = "r_usr+w_usr+w_sub"
    OAUTH_DEVICE_GRANT = "urn:ietf:params:oauth:grant-type:device_code"
    OAUTH_REFRESH_GRANT = "refresh_token"
    OAUTH_POLL_INTERVAL = 2
    OAUTH_POLL_EXPIRY = 300
    WEB_FORMATS = ["HEAACV1", "AACLC", "FLAC", "FLAC_HIRES"]
    WEB_ATMOS_FORMAT = "EAC3_JOC"
    WEB_MANIFEST_TYPE = "MPEG_DASH"
    WEB_URI_SCHEME = "DATA"
    WEB_USAGE = "PLAYBACK"
    WEB_ACCEPT = "application/vnd.api+json"
    WEB_ADAPTIVE = True
    WEB_FORMAT_OF = {
        "HI_RES_LOSSLESS": "FLAC_HIRES",
        "LOSSLESS": "FLAC",
        "HIGH": "AACLC",
        "LOW": "HEAACV1",
    }

    @staticmethod
    @click.command(name="TIDL", short_help="https://tidal.com", help=__doc__)
    @click.argument("title", type=str)
    @click.option("-q", "--quality", "quality",
                  type=click.Choice(
                      ["LOW", "HIGH", "LOSSLESS", "HI_RES_LOSSLESS",
                       "96", "320", "AAC", "FLAC", "CD", "16", "HI_RES", "HIRES", "MAX", "24"],
                      case_sensitive=False),
                  default=None,
                  help="Quality: LOW/96=AAC 96, HIGH/320=AAC 320, LOSSLESS/CD=FLAC 16/44.1, "
                       "HI_RES_LOSSLESS/MAX=FLAC 24-bit. TIDAL downgrades automatically if your "
                       "plan or the track can't serve.")
    @click.option("-s", "--source", "source",
                  type=click.Choice(["auto", "api", "web"], case_sensitive=False), default=None,
                  help="Playback source. 'auto' takes web for hi-res tracks and api otherwise. "
                       "'api' is unencrypted and needs no CDM. 'web' mirrors the tidal.com player, "
                       "but is Widevine-protected. Whichever is picked, the other is the fallback.")
    @click.option("--atmos", is_flag=True, default=False,
                  help="Request Dolby Atmos where the track offers it.")
    @click.option("--client", "client", type=str, default=None,
                  help="OAuth client to log in as, from alt_clients in config.yaml. TIDAL caps "
                       "quality per client, so if hi-res comes back as LOSSLESS try another. "
                       "Each client keeps its own cached token.")
    @click.pass_context
    def cli(ctx, **kwargs):
        return TIDL(ctx, **kwargs)

    def __init__(self, ctx, title: str, quality: Optional[str], source: Optional[str], atmos: bool,
                 client: Optional[str] = None):
        super().__init__(ctx)
        self.title = title
        self.atmos = atmos

        self.endpoints = self.config["endpoints"]
        self.client_name, self.client_id, self.client_secret = self._resolve_client(client)

        self.source = str(source or self.config.get("default_source") or "api").lower()

        raw_quality = str(quality or self.config.get("default_quality") or "HI_RES_LOSSLESS").upper()
        if raw_quality not in self.QUALITY_MAP:
            self.log.error(
                f" - Unknown quality {raw_quality!r}. Valid: {', '.join(sorted(self.QUALITY_MAP))}"
            )
            raise SystemExit(1)
        self.quality = self.QUALITY_MAP[raw_quality]

        self.access_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.country_code: str = str(self.config.get("country_code") or "US")
        self.user_id: Optional[str] = None
        self.client_device: Optional[str] = None

        self.cover_variant = self._cover_variant(self.config.get("cover_size"))

        m = re.search(self.TITLE_RE, self.title)
        if not m:
            self.log.error(" - Could not parse a TIDAL track/album/playlist/artist/mix URL.")
            raise SystemExit(1)
        self.item_type = m.group("type")
        self.item_id = m.group("id")

    def authenticate(self, cookies: Optional[CookieJar] = None, credential: Optional[Credential] = None) -> None:
        super().authenticate(cookies, credential)
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": self.config.get("user_agent", "TIDAL/3.0 (unshackle)"),
        })

        if not self.client_id or not self.client_secret:
            self.log.error(" - No client_id/client_secret in config.yaml.")
            raise SystemExit(1)

        token, refresh = self._resolve_tokens(credential)
        self.refresh_token = refresh

        if not token and refresh:
            token = self._refresh_access_token(refresh)
        if not token:
            token = self._device_code_flow()
        if not token:
            self.log.error(
                " - No TIDAL credentials. TIDAL uses OAuth tokens: provide "
                "'refresh_token:VALUE' (or 'access_token:VALUE') in unshackle.yaml credentials, "
                "set one in config.yaml, or complete the interactive device-code login."
            )
            raise SystemExit(1)

        self.access_token = token
        self.session.headers["Authorization"] = f"Bearer {self.access_token}"

        try:
            self._load_session()
        except _AuthError as e:
            if not self.refresh_token:
                self.log.error(
                    f" - TIDAL rejected the access token ({e}) and no refresh token is available. "
                    f"Provide 'refresh_token:VALUE', or delete '{self._token_cache_path}' to "
                    "re-run the device-code login."
                )
                raise SystemExit(1)
            self.log.debug("access token rejected, refreshing")
            self.access_token = self._refresh_access_token(self.refresh_token)
            self.session.headers["Authorization"] = f"Bearer {self.access_token}"
            try:
                self._load_session()
            except _AuthError as e2:
                self.log.error(f" - TIDAL rejected the refreshed token too ({e2}). Re-authenticate.")
                raise SystemExit(1)

        self.log.info(f" + Authenticated with TIDAL ({self.country_code}, user {self.user_id})")
        self._report_subscription()
        if self.source == "web":
            self.log.info(
                " + Web-player source requires CDM."
            )

    def _resolve_client(self, override: Optional[str] = None) -> tuple[str, str, str]:
        alt = {str(k): v for k, v in (self.config.get("alt_clients") or {}).items()}
        chosen = str(override or self.config.get("client") or "").strip()

        if chosen:
            if chosen not in alt:
                self.log.error(f" - config.yaml has no alt_clients.{chosen}. "
                               f"Known: {', '.join(sorted(alt)) or 'none'}")
                raise SystemExit(1)
            pair = alt[chosen]
            return chosen, str(pair[0]).strip(), str(pair[1]).strip()

        client_id = str(self.config.get("client_id") or "").strip()
        client_secret = str(self.config.get("client_secret") or "").strip()
        if client_id and client_secret:
            name = next((k for k, v in alt.items() if str(v[0]).strip() == client_id), "custom")
            return name, client_id, client_secret

        self.log.error(" - No client selected. Set `client:` to one of "
                       f"{', '.join(sorted(alt)) or '(no alt_clients)'} in config.yaml, "
                       "or give an explicit client_id and client_secret.")
        raise SystemExit(1)

    def _report_subscription(self) -> None:
        if not self.user_id:
            return
        try:
            resp = self.session.get(
                f"{self.endpoints['api']}/users/{self.user_id}/subscription",
                params={"countryCode": self.country_code},
            )
            if resp.status_code != 200:
                self.log.debug(f"subscription lookup returned {resp.status_code}")
                return
            info = resp.json()
        except Exception as e:
            self.log.debug(f"could not read subscription: {e}")
            return

        plan = str((info.get("subscription") or {}).get("type") or "Unknown")
        ceiling = self.QUALITY_MAP.get(str(info.get("highestSoundQuality") or "").upper())
        label = self.FORMATS[ceiling][4] if ceiling else "Unknown"
        self.log.info(f" + Subscription: {plan} (Up to: {label})")

        if ceiling and self.FALLBACK_ORDER.index(self.quality) < self.FALLBACK_ORDER.index(ceiling):
            self.log.warning(
                f" - Asked for {self.FORMATS[self.quality][4]} but this plan tops out at "
                f"{label}. TIDAL will downgrade."
            )

    def _resolve_tokens(self, credential: Optional[Credential]) -> tuple[Optional[str], Optional[str]]:
        access = refresh = None

        if credential:
            user = (credential.username or "").strip().lower()
            pw = (credential.password or "").strip()
            if pw:
                if user in ("refresh_token", "refresh", "token", "tidal"):
                    refresh = pw
                elif user in ("access_token", "access"):
                    access = pw
                else:
                    refresh = pw
            elif user:
                refresh = credential.username.strip()

        refresh = refresh or str(self.config.get("refresh_token") or "").strip() or None
        access = access or str(self.config.get("access_token") or "").strip() or None

        if not refresh and not access:
            cached = self._read_token_cache()
            refresh = cached.get("refresh_token")
            access = cached.get("access_token")
            expires = float(cached.get("expires_at") or 0)
            if access and expires and time.time() >= expires - 60:
                access = None

        return access, refresh

    @property
    def _token_cache_path(self) -> Path:
        return self.cache_dir / f"token_{self.client_name}.json"

    def _read_token_cache(self) -> dict:
        try:
            return json.loads(self._token_cache_path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _write_token_cache(self, data: dict) -> None:
        try:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            self._token_cache_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        except Exception as e:
            self.log.debug(f"could not cache token: {e}")

    def _refresh_access_token(self, refresh_token: str) -> str:
        resp = self.session.post(
            self.endpoints["token"],
            data={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "refresh_token": refresh_token,
                "grant_type": self.OAUTH_REFRESH_GRANT,
                "scope": self.OAUTH_SCOPE,
            },
            headers={"Authorization": None},
        )
        if resp.status_code != 200:
            self.log.error(f" - Could not refresh the TIDAL token: {self._oauth_error(resp)}")
            if "invalid_client" in (resp.text or ""):
                self.log.error(
                    f" - The '{self.client_name}' client cannot use the refresh grant. Pick a "
                    "playback-capable one with `client: tv_android` in config.yaml."
                )
            else:
                self.log.error(f" - Delete '{self._token_cache_path}' to log in again.")
            raise SystemExit(1)
        info = resp.json()
        token = info.get("access_token")
        if not token:
            self.log.error(f" - Token refresh returned no access_token: {info}")
            raise SystemExit(1)
        self.refresh_token = info.get("refresh_token") or refresh_token
        self._write_token_cache({
            "access_token": token,
            "refresh_token": self.refresh_token,
            "expires_at": time.time() + float(info.get("expires_in") or 3600),
        })
        return token

    def _device_code_flow(self) -> Optional[str]:
        resp = self.session.post(
            self.endpoints["device_authorization"],
            data={"client_id": self.client_id, "scope": self.OAUTH_SCOPE},
            headers={"Authorization": None},
        )
        if resp.status_code != 200:
            self.log.error(f" - Device authorization failed ({resp.status_code}): {resp.text[:200]}")
            return None
        info = resp.json()
        device_code = info.get("deviceCode")
        user_code = info.get("userCode")
        verify = info.get("verificationUriComplete") or info.get("verificationUri")
        if not device_code or not user_code:
            self.log.error(f" - Unexpected device authorization response: {info}")
            return None
        if verify and not str(verify).startswith("http"):
            verify = f"https://{verify}"

        interval = int(info.get("interval") or self.OAUTH_POLL_INTERVAL)
        expires_in = int(info.get("expiresIn") or self.OAUTH_POLL_EXPIRY)

        self.log.info(f" + Open {verify}")
        self.log.info(f" + Enter: {user_code}")
        self.log.info(f" + Waiting for authorization. (Expires: {expires_in}s)...")

        deadline = time.time() + expires_in
        while time.time() < deadline:
            time.sleep(interval)
            poll = self.session.post(
                self.endpoints["token"],
                data={
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "device_code": device_code,
                    "grant_type": self.OAUTH_DEVICE_GRANT,
                    "scope": self.OAUTH_SCOPE,
                },
                headers={"Authorization": None},
            )
            if poll.status_code == 200:
                data = poll.json()
                token = data.get("access_token")
                if not token:
                    self.log.error(f" - Device token response had no access_token: {data}")
                    return None
                self.refresh_token = data.get("refresh_token")
                self._write_token_cache({
                    "access_token": token,
                    "refresh_token": self.refresh_token,
                    "expires_at": time.time() + float(data.get("expires_in") or 3600),
                })
                self.log.info(" + Authorized.")
                return token
            try:
                err = poll.json().get("error", "")
            except Exception:
                err = ""
            if err in ("authorization_pending", "slow_down"):
                if err == "slow_down":
                    interval += 1
                continue
            if err == "expired_token":
                self.log.error(" - The code expired.")
                return None
            self.log.error(f" - Device authorization failed: {self._oauth_error(poll)}")
            return None

        self.log.error(" - Device authorization timed out.")
        return None

    ENTITLEMENT_SUBSTATUS = {4005, 4006}

    def _entitlement_refusal(self, resp: Any) -> Optional[str]:
        try:
            body = resp.json()
        except Exception:
            return None
        if not isinstance(body, dict):
            return None
        sub_status = body.get("subStatus")
        if sub_status not in self.ENTITLEMENT_SUBSTATUS:
            return None
        message = body.get("userMessage") or "asset refused"
        return (f"{message} (subStatus {sub_status}). The '{self.client_name}' client is not "
                "allowed to open a playback session for this track.")

    @staticmethod
    def _oauth_error(resp: Any) -> str:
        try:
            body = resp.json()
            detail = body.get("error_description") or body.get("error") or body
            return f"{resp.status_code} {detail}"
        except Exception:
            text = (resp.text or "").strip()
            if text[:1] == "<":
                return (f"{resp.status_code} (Blocked by TIDAL's edge")
            return f"{resp.status_code} {text[:150]}"

    def _load_session(self) -> None:
        resp = self.session.get(self.endpoints["sessions"])
        if resp.status_code in (401, 403):
            raise _AuthError(f"{resp.status_code} on /sessions")
        if resp.status_code != 200:
            self.log.error(f" - Could not read TIDAL session: {resp.status_code} {resp.text[:200]}")
            raise SystemExit(1)
        info = resp.json()
        self.country_code = info.get("countryCode") or self.country_code
        self.user_id = str(info.get("userId") or "") or None
        raw = str((info.get("client") or {}).get("name") or "")
        self.client_device = re.sub(r"^\d+_\d+_", "", raw).strip() or None
        if self.client_device:
            self.log.debug(f"Client '{self.client_name}' registers as: {self.client_device}")

    def _api(self, path: str, params: Optional[dict] = None, _retried: bool = False,
             soft: bool = False) -> dict:
        query = {"countryCode": self.country_code}
        query.update(params or {})
        resp = self.session.get(f"{self.endpoints['api']}{path}", params=query)

        if resp.status_code in (401, 403):
            entitlement = self._entitlement_refusal(resp)
            if entitlement:
                if soft:
                    raise _ApiError(entitlement)
                self.log.error(f" - {entitlement}")
                raise SystemExit(1)
            if not _retried and self.refresh_token:
                self.log.debug(f"{resp.status_code} on {path}, refreshing token")
                self.access_token = self._refresh_access_token(self.refresh_token)
                self.session.headers["Authorization"] = f"Bearer {self.access_token}"
                return self._api(path, params, _retried=True, soft=soft)
            if soft:
                raise _ApiError(f"{resp.status_code} on {path}. This plan cannot stream.")
            raise _AuthError(f"{resp.status_code} on {path}")

        if resp.status_code == 404:
            if soft:
                raise _ApiError(f"nothing playable at {path}")
            self.log.error(f" - TIDAL has no such item ({path}). Check the URL and your region.")
            raise SystemExit(1)
        if resp.status_code != 200:
            if soft:
                raise _ApiError(f"{path} returned {resp.status_code}: {resp.text[:120]}")
            self.log.error(f" - TIDAL API error on {path}: {resp.status_code} {resp.text[:200]}")
            raise SystemExit(1)
        return resp.json()

    def _api_paged(self, path: str, params: Optional[dict] = None) -> list[dict]:
        limit = self.PAGE_LIMIT
        items: list[dict] = []
        offset = 0
        while True:
            page = self._api(path, {**(params or {}), "limit": limit, "offset": offset})
            batch = page.get("items") or []
            items.extend(batch)
            total = page.get("totalNumberOfItems")
            offset += len(batch)
            if not batch or (total is not None and offset >= int(total)) or len(batch) < limit:
                break
        return items

    def get_titles(self) -> Titles_T:
        if self.item_type == "track":
            return self._titles_from_track()
        if self.item_type == "album":
            return self._titles_from_album()
        if self.item_type == "playlist":
            return self._titles_from_playlist()
        if self.item_type == "mix":
            return self._titles_from_mix()
        return self._titles_from_artist()

    def _titles_from_track(self) -> Music:
        track = self._api(f"/tracks/{self.item_id}")
        song = self._build_song(track, self._album_of(track))
        return Music(
            [song], kind="single", title=song.album,
            artist=song.album_artist or song.artist, year=song.year,
            total_tracks=1, artwork_url=song.artwork_url,
        )

    def _titles_from_album(self) -> Music:
        album = self._api(f"/albums/{self.item_id}")
        items = self._api_paged(f"/albums/{self.item_id}/items")
        tracks = [self._unwrap(i) for i in items]
        songs = [self._build_song(t, album) for t in tracks if t and t.get("id")]
        if not songs:
            self.log.error(f" - No tracks found for album {self.item_id}.")
            raise SystemExit(1)
        return Music(
            songs, kind=self._release_kind(album, len(songs)),
            title=album.get("title"), artist=self._first_artist(album),
            year=self._year(album.get("releaseDate"), album.get("streamStartDate")) or None,
            total_tracks=album.get("numberOfTracks") or len(songs),
            total_discs=album.get("numberOfVolumes") or max((s.disc for s in songs), default=1),
            artwork_url=self._cover(album.get("cover")),
            total_duration=album.get("duration") or None,
        )

    def _titles_from_playlist(self) -> Music:
        pl = self._api(f"/playlists/{self.item_id}")
        items = self._api_paged(f"/playlists/{self.item_id}/items")
        songs = []
        for position, entry in enumerate(items, start=1):
            track = self._unwrap(entry)
            if not track or not track.get("id") or track.get("type") == "video":
                continue
            songs.append(self._build_song(track, self._album_of(track), playlist_position=position))
        if not songs:
            self.log.error(f" - No playable tracks found for playlist {self.item_id}.")
            raise SystemExit(1)
        creator = (pl.get("creator") or {}).get("name") or pl.get("profileName")
        return Music(
            songs, kind="playlist", title=pl.get("title"),
            artist=creator or None, total_tracks=len(songs), owner=creator or None,
            artwork_url=(self._cover(pl.get("squareImage") or pl.get("image"))
                         or songs[0].artwork_url),
            description=(pl.get("description") or None),
        )

    def _titles_from_mix(self) -> Music:
        items = self._api_paged(f"/mixes/{self.item_id}/items")
        songs = []
        for position, entry in enumerate(items, start=1):
            track = self._unwrap(entry)
            if not track or not track.get("id") or track.get("type") == "video":
                continue
            songs.append(self._build_song(track, self._album_of(track), playlist_position=position))
        if not songs:
            self.log.error(f" - No playable tracks found for mix {self.item_id}.")
            raise SystemExit(1)
        return Music(songs, kind="playlist", title=f"TIDAL Mix {self.item_id}",
                     artist=songs[0].artist, total_tracks=len(songs),
                     artwork_url=songs[0].artwork_url)

    def _titles_from_artist(self) -> Music:
        artist = self._api(f"/artists/{self.item_id}")
        items = self._api_paged(f"/artists/{self.item_id}/toptracks")
        songs = []
        for position, entry in enumerate(items, start=1):
            track = self._unwrap(entry)
            if not track or not track.get("id"):
                continue
            songs.append(self._build_song(track, self._album_of(track), playlist_position=position))
        if not songs:
            self.log.error(f" - No top tracks found for artist {self.item_id}.")
            raise SystemExit(1)
        name = artist.get("name") or songs[0].artist
        return Music(songs, kind="playlist", title=f"{name} - Top Tracks",
                     artist=name, total_tracks=len(songs),
                     artwork_url=self._cover(artist.get("picture")) or songs[0].artwork_url)

    @staticmethod
    def _unwrap(entry: Any) -> dict:
        if not isinstance(entry, dict):
            return {}
        if isinstance(entry.get("item"), dict):
            return entry["item"]
        return entry

    @staticmethod
    def _album_of(track: dict) -> dict:
        return (track or {}).get("album") or {}

    def _build_song(self, track: dict, album: dict, playlist_position: Optional[int] = None) -> Song:
        album = album or {}
        title = (track.get("title") or "Unknown").strip()
        if track.get("version"):
            title = f"{title} ({str(track['version']).strip()})"
        artist = self._first_artist(track) or self._first_artist(album) or "Unknown Artist"
        album_title = (album.get("title") or title).strip()
        album_artist = self._first_artist(album) or artist
        artwork = self._cover(album.get("cover"))
        tags = [str(t).upper() for t in ((track.get("mediaMetadata") or {}).get("tags") or [])]
        modes = [str(m).upper() for m in (track.get("audioModes") or [])]

        data = {
            "service": self.ALIASES[0],
            "track_id": track.get("id"),
            "album_id": album.get("id"),
            "duration": int(track.get("duration") or 0),
            "tags": tags,
            "audio_modes": modes,
            "audio_quality": track.get("audioQuality"),
            "replay_gain": track.get("replayGain"),
            "peak": track.get("peak"),
            "artwork_url": artwork,
            "release_date": str(album.get("releaseDate") or "").strip() or None,
            "track_url": f"https://tidal.com/browse/track/{track['id']}" if track.get("id") else None,
            "album_url": f"https://tidal.com/browse/album/{album['id']}" if album.get("id") else None,
        }
        return Song(
            id_=track.get("id"),
            service=self.__class__,
            name=title,
            artist=artist,
            album=album_title,
            track=int(playlist_position or track.get("trackNumber") or 1),
            disc=int(track.get("volumeNumber") or 1),
            year=self._year(album.get("releaseDate"), track.get("streamStartDate"),
                            album.get("streamStartDate")),
            album_artist=album_artist,
            release_type=self._release_kind(album, None),
            total_tracks=int(album["numberOfTracks"]) if album.get("numberOfTracks") else None,
            total_discs=int(album["numberOfVolumes"]) if album.get("numberOfVolumes") else None,
            explicit=bool(track.get("explicit")),
            isrc=(track.get("isrc") or "").strip() or None,
            upc=(album.get("upc") or "").strip() or None,
            copyright=(track.get("copyright") or album.get("copyright") or "").strip() or None,
            artwork_url=artwork,
            data=data,
        )

    def get_music_track_options(self, song: Song) -> list[MusicTrackOption]:
        data = song.data if isinstance(song.data, dict) else {}
        tags = data.get("tags") or []
        modes = data.get("audio_modes") or []
        fmt = self._effective_quality(tags)
        codec, lossless, hires, _tag, label = self.FORMATS[fmt]

        if self.atmos and self.ATMOS_TAG in modes:
            option = MusicTrackOption(codec=self.ATMOS_CODEC, channels=self.ATMOS_CHANNELS,
                                      lossless=False, atmos=True)
            option.quality_label = self.ATMOS_LABEL
        elif lossless:
            bit_depth, sample_rate = self.BIT_DEPTHS.get(fmt, (None, None))
            option = MusicTrackOption(codec=codec, bit_depth=bit_depth, sample_rate=sample_rate,
                                      channels=2.0, lossless=True, hires=hires)
            option.quality_label = label
        else:
            option = MusicTrackOption(codec=codec, bitrate=self.BITRATES.get(fmt),
                                      channels=2.0, lossless=False)
            option.quality_label = label

        option.explicit = bool(song.explicit)
        option.duration = int(data.get("duration")) if data.get("duration") else None
        return [option]

    def _effective_quality(self, tags: list[str]) -> str:
        if self.quality not in self.FALLBACK_ORDER:
            return self.quality
        for fmt in self.FALLBACK_ORDER[self.FALLBACK_ORDER.index(self.quality):]:
            required_tag = self.FORMATS[fmt][3]
            if not required_tag or required_tag in tags:
                return fmt
        return self.FALLBACK_ORDER[-1]

    def get_tracks(self, song: Song) -> Tracks:
        data = song.data if isinstance(song.data, dict) else {}
        wanted = self._effective_quality(data.get("tags") or [])
        order = self._source_order(song)

        failures: list[str] = []
        best: Optional[tuple[int, Tracks]] = None

        for index, source in enumerate(order):
            remaining = order[index + 1:]
            try:
                tracks = self._tracks_web(song) if source == "web" else self._tracks_api(song)
            except _ApiError as e:
                failures.append(f"{source} ({e})")
                self.log.warning(f" - The {source} source could not serve this track: {e}")
                if remaining:
                    note = (", which is Widevine-protected and needs a CDM"
                            if remaining[0] == "web" else "")
                    self.log.warning(f" - Falling back to the {remaining[0]} source{note}.")
                continue

            achieved = self._achieved_quality(tracks) or wanted
            rank = (self.FALLBACK_ORDER.index(achieved) if achieved in self.FALLBACK_ORDER
                    else len(self.FALLBACK_ORDER))
            if best is None or rank < best[0]:
                best = (rank, tracks)

            if achieved == wanted or not remaining:
                return best[1]

            self.log.warning(
                f" - The {source} source only offered {self._label(achieved)}; trying "
                f"{remaining[0]} for {self._label(wanted)}."
            )

        if best is not None:
            return best[1]
        self.log.error(f" - No source could serve this track. Tried: {'; '.join(failures)}.")
        raise SystemExit(1)

    def _label(self, quality: str) -> str:
        return self.FORMATS[quality][4] if quality in self.FORMATS else quality

    def _achieved_quality(self, tracks: Tracks) -> Optional[str]:
        for track in tracks:
            value = str((getattr(track, "data", None) or {}).get("tidl_quality") or "").upper()
            if not value:
                continue
            if value in self.FORMATS:
                return value
            for quality, web_format in self.WEB_FORMAT_OF.items():
                if web_format == value:
                    return quality
        return None

    def _source_order(self, song: Song) -> list[str]:
        if self.source in ("api", "web"):
            return [self.source, "web" if self.source == "api" else "api"]
        return ["api", "web"]

    def _tracks_api(self, song: Song) -> Tracks:
        track_id = str(song.id)
        data = song.data if isinstance(song.data, dict) else {}
        tags = data.get("tags") or []
        modes = data.get("audio_modes") or []
        requested = self._effective_quality(tags)

        params = {
            "audioquality": requested,
            "playbackmode": self.PLAYBACK_MODE,
            "assetpresentation": self.ASSET_PRESENTATION,
        }
        if self.atmos and self.ATMOS_TAG in modes:
            params[self.ATMOS_PARAM] = "true"

        stream = self._api(self.PLAYBACK_INFO.format(track_id=track_id), params, soft=True)

        got = str(stream.get("audioQuality") or "").upper()
        if got and got != requested:
            required_tag = self.FORMATS[requested][3]
            if required_tag and required_tag in tags:
                device = f" ({self.client_device})" if self.client_device else ""
                self.log.warning(
                    f" - This track has a {self.FORMATS[requested][4]} master, but the "
                    f"'{self.client_name}'{device} client only served {got}. TIDAL caps this "
                    "endpoint per client."
                )
            else:
                self.log.warning(
                    f" - This track has no {self.FORMATS[requested][4]} master. Using {got}."
                )
        bit_depth, sample_rate = stream.get("bitDepth"), stream.get("sampleRate")
        if bit_depth and sample_rate:
            self.log.info(f" + Stream: {got} {bit_depth}-bit / {int(sample_rate) / 1000:.1f} kHz")
        if isinstance(song.data, dict) and got:
            song.data["quality"] = self.FORMATS.get(got, (None,) * 5)[4] or got

        urls, codecs, mime = self._parse_manifest(stream)
        if not urls:
            raise _ApiError(f"no stream URLs in the manifest for track {track_id}")

        codec = self._codec_of(codecs)
        audio_mode = str(stream.get("audioMode") or "STEREO").upper()
        extra = {
            "tidl_flac": codec == Audio.Codec.FLAC,
            "tidl_quality": got or requested,
            "tidl_mode": audio_mode,
            "tidl_source": "api",
        }

        if mime == self.MANIFEST_DASH:
            return self._tracks_from_mpd(self._decode_manifest(stream), song, track_id, extra,
                                         requested=requested, announce=False)

        if len(urls) == 1:
            audio = Audio(
                urls[0], language=song.language or "en", codec=codec,
                channels=6 if audio_mode == "DOLBY_ATMOS" else 2,
                descriptor=Track.Descriptor.URL, id_=track_id, data=extra,
            )
            return Tracks([audio])

        mpd = self._segments_to_mpd(urls, codecs, stream)
        return self._tracks_from_mpd(mpd, song, track_id, extra)

    def _tracks_web(self, song: Song) -> Tracks:
        track_id = str(song.id)
        data = song.data if isinstance(song.data, dict) else {}
        requested = self._effective_quality(data.get("tags") or [])
        url = str(self.endpoints["track_manifests"]).format(track_id=track_id)

        formats = list(self.WEB_FORMATS)
        if self.atmos and self.ATMOS_TAG in (data.get("audio_modes") or []):
            formats.append(self.WEB_ATMOS_FORMAT)

        params = [
            ("adaptive", str(self.WEB_ADAPTIVE).lower()),
            ("manifestType", self.WEB_MANIFEST_TYPE),
            ("uriScheme", self.WEB_URI_SCHEME),
            ("usage", self.WEB_USAGE),
        ]
        params += [("formats", f) for f in formats]

        headers = {"Accept": self.WEB_ACCEPT}

        resp = self.session.get(url, params=params, headers=headers)
        if resp.status_code in (401, 403) and self.refresh_token:
            self.access_token = self._refresh_access_token(self.refresh_token)
            self.session.headers["Authorization"] = f"Bearer {self.access_token}"
            resp = self.session.get(url, params=params, headers=headers)
        if resp.status_code != 200:
            raise _ApiError(
                f"web manifest for {track_id} returned {resp.status_code}: {resp.text[:120]}")

        attrs = ((resp.json() or {}).get("data") or {}).get("attributes") or {}
        presentation = str(attrs.get("trackPresentation") or "").upper()
        if presentation and presentation != "FULL":
            reason = f" ({attrs['previewReason']})" if attrs.get("previewReason") else ""
            raise _ApiError(f"the web player only offers a {presentation} of this track{reason}")

        mpd = self._resolve_manifest_uri(attrs.get("uri") or "")
        drm_data = attrs.get("drmData") or {}
        drm_system = str(drm_data.get("drmSystem") or "").upper()
        license_url = drm_data.get("licenseUrl") or self._default_license_url(drm_system)

        if attrs.get("formats"):
            self.log.debug(f"web manifest offers: {', '.join(map(str, attrs['formats']))}")
        if drm_system:
            self.log.debug(f"web player DRM: {drm_system}")

        extra = {
            "tidl_quality": "WEB",
            "tidl_mode": "STEREO",
            "tidl_source": "web",
            "tidl_license_url": license_url,
            "tidl_drm_system": drm_system,
        }
        return self._tracks_from_mpd(mpd, song, track_id, extra, requested=requested)

    def _default_license_url(self, drm_system: str) -> str:
        key = "playready_license" if drm_system == "PLAYREADY" else "widevine_license"
        return str(self.endpoints.get(key) or "")

    def _resolve_manifest_uri(self, uri: str) -> str:
        if not uri:
            self.log.error(" - Web-player manifest response had no uri.")
            raise SystemExit(1)
        if uri.startswith("data:"):
            if "base64," not in uri:
                self.log.error(" - Unsupported data URI in web-player manifest.")
                raise SystemExit(1)
            return base64.b64decode(uri.split("base64,", 1)[1]).decode("utf-8", errors="replace")
        resp = self.session.get(uri)
        if resp.status_code != 200:
            self.log.error(f" - Could not fetch the web-player manifest: {resp.status_code}")
            raise SystemExit(1)
        return resp.text

    def _tracks_from_mpd(self, mpd: str, song: Song, track_id: str, extra: dict,
                         requested: Optional[str] = None, announce: bool = True) -> Tracks:
        dash = DASH.from_text(mpd, self.endpoints["api"])
        audios = list(dash.to_tracks(language=song.language or "en").audio)
        if not audios:
            self.log.error(" - The DASH manifest contained no audio tracks.")
            raise SystemExit(1)

        audio = self._pick_audio(audios, requested)
        fmt, sample_rate, bit_depth = self._parse_rep_id(
            ((audio.data or {}).get("dash") or {}).get("representation_id")
        )

        extra = dict(extra)
        extra["tidl_flac"] = audio.codec == Audio.Codec.FLAC
        if fmt:
            extra["tidl_quality"] = fmt

        if announce and bit_depth and sample_rate:
            self.log.info(f" + Stream: {fmt} {bit_depth}-bit / {sample_rate / 1000:.1f} kHz")
        elif announce and fmt:
            self.log.info(f" + Stream: {fmt}")
        if isinstance(song.data, dict) and fmt:
            song.data["quality"] = fmt

        audio.id = track_id
        if isinstance(audio.data, dict):
            audio.data.update(extra)
        else:
            audio.data = dict(extra)
        return Tracks([audio])

    def _pick_audio(self, audios: list, requested: Optional[str]) -> Any:
        if len(audios) == 1 or not requested or requested not in self.FALLBACK_ORDER:
            return audios[0]

        by_format: dict[str, Any] = {}
        for audio in audios:
            rep_id = ((audio.data or {}).get("dash") or {}).get("representation_id")
            fmt = self._parse_rep_id(rep_id)[0]
            if fmt:
                by_format.setdefault(fmt, audio)

        if self.atmos and self.WEB_ATMOS_FORMAT in by_format:
            return by_format[self.WEB_ATMOS_FORMAT]

        for quality in self.FALLBACK_ORDER[self.FALLBACK_ORDER.index(requested):]:
            match = by_format.get(self.WEB_FORMAT_OF[quality])
            if match is None:
                continue
            if quality != requested:
                self.log.warning(
                    f" - {self.FORMATS[requested][4]} is not in this manifest. "
                    f"Using {self.FORMATS[quality][4]}."
                )
            return match

        best = max(audios, key=lambda a: int(getattr(a, "bitrate", 0) or 0))
        self.log.warning(" - No requested format in the manifest.")
        return best

    @staticmethod
    def _parse_rep_id(rep_id: Any) -> tuple[Optional[str], Optional[int], Optional[int]]:
        parts = [p.strip() for p in str(rep_id or "").split(",")]
        fmt = parts[0].upper() or None
        rate = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
        depth = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else None
        return fmt, rate, depth

    def get_chapters(self, song: Song) -> Chapters:
        return Chapters()

    @staticmethod
    def _decode_manifest(stream: dict) -> str:
        return base64.b64decode(stream.get("manifest") or "").decode("utf-8", errors="replace")

    def _parse_manifest(self, stream: dict) -> tuple[list[str], str, str]:
        mime = str(stream.get("manifestMimeType") or "")
        decoded = self._decode_manifest(stream)

        if mime == self.MANIFEST_BTS:
            try:
                body = json.loads(decoded)
            except json.JSONDecodeError as e:
                raise _ApiError(f"could not parse the BTS manifest: {e}")
            encryption = str(body.get("encryptionType") or "NONE").upper()
            if encryption not in ("NONE", ""):
                raise _ApiError(f"the stream is {encryption}-encrypted")
            return list(body.get("urls") or []), str(body.get("codecs") or ""), mime

        if mime == self.MANIFEST_DASH:
            if not re.search(r"<Representation\b", decoded):
                return [], "", mime
            return ["<dash>"], self._dash_codecs(decoded), mime

        raise _ApiError(f"unsupported manifest type {mime!r}")

    @staticmethod
    def _dash_codecs(mpd: str) -> str:
        m = re.search(r'<Representation[^>]*\bcodecs="([^"]+)"', mpd)
        return m.group(1) if m else ""

    def _codec_of(self, codecs: str) -> Audio.Codec:
        try:
            return Audio.Codec.from_codecs(codecs)
        except ValueError:
            lowered = (codecs or "").lower()
            if "flac" in lowered:
                return Audio.Codec.FLAC
            if "ac-4" in lowered or "ac4" in lowered:
                return Audio.Codec.AC4
            if "ec-3" in lowered or "eac3" in lowered:
                return Audio.Codec.EC3
            return Audio.Codec.AAC

    @staticmethod
    def _segments_to_mpd(urls: list[str], codecs: str, stream: dict) -> str:
        from xml.sax.saxutils import escape
        segments = "".join(f'<SegmentURL media="{escape(u)}"/>' for u in urls)
        rate = stream.get("sampleRate") or 44100
        return (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<MPD xmlns="urn:mpeg:dash:schema:mpd:2011" type="static" '
            'profiles="urn:mpeg:dash:profile:isoff-main:2011">'
            '<Period id="0">'
            '<AdaptationSet id="0" contentType="audio" mimeType="audio/mp4">'
            f'<Representation id="0" codecs="{escape(codecs or "flac")}" '
            f'audioSamplingRate="{int(rate)}" bandwidth="1000000">'
            f"<SegmentList>{segments}</SegmentList>"
            "</Representation></AdaptationSet></Period></MPD>"
        )

    def get_widevine_service_certificate(self, **_: Any) -> None:
        return None

    def get_widevine_license(self, *, challenge: bytes, title: Any, track: Any) -> Optional[bytes]:
        return self._license(challenge, track, "WIDEVINE")

    def get_playready_license(self, *, challenge: Any, title: Any, track: Any) -> Optional[bytes]:
        return self._license(challenge, track, "PLAYREADY")

    def _license(self, challenge: Any, track: Any, system: str) -> Optional[bytes]:
        data = getattr(track, "data", None) or {}
        url = data.get("tidl_license_url") or self._default_license_url(system)
        offered = str(data.get("tidl_drm_system") or "").upper()

        if not url:
            self.log.error(
                f" - No {system} license URL. TIDAL currently only exposes a Widevine endpoint"
                f"{f' (Manifest offered {offered})' if offered else ''}; "
                "set endpoints.playready_license in config.yaml if that changes, "
                "or use a Widevine CDM endpoint."
            )
            raise SystemExit(1)
        if offered and offered != system:
            self.log.warning(
                f" - Loaded CDM is {system} but TIDAL's manifest offered {offered}. "
                "Trying regardless."
            )

        body = challenge if isinstance(challenge, (bytes, bytearray)) else str(challenge).encode()
        resp = self.session.post(url, data=body, headers={"Content-Type": "application/octet-stream"})

        if resp.status_code in (401, 403) and self.refresh_token:
            self.access_token = self._refresh_access_token(self.refresh_token)
            self.session.headers["Authorization"] = f"Bearer {self.access_token}"
            resp = self.session.post(url, data=body,
                                     headers={"Content-Type": "application/octet-stream"})

        if resp.status_code != 200 or not resp.content:
            self.log.error(f" - TIDAL {system} license error: {resp.status_code} {resp.text[:200]}")
            raise SystemExit(1)
        return resp.content

    def on_track_downloaded(self, track: Any) -> None:
        if getattr(track, "drm", None):
            return
        self._finalize(track)

    def on_track_decrypted(self, track: Any, drm: Any = None, segment: Any = None) -> None:
        self._finalize(track)

    def _finalize(self, track: Any) -> None:
        try:
            data = getattr(track, "data", None)
            path = getattr(track, "path", None)
            if not isinstance(data, dict) or not path:
                return
            path = Path(path)
            if not path.exists():
                return
            if not data.get("tidl_flac"):
                self._rename_ext(track, path, "m4a")
                return
            if path.suffix.lower() == ".flac":
                return
            with path.open("rb") as f:
                magic = f.read(4)
            if magic == b"fLaC":
                self._rename_ext(track, path, "flac")
            else:
                self._remux_flac(track, path)
        except Exception as e:
            self.log.debug(f"post-download step skipped: {e}")

    def _rename_ext(self, track: Any, path: Path, ext: str) -> None:
        if path.suffix.lower() == f".{ext}":
            return
        new_path = path.with_suffix(f".{ext}")
        if new_path.exists():
            new_path.unlink()
        path.rename(new_path)
        track.path = new_path

    def _remux_flac(self, track: Any, path: Path) -> None:
        import subprocess

        from unshackle.core import binaries

        if not binaries.FFMPEG:
            self.log.warning(" - ffmpeg not installed.")
            self._rename_ext(track, path, "m4a")
            return
        out_path = path.with_suffix(".flac")
        if out_path.exists():
            out_path.unlink()
        proc = subprocess.run(
            [str(binaries.FFMPEG), "-y", "-hide_banner", "-loglevel", "error",
             "-i", str(path), "-map", "0:a", "-c:a", "copy", str(out_path)],
            capture_output=True,
        )
        if proc.returncode != 0 or not out_path.exists() or out_path.stat().st_size == 0:
            self.log.warning(
                f" - FLAC remux failed: {proc.stderr.decode(errors='ignore')[:200]}"
            )
            if out_path.exists():
                out_path.unlink()
            self._rename_ext(track, path, "m4a")
            return
        path.unlink()
        track.path = out_path

    @staticmethod
    def _first_artist(obj: dict) -> Optional[str]:
        obj = obj or {}
        artist = obj.get("artist")
        if isinstance(artist, dict) and artist.get("name"):
            return str(artist["name"]).strip()
        artists = obj.get("artists") or []
        if isinstance(artists, list) and artists:
            for a in artists:
                if isinstance(a, dict) and str(a.get("type", "MAIN")).upper() == "MAIN" and a.get("name"):
                    return str(a["name"]).strip()
            name = (artists[0] or {}).get("name")
            return str(name).strip() if name else None
        return None

    def _cover_variant(self, size: Any) -> str:
        raw = str(size if size is not None else "").strip().lower()
        if not raw or raw == "origin":
            return self.COVER_VARIANT
        digits = raw.split("x")[0]
        if digits.isdigit() and int(digits) in self.COVER_SIZES:
            return f"{digits}x{digits}"
        self.log.warning(
            f" - cover_size {size!r} is not one of {', '.join(map(str, self.COVER_SIZES))} "
            "or 'origin'. Using origin."
        )
        return self.COVER_VARIANT

    def _cover(self, cover_id: Optional[str]) -> Optional[str]:
        if not cover_id:
            return None
        return self.COVER_URL.format(
            path=str(cover_id).replace("-", "/"), variant=self.cover_variant
        )

    @staticmethod
    def _year(*values: Any) -> int:
        for value in values:
            if not value:
                continue
            m = re.search(r"(\d{4})", str(value))
            if m and int(m.group(1)) > 0:
                return int(m.group(1))
        return 0

    @staticmethod
    def _release_kind(album: dict, track_count: Optional[int]) -> str:
        kind = str((album or {}).get("type") or "").lower()
        if kind in ("ep", "single", "compilation", "album"):
            return kind
        if track_count is not None and track_count <= 3:
            return "single"
        return "album"