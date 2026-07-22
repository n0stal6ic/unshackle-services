from __future__ import annotations
import hmac
import hashlib
import re
import time
from http.cookiejar import CookieJar
from typing import Any, Optional
import click
from pywidevine.pssh import PSSH
from pywidevine.license_protocol_pb2 import WidevinePsshData
from unshackle.core.credential import Credential
from unshackle.core.drm import Widevine
from unshackle.core.music import MusicTrackOption
from unshackle.core.service import Service
from unshackle.core.titles import Music, Song, Titles_T
from unshackle.core.tracks import Audio, Chapters, Tracks
from unshackle.core.tracks.track import Track


class SPOT(Service):
    """
    Service code for Spotify (https://spotify.com)
    www.nostalgic.cc
    Authorization: Cookies, Credentials
    Security: FLAC@L1, AAC@L3
    """

    ALIASES = ("SPOT", "spotify")
    GROUP_AUDIO_DOWNLOADS = True

    TITLE_RE = (
        r"(?:https?://open\.spotify\.com/(?:intl-[a-z]{2}/)?|spotify:)"
        r"(?P<type>track|album|playlist|artist)[:/](?P<id>[0-9A-Za-z]{22})"
    )
    B62 = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    PROTECTION_SCHEME_CENC = 1667591779

    QUALITIES = {
        "AAC_MEDIUM": ("10", "m4a", "AAC 128 kb/s", False, False),
        "AAC_HIGH": ("11", "m4a", "AAC 256 kb/s", False, True),
        "FLAC": ("17", "flac", "FLAC (MP4)", True, True),
        "FLAC_24": ("23", "flac", "FLAC 24-bit (MP4)", True, True),
    }
    QUALITY_MAP = {
        "AAC_MEDIUM": "AAC_MEDIUM", "AAC-MEDIUM": "AAC_MEDIUM", "128": "AAC_MEDIUM",
        "AAC_HIGH": "AAC_HIGH", "AAC-HIGH": "AAC_HIGH", "AAC": "AAC_HIGH", "256": "AAC_HIGH",
        "FLAC": "FLAC", "LOSSLESS": "FLAC",
        "FLAC_24": "FLAC_24", "FLAC-24": "FLAC_24", "24": "FLAC_24",
    }
    FALLBACK_ORDER = ["FLAC_24", "FLAC", "AAC_HIGH", "AAC_MEDIUM"]

    @staticmethod
    @click.command(name="SPOT", short_help="https://spotify.com", help=__doc__)
    @click.argument("title", type=str)
    @click.option("-q", "--quality", "quality",
                  type=click.Choice(["AAC_MEDIUM", "AAC_HIGH", "FLAC", "FLAC_24",
                                     "128", "256", "AAC", "LOSSLESS", "24"], case_sensitive=False),
                  default=None,
                  help="Audio quality (Default: config default_quality, or FLAC_24). "
                       "FLAC needs premium or will fallback to best available.")
    @click.pass_context
    def cli(ctx, **kwargs):
        return SPOT(ctx, **kwargs)

    def __init__(self, ctx, title: str, quality: Optional[str]):
        super().__init__(ctx)
        self.title = title
        self.endpoints = self.config["endpoints"]
        self.client_version = self.config.get("client_version") or "1.2.87.27.ga2033a72"

        if quality:
            self.quality = self.QUALITY_MAP[quality.upper()]
        else:
            cfg_q = str(self.config.get("default_quality", "AAC_HIGH")).upper()
            self.quality = self.QUALITY_MAP.get(cfg_q, "AAC_HIGH")

        self.sp_dc: Optional[str] = None
        self.access_token: Optional[str] = None
        self.client_token: Optional[str] = None
        self.token_expiry: float = 0.0
        self.is_premium: bool = False

        m = re.search(self.TITLE_RE, self.title)
        if not m:
            self.log.error(" - Could not parse a Spotify track/album/playlist/artist URL or URI.")
            raise SystemExit(1)
        self.item_type = m.group("type")
        self.item_id = m.group("id")

    def authenticate(self, cookies: Optional[CookieJar] = None, credential: Optional[Credential] = None) -> None:
        super().authenticate(cookies, credential)
        self.session.headers.update({
            "accept": "application/json",
            "accept-language": "en-US",
            "content-type": "application/json",
            "origin": "https://open.spotify.com",
            "referer": "https://open.spotify.com/",
            "user-agent": self.config.get(
                "user_agent",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
            ),
            "spotify-app-version": self.client_version,
            "app-platform": "WebPlayer",
        })

        self.sp_dc = self._resolve_sp_dc(cookies, credential)
        if not self.sp_dc:
            self.log.error(
                " - No Spotify 'sp_dc' found. Provide it via unshackle.yaml credentials as "
                "'sp_dc:VALUE' (or 'token:VALUE'), an 'sp_dc' cookie, or 'sp_dc:' in config.yaml."
            )
            raise SystemExit(1)
        self.session.cookies.set("sp_dc", self.sp_dc, domain=".spotify.com")

        self._refresh_token()
        self._check_account()
        self.log.info(
            f" + Authenticated with Spotify ({'Premium' if self.is_premium else 'Free/Unknown'})"
        )

    def _resolve_sp_dc(self, cookies: Optional[CookieJar], credential: Optional[Credential]) -> Optional[str]:
        if credential:
            user = (credential.username or "").strip()
            pw = (credential.password or "").strip()
            if user.lower() in ("sp_dc", "token", "spotify") and pw:
                return pw
            if pw and not user:
                return pw
            if user and not pw:
                return user
        if cookies:
            for cookie in cookies:
                if cookie.name.lower() == "sp_dc" and cookie.value:
                    return cookie.value
        cfg = str(self.config.get("sp_dc") or "").strip()
        return cfg or None

    def _refresh_token(self) -> None:
        server_time_ms = self._server_time_ms()
        totp = self._generate_totp(server_time_ms)
        version = self._totp_version

        resp = self.session.get(self.endpoints["session_token"], params={
            "reason": "init",
            "productType": "web-player",
            "totp": totp,
            "totpServer": totp,
            "totpVer": version,
        })
        resp.raise_for_status()
        info = resp.json()
        self.access_token = info.get("accessToken")
        if not self.access_token:
            self.log.error(f" - Spotify token request returned no accessToken: {info}")
            raise SystemExit(1)
        self.token_expiry = float(info.get("accessTokenExpirationTimestampMs", 0)) / 1000 or (time.time() + 3000)

        client_id = info.get("clientId")
        try:
            ct = self.session.post(self.endpoints["client_token"], json={
                "client_data": {
                    "client_version": self.client_version,
                    "client_id": client_id,
                    "js_sdk_data": {},
                }
            }, headers={"Accept": "application/json"}).json()
            self.client_token = ct.get("granted_token", {}).get("token")
        except Exception as e:
            self.log.debug(f"client-token fetch failed: {e}")

        self.session.headers["authorization"] = f"Bearer {self.access_token}"
        if self.client_token:
            self.session.headers["client-token"] = self.client_token

    def _ensure_token(self) -> None:
        if not self.access_token or time.time() >= self.token_expiry:
            self._refresh_token()

    def _server_time_ms(self) -> int:
        try:
            resp = self.session.get(self.endpoints["server_time"])
            resp.raise_for_status()
            return int(1e3 * resp.json()["serverTime"])
        except Exception:
            return int(time.time() * 1000)

    def _check_account(self) -> None:
        try:
            resp = self.session.get(f"{self.endpoints['web_api']}/me")
            if resp.status_code == 200:
                self.is_premium = resp.json().get("product") == "premium"
        except Exception as e:
            self.log.debug(f"Account check failed: {e}")


    @property
    def _totp_version(self) -> str:
        secrets = self._totp_secrets()
        return max(secrets.keys(), key=int)

    def _totp_secrets(self) -> dict:
        if getattr(self, "_totp_cache", None):
            return self._totp_cache
        pinned_v = self.config.get("totp_version")
        pinned_s = self.config.get("totp_secret")
        if pinned_v and pinned_s:
            self._totp_cache = {str(pinned_v): list(pinned_s)}
            return self._totp_cache
        url = str(self.config.get("totp_secrets_url") or "").strip()
        if not url:
            self.log.error(
                " - No totp secrets url in config. "
                "Set one of them so a web-player token can be generated."
            )
            raise SystemExit(1)
        resp = self.session.get(url, headers={"authorization": "", "client-token": ""})
        resp.raise_for_status()
        self._totp_cache = {str(k): v for k, v in resp.json().items()}
        return self._totp_cache

    def _generate_totp(self, timestamp_ms: int) -> str:
        secrets = self._totp_secrets()
        version = max(secrets.keys(), key=int)
        cipher = secrets[version]
        secret = "".join(str(byte ^ ((i % 33) + 9)) for i, byte in enumerate(cipher)).encode("ascii")
        counter = int(timestamp_ms) // 1000 // 30
        h = hmac.new(secret, counter.to_bytes(8, "big"), hashlib.sha1).digest()
        offset = h[-1] & 0x0F
        binary = ((h[offset] & 0x7F) << 24 | (h[offset + 1] & 0xFF) << 16
                  | (h[offset + 2] & 0xFF) << 8 | (h[offset + 3] & 0xFF))
        return str(binary % 1_000_000).zfill(6)

    def _api(self, path: str, params: Optional[dict] = None) -> dict:
        self._ensure_token()
        resp = self.session.get(f"{self.endpoints['web_api']}{path}", params=params or {})
        if resp.status_code != 200:
            self.log.error(f" - Spotify Web API error on {path}: {resp.status_code} {resp.text[:200]}")
            raise SystemExit(1)
        return resp.json()

    def _media_id_to_gid(self, media_id: str) -> str:
        n = 0
        for ch in media_id:
            n = n * 62 + self.B62.index(ch)
        return f"{n:032x}"

    def get_titles(self) -> Titles_T:
        if self.item_type == "track":
            return self._titles_from_track(self.item_id)
        if self.item_type == "album":
            return self._titles_from_album(self.item_id)
        if self.item_type == "playlist":
            return self._titles_from_playlist(self.item_id)
        return self._titles_from_artist(self.item_id)

    def _titles_from_track(self, track_id: str) -> Music:
        track = self._api(f"/tracks/{track_id}")
        album = track.get("album") or {}
        song = self._build_song(track, album)
        return Music(
            [song], kind="single", title=song.album,
            artist=song.album_artist or song.artist, year=song.year,
            total_tracks=1, artwork_url=song.artwork_url,
        )

    def _titles_from_album(self, album_id: str) -> Music:
        album = self._api(f"/albums/{album_id}")
        items = list((album.get("tracks") or {}).get("items") or [])
        next_url = (album.get("tracks") or {}).get("next")
        while next_url:
            page = self._api_absolute(next_url)
            items.extend(page.get("items") or [])
            next_url = page.get("next")
        songs = [self._build_song(t, album) for t in items]
        if not songs:
            self.log.error(f" - No tracks found for album {album_id}."); raise SystemExit(1)
        return Music(
            songs, kind=self._release_kind(album, len(songs)),
            title=album.get("name"), artist=self._first_artist(album),
            year=self._year(album.get("release_date")),
            total_tracks=album.get("total_tracks") or len(songs),
            total_discs=max((s.disc for s in songs), default=1),
            artwork_url=self._cover(album),
            total_duration=sum(int((t.get("duration_ms") or 0) / 1000) for t in items) or None,
        )

    def _titles_from_playlist(self, playlist_id: str) -> Music:
        pl = self._api(f"/playlists/{playlist_id}")
        items = list((pl.get("tracks") or {}).get("items") or [])
        next_url = (pl.get("tracks") or {}).get("next")
        while next_url:
            page = self._api_absolute(next_url)
            items.extend(page.get("items") or [])
            next_url = page.get("next")
        songs = []
        for position, entry in enumerate(items, start=1):
            track = entry.get("track") if isinstance(entry, dict) else None
            if not track or track.get("type") != "track" or not track.get("id"):
                continue
            songs.append(self._build_song(track, track.get("album") or {}, playlist_position=position))
        if not songs:
            self.log.error(f" - No playable tracks found for playlist {playlist_id}."); raise SystemExit(1)
        return Music(
            songs, kind="playlist", title=pl.get("name"),
            artist=(pl.get("owner") or {}).get("display_name") or None,
            total_tracks=len(songs),
            owner=(pl.get("owner") or {}).get("display_name") or None,
            artwork_url=self._cover(pl),
            description=(pl.get("description") or None),
        )

    def _titles_from_artist(self, artist_id: str) -> Music:
        data = self._api(f"/artists/{artist_id}/top-tracks", params={"market": "US"})
        items = data.get("tracks") or []
        songs = []
        for position, track in enumerate(items, start=1):
            songs.append(self._build_song(track, track.get("album") or {}, playlist_position=position))
        if not songs:
            self.log.error(f" - No top tracks found for artist {artist_id}."); raise SystemExit(1)
        return Music(
            songs, kind="playlist",
            title=f"{songs[0].artist} - Top Tracks", artist=songs[0].artist,
            total_tracks=len(songs),
        )

    def _api_absolute(self, url: str) -> dict:
        self._ensure_token()
        resp = self.session.get(url)
        if resp.status_code != 200:
            self.log.error(f" - Spotify Web API pagination error: {resp.status_code} {resp.text[:160]}")
            raise SystemExit(1)
        return resp.json()

    def _build_song(self, track: dict, album: dict, playlist_position: Optional[int] = None) -> Song:
        album = album or track.get("album") or {}
        title = (track.get("name") or "Unknown").strip()
        artist = self._first_artist(track) or self._first_artist(album) or "Unknown Artist"
        album_title = (album.get("name") or "Unknown Album").strip()
        album_artist = self._first_artist(album) or artist
        year = self._year(album.get("release_date")) or 1
        disc = int(track.get("disc_number") or 1)
        track_num = playlist_position or int(track.get("track_number") or 1)
        isrc = ((track.get("external_ids") or {}).get("isrc") or "").strip() or None
        artwork = self._cover(album)

        data = {
            "service": self.ALIASES[0],
            "track_id": track.get("id"),
            "album_id": album.get("id"),
            "title": title,
            "artist": artist,
            "album": album_title,
            "album_artist": album_artist,
            "duration": int((track.get("duration_ms") or 0) / 1000),
            "isrc": isrc,
            "artwork_url": artwork,
        }
        return Song(
            id_=track.get("id"),
            service=self.__class__,
            name=title,
            artist=artist,
            album=album_title,
            track=int(track_num),
            disc=int(disc),
            year=int(year),
            album_artist=album_artist,
            release_type=self._release_kind(album, None),
            total_tracks=int(album["total_tracks"]) if album.get("total_tracks") else None,
            explicit=bool(track.get("explicit")),
            isrc=isrc,
            artwork_url=artwork,
            data=data,
        )

    def get_music_track_options(self, song: Song) -> list[MusicTrackOption]:
        fmt = self._effective_quality()
        _fid, container, label, lossless, _prem = self.QUALITIES[fmt]
        data = song.data if isinstance(song.data, dict) else {}
        if lossless:
            option = MusicTrackOption(codec="FLAC", bit_depth=24 if fmt == "FLAC_24" else 16,
                                      sample_rate=44100, channels=2.0, lossless=True,
                                      hires=fmt == "FLAC_24")
        else:
            option = MusicTrackOption(codec="AAC", bitrate=256000 if fmt == "AAC_HIGH" else 128000,
                                      channels=2.0, lossless=False)
        option.explicit = bool(song.explicit)
        option.duration = int(data.get("duration")) if data.get("duration") else None
        option.quality_label = label
        return [option]

    def get_tracks(self, song: Song) -> Tracks:
        track_id = str(song.id)
        fmt, file_id = self._resolve_file(track_id)
        format_id, container, _label, lossless, _prem = self.QUALITIES[fmt]

        stream_url = self._storage_resolve(format_id, file_id)
        pssh = self._build_pssh(file_id)
        drm = Widevine(pssh=pssh, kid=bytes.fromhex(file_id[:32]))

        audio = Audio(
            stream_url,
            language=song.language or "en",
            codec=Audio.Codec.FLAC if lossless else Audio.Codec.AAC,
            bitrate=None if lossless else (256000 if fmt == "AAC_HIGH" else 128000),
            channels=2,
            descriptor=Track.Descriptor.URL,
            drm=[drm],
            id_=track_id,
            data={"spot_ext": container, "spot_format": fmt},
        )
        return Tracks([audio])

    def get_chapters(self, song: Song) -> Chapters:
        return Chapters()

    def _effective_quality(self) -> str:
        fmt = self.quality
        if self.QUALITIES[fmt][4] and not self.is_premium:
            return "AAC_MEDIUM"
        return fmt

    def _resolve_file(self, track_id: str):
        start = self._effective_quality()
        order = self.FALLBACK_ORDER[self.FALLBACK_ORDER.index(start):]
        last = ""
        for fmt in order:
            format_id, _c, _l, lossless, prem = self.QUALITIES[fmt]
            if prem and not self.is_premium:
                continue
            manifest_key = "file_ids_mp4flac" if lossless else "file_ids_mp4"
            file_id = self._playback_file_id(track_id, manifest_key, format_id)
            if file_id:
                if fmt != self.quality:
                    self.log.warning(f" - {self.quality} unavailable for this track. Using: {fmt}.")
                return fmt, file_id
            last = f"no file for format {format_id}"
        self.log.error(f" - No usable file for track {track_id}. {last}")
        raise SystemExit(1)

    def _playback_file_id(self, track_id: str, manifest_key: str, format_id: str) -> Optional[str]:
        self._ensure_token()
        try:
            resp = self.session.get(
                self.endpoints["playback_info"].format(media_type="track", media_id=track_id),
                params={"manifestFileFormat": manifest_key},
            )
            resp.raise_for_status()
            body = resp.json()
        except Exception as e:
            self.log.debug(f"playback-info failed ({manifest_key}): {e}")
            return None
        media = body.get("media") or {}
        item = (media.get(next(iter(media), "")) or {}).get("item") or body.get("item") or {}
        files = (item.get("manifest") or {}).get(manifest_key) or []
        for f in files:
            if str(f.get("format")) == str(format_id):
                return f.get("file_id")
        return None

    def _storage_resolve(self, format_id: str, file_id: str) -> str:
        self._ensure_token()
        resp = self.session.get(self.endpoints["storage_resolve"].format(format_id=format_id, file_id=file_id))
        if resp.status_code != 200:
            self.log.error(f" - storage-resolve failed: {resp.status_code} {resp.text[:160]}")
            raise SystemExit(1)
        urls = resp.json().get("cdnurl") or []
        if not urls:
            self.log.error(" - storage-resolve returned no CDN URL."); raise SystemExit(1)
        return urls[0]

    def _build_pssh(self, file_id: str) -> PSSH:
        wv = WidevinePsshData()
        wv.algorithm = WidevinePsshData.AESCTR
        wv.key_ids.append(bytes.fromhex(file_id[:32]))
        wv.provider = "spotify"
        wv.content_id = bytes.fromhex(file_id)
        wv.protection_scheme = self.PROTECTION_SCHEME_CENC
        return PSSH.new(system_id=PSSH.SystemId.Widevine, init_data=wv.SerializeToString())

    def get_widevine_service_certificate(self, **_: Any) -> None:
        return None

    def get_widevine_license(self, *, challenge: bytes, title: Any, track: Any) -> bytes:
        self._ensure_token()
        resp = self.session.post(self.endpoints["widevine_license"], data=challenge)
        if resp.status_code != 200 or not resp.content:
            self.log.error(f" - Spotify Widevine license error: {resp.status_code} {resp.text[:200]}")
            raise SystemExit(1)
        return resp.content

    def on_track_downloaded(self, track: Any) -> None:
        try:
            data = getattr(track, "data", None)
            path = getattr(track, "path", None)
            if not isinstance(data, dict) or not path:
                return
            from pathlib import Path
            path = Path(path)
            if not path.exists():
                return
            ext = "m4a" if data.get("spot_format", "").startswith("AAC") else "mp4"
            if path.suffix.lower() == f".{ext}":
                return
            new_path = path.with_suffix(f".{ext}")
            if new_path.exists():
                new_path.unlink()
            path.rename(new_path)
            track.path = new_path
        except Exception as e:
            self.log.debug(f"Extension rename skipped: {e}")

    @staticmethod
    def _first_artist(obj: dict) -> Optional[str]:
        artists = (obj or {}).get("artists") or []
        if artists and isinstance(artists, list):
            name = (artists[0] or {}).get("name")
            return name.strip() if name else None
        return None

    @staticmethod
    def _cover(obj: dict) -> Optional[str]:
        images = (obj or {}).get("images") or []
        if images and isinstance(images, list):
            return images[0].get("url")
        return None

    @staticmethod
    def _year(release_date: Optional[str]) -> int:
        if release_date:
            m = re.match(r"(\d{4})", str(release_date))
            if m:
                return int(m.group(1))
        return 0

    @staticmethod
    def _release_kind(album: dict, track_count: Optional[int]) -> str:
        at = str((album or {}).get("album_type") or "").lower()
        if at in ("single", "compilation", "album"):
            return at
        if track_count is not None and track_count <= 3:
            return "single"
        return "album"