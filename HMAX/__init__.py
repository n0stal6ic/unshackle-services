from __future__ import annotations
import base64
import json
import re
import uuid
from collections import defaultdict
from copy import deepcopy
from datetime import datetime
from http.cookiejar import CookieJar
from typing import Generator, Optional, Union
from zlib import crc32
import click
from langcodes import Language
from lxml import etree
from unshackle.core.config import config
from unshackle.core.constants import AnyTrack
from unshackle.core.credential import Credential
from unshackle.core.manifests import DASH
from unshackle.core.search_result import SearchResult
from unshackle.core.service import Service
from unshackle.core.titles import Episode, Movie, Movies, Series, Title_T, Titles_T
from unshackle.core.tracks import Attachment, Chapter, Chapters, Subtitle, Tracks, Video
from unshackle.core.utilities import is_close_match


class HMAX(Service):
    """
    Service code for HBO Max (https://play.hbomax.com)
    www.nostalgic.cc
    Authorization: Cookies, Credentials
    """

    ALIASES = ("HMAX", "max", "hbomax")

    TITLE_RE = r"^(?:https?://(?:www\.|play\.)?(?:hbomax|max)\.com/)?(?P<type>[^/]+)/(?P<id>[^/?#]+)"

    VIDEO_CODEC_MAP = {"H264": [Video.Codec.AVC], "H265": [Video.Codec.HEVC]}
    AUDIO_CODEC_MAP = {"AAC": "mp4a", "AC3": "ac-3", "EC3": "ec-3"}

    SHOW_TYPES = ("show", "mini-series", "topical")
    SEASON_ALIASES = {
        "mini-series": "generic-miniseries-page-rail-episodes",
        "topical": "generic-topical-show-page-rail-episodes",
    }

    @staticmethod
    @click.command(name="HMAX", short_help="https://play.hbomax.com")
    @click.argument("title", type=str)
    @click.option("-vcodec", "--video-codec", type=click.Choice(["H264", "H265"], case_sensitive=False),
                  default=None, help="Pick a specific video codec.")
    @click.option("-acodec", "--audio-codec", type=click.Choice(["AAC", "AC3", "EC3"], case_sensitive=False),
                  default=None, help="Pick a specific audio codec.")
    @click.option("-e", "--extras", is_flag=True, default=False,
                  help="Fetch the extras for a title.")
    @click.option("--no-attachments", is_flag=True, default=False,
                  help="Skip downloading cover art.")
    @click.option("-d", "--device", "device_name", default=None,
                  help="Device profile from config to present.")
    @click.option("-m", "--market", "market", default=None,
                  help="Route API calls through a specific market host from config.")
    @click.option("-lm", "--login-method",
                  type=click.Choice(["auto", "credential", "cookies"], case_sensitive=False),
                  default="auto",
                  help="Force one sign-in method. Default tries them in the configured order.")
    @click.pass_context
    def cli(ctx, **kwargs):
        return HMAX(ctx, **kwargs)

    def __init__(self, ctx, title: str, video_codec: Optional[str], audio_codec: Optional[str],
                 extras: bool, no_attachments: bool, device_name: Optional[str],
                 market: Optional[str], login_method: str):
        super().__init__(ctx)
        self.title = title
        self.login_method = (login_method or "auto").lower()
        self.vcodec = (video_codec or "").upper() or None
        self.acodec = (audio_codec or "").upper() or None
        self.extras = extras
        self.no_attachments = no_attachments
        self.market = market
        self.profile = getattr(ctx.obj, "profile", None)

        range_param = ctx.parent.params.get("range_")
        self.range = range_param[0].name if range_param else "SDR"
        if self.range == "HDR10":
            self.vcodec = "H265"

        self.device_name = device_name or self.config["default_device"]
        if self.device_name not in self.config["devices"]:
            self.log.error(
                f" - Unknown device profile '{self.device_name}'. "
                f"Available: {', '.join(self.config['devices'])}"
            )
            raise SystemExit(1)
        self.device = self.config["devices"][self.device_name]
        self.playready = "certificate_chain" in dir(getattr(ctx.obj, "cdm", None))
        self.drm_system = "playready" if self.playready else "widevine"
        self.log.info(f" + Device Profile: {self.device_name} ({self.drm_system})")

        self.device_id = self.device["playback"].get("deviceId") or str(uuid.uuid4())
        self.wv_license_url: Optional[str] = None
        self.pr_license_url: Optional[str] = None
        self._saved_headers: dict = {}

    def authenticate(self, cookies: Optional[CookieJar] = None, credential: Optional[Credential] = None) -> None:
        super().authenticate(cookies, credential)

        self.session.headers.update(self.config["headers"])
        self.session.headers["x-wbd-time-zone"] = self.config["x_wbd_time_zone"]
        self.session.headers["x-wbd-ace"] = self._ace_header()
        for name, value in self.device["headers"].items():
            self.session.headers[name] = value % self.device_id if "%s" in value else value
        self.session.headers["traceparent"] = f"00-{uuid.uuid4().hex}-{uuid.uuid4().hex[:16]}-01"
        self.session.hooks.setdefault("response", [])
        self.session.hooks["response"].append(self._absorb_session_state)

        order = [self.login_method] if self.login_method != "auto" else self.config["login_order"]
        attempts = {
            "credential": lambda: self._auth_credential(credential),
            "cookies": lambda: self._auth_cookies(cookies),
        }

        failures = []
        for method in order:
            if method not in attempts:
                self.log.warning(f" - Unknown login method {method!r}, skipping.")
                continue
            try:
                if attempts[method]():
                    break
            except SystemExit:
                raise
            except Exception as e:
                failures.append(f"{method}: {e}")
                self.log.debug(f"{method} login failed: {e}")
        else:
            self.log.error(f" - Could not authenticate with: {', '.join(order)}.")
            for failure in failures:
                self.log.error(f"     {failure}")
            self.log.error(
                " - Provide a cookie file containing 'st', or put the token in unshackle.yaml "
                "under services: HMAX: st."
            )
            raise SystemExit(1)


    def _auth_credential(self, credential: Optional[Credential]) -> bool:
        token = str(self.config.get("st") or "").strip()
        if not token and credential:
            if (credential.username or "").lower() == "st":
                token = credential.password
            elif not credential.username:
                token = credential.password
        if not token:
            return False

        self.session.cookies.set("st", token, domain=".hbomax.com")
        self.session.headers["tracestate"] = f"wbd=session:{self.device_id}"
        self.session.headers["x-wbd-session-state"] = self._get_device_token()
        self.log.info(" + Authenticated with a configured 'st' token")
        return True

    def _auth_cookies(self, cookies: Optional[CookieJar]) -> bool:
        if not cookies:
            return False
        if not any(c.name == "st" for c in cookies):
            self.log.warning(" - Cookie file has no 'st' value.")
            return False

        self.session.cookies.update(cookies)
        session_cookie = next((c.value for c in cookies if c.name == "session"), None)
        if session_cookie:
            self.device_id = self._session_device_id(session_cookie) or self.device_id
        st = next((c.value for c in cookies if c.name == "st"), None)
        if st:
            self.session.headers["Authorization"] = f"Bearer {st}"
        self.session.headers["tracestate"] = f"wbd=session:{self.device_id}"
        self.session.headers["x-wbd-session-state"] = self._get_device_token()
        self.log.info(" + Authenticated with cookies")
        return True

    def _ace_header(self) -> str:
        stamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        packed = f"{stamp}|{self.config['ace_region']}|{self.config['ace_tier']}|{self.config.get('ace_flags', '')}"
        return base64.b64encode(packed.encode()).decode()

    def _absorb_session_state(self, response, *_args, **_kwargs) -> None:
        if self._saved_headers:
            return
        state = response.headers.get("x-wbd-session-state")
        if state:
            self.session.headers["x-wbd-session-state"] = state

    @staticmethod
    def _session_device_id(raw: str) -> Optional[str]:
        try:
            parsed = json.loads(raw)
        except ValueError:
            return raw.strip() or None
        if isinstance(parsed, dict):
            value = parsed.get("uuid") or parsed.get("deviceId") or parsed.get("id")
            return str(value) if value else None
        return str(parsed) if parsed else None

    @staticmethod
    def _api_errors(response) -> list[str]:
        try:
            errors = response.json().get("errors") or []
        except ValueError:
            return [response.text[:300]] if response.text else []
        return [
            " ".join(filter(None, (e.get("code"), e.get("message") or e.get("detail"))))
            for e in errors
        ] or [response.text[:300]]

    def _route(self, url: str) -> str:
        if not self.market:
            return url
        host = self.config["markets"].get(self.market)
        if not host:
            self.log.warning(f" - Unknown market {self.market!r}. Known: {', '.join(self.config['markets'])}")
            return url
        return re.sub(r"^https://[^/]+", f"https://{host}", url)

    def _get_device_token(self) -> str:
        response = self.session.post(self._route(self.config["endpoints"]["bootstrap"]))
        response.raise_for_status()
        state = response.headers.get("x-wbd-session-state")
        if not state:
            raise RuntimeError("bootstrap returned no x-wbd-session-state")
        return state

    def search(self) -> Generator[SearchResult, None, None]:
        response = self.session.get(self._route(self.config["endpoints"]["search"]), params={"q": self.title})
        if response.status_code != 200:
            self.log.warning(f" - Search returned {response.status_code}.")
            return
        for result in response.json().get("results", []):
            yield SearchResult(
                id_=result.get("id"),
                title=result.get("title", "Unknown"),
                label=str(result.get("type", "")).upper() or None,
                url=f"https://play.hbomax.com/{result.get('type', 'content')}/{result.get('id')}",
            )

    def get_titles(self) -> Titles_T:
        match = re.match(self.TITLE_RE, self.title)
        if not match:
            self.log.error(f" - Could not parse an HBO Max URL or ID from: {self.title!r}")
            raise SystemExit(1)

        content_type = match.group("type")
        external_id = match.group("id")

        response = self.session.get(self._route(self.config["endpoints"]["contentRoutes"] % (content_type, external_id)))
        response.raise_for_status()
        payload = response.json()
        content_title = self._content_title(payload, content_type, external_id)

        if content_type in ("sport", "event"):
            return Movies([self._build_event(payload, external_id, content_title)])
        if content_type in ("movie", "standalone"):
            return Movies([self._build_movie(payload, external_id, content_title)])
        if content_type in self.SHOW_TYPES:
            return self._build_series(payload, content_type, external_id, content_title)

        self.log.error(f" - Unsupported content type '{content_type}'.")
        raise SystemExit(1)

    @staticmethod
    def _content_title(payload: dict, content_type: str, external_id: str) -> str:
        alias = f"generic-{re.sub(r'-', '', content_type)}-blueprint-page"
        for item in payload.get("included", []):
            attrs = item.get("attributes") or {}
            if attrs.get("alias") == alias and attrs.get("title"):
                return attrs["title"]
        for item in payload.get("included", []):
            attrs = item.get("attributes") or {}
            if attrs.get("alternateId") == external_id and attrs.get("originalName"):
                return attrs["originalName"]
        raise ValueError(f"Could not find a title for {external_id}")

    @staticmethod
    def _year(attrs: dict) -> Optional[int]:
        stamp = attrs.get("airDate") or attrs.get("firstAvailableDate")
        if not stamp:
            return None
        try:
            return datetime.strptime(stamp, "%Y-%m-%dT%H:%M:%SZ").year
        except ValueError:
            return None

    @staticmethod
    def _image_map(payload: dict) -> dict:
        return {obj["id"]: obj for obj in payload.get("included", []) if obj.get("type") == "image"}

    @classmethod
    def _attach_images(cls, item: dict, image_map: dict) -> dict:
        ids = [img["id"] for img in (item.get("relationships", {}).get("images", {}).get("data") or [])]
        enriched = dict(item)
        enriched["_images"] = [image_map[i] for i in ids if i in image_map]
        return enriched

    def _build_event(self, payload: dict, external_id: str, content_title: str) -> Movie:
        event = next(
            (inc for inc in payload.get("included", []) if "VOD" in (inc.get("attributes") or {}).values()),
            None,
        )
        if not event:
            try:
                event = self.session.get(
                    self._route(self.config["endpoints"]["videoPages"] % external_id), timeout=15
                ).json().get("data")
            except Exception as e:
                self.log.debug(f"videoPages lookup failed: {e}")
        if not event:
            self.log.error(f" - No VOD entry found for event {external_id}.")
            raise SystemExit(1)
        return Movie(
            id_=external_id, service=self.__class__, name=content_title.title(),
            year=self._year(event["attributes"]),
            data=self._attach_images(event, self._image_map(payload)),
        )

    def _build_movie(self, payload: dict, external_id: str, content_title: str) -> Movie:
        metadata = self.session.get(self._route(self.config["endpoints"]["moviePages"] % external_id)).json().get("data")
        if not metadata or "edit" not in (metadata.get("relationships") or {}):
            metadata = next(
                (x for x in payload.get("included", [])
                 if x.get("type") == "video"
                 and x.get("relationships", {}).get("show", {}).get("data", {}).get("id") == external_id),
                metadata,
            )
        if not metadata:
            self.log.error(f" - No playable video found for {external_id}.")
            raise SystemExit(1)
        return Movie(
            id_=external_id, service=self.__class__, name=content_title,
            year=self._year(metadata["attributes"]),
            data=self._attach_images(metadata, self._image_map(payload)),
        )

    def _season_filters(self, payload: dict, content_type: str) -> list:
        alias = self.SEASON_ALIASES.get(content_type, f"-{content_type}-page-rail-episodes-tabbed-content")
        for included in payload.get("included", []):
            attrs = included.get("attributes") or {}
            if alias in str(attrs).lower():
                filters = (attrs.get("component") or {}).get("filters")
                if filters:
                    return filters[0]["options"]
        for included in payload.get("included", []):
            component = (included.get("attributes") or {}).get("component") or {}
            for filt in component.get("filters") or []:
                if filt.get("options"):
                    return filt["options"]
        self.log.error(f" - Could not find the season list for '{content_type}'.")
        raise SystemExit(1)

    def _build_series(self, payload: dict, content_type: str, external_id: str, content_title: str) -> Series:
        options = self._season_filters(payload, content_type)
        wanted = "EXTRA" if self.extras else "EPISODE"
        collected: list[dict] = []
        seen: set = set()

        season_url = self.config["season_pages"].get(content_type, self.config["endpoints"]["showPages"])

        for option in options:
            parameter = option["parameter"]
            season_number = int(option["value"])
            data = self.session.get(self._route(season_url % (external_id, parameter))).json()
            image_map = self._image_map(data)

            for item in data.get("included", []):
                attrs = item.get("attributes") or {}
                if self.extras:
                    if attrs.get("materialType") != "EXTRA" or item["id"] in seen:
                        continue
                else:
                    if attrs.get("videoType") != "EPISODE":
                        continue
                    if int(attrs.get("seasonNumber", -1)) != int(parameter.split("=")[-1]):
                        continue
                seen.add(item["id"])
                enriched = self._attach_images(item, image_map)
                enriched["_season"] = season_number
                collected.append(enriched)

        if not collected:
            self.log.error(f" - No {wanted.lower()}s found for {external_id}.")
            raise SystemExit(1)

        collected.sort(key=lambda x: (x["_season"], x["attributes"].get("episodeNumber") or 0))
        year = self._year(collected[0]["attributes"])

        return Series([
            Episode(
                id_=item["id"],
                service=self.__class__,
                title=f"{content_title}: Extras" if self.extras else content_title,
                season=0 if self.extras else item["_season"],
                number=index if self.extras else item["attributes"]["episodeNumber"],
                name=item["attributes"].get("name"),
                year=year,
                data=item,
            )
            for index, item in enumerate(collected, start=1)
        ])

    def _drm_modules(self) -> list[dict]:
        drm = self.config.get("drm") or {}
        modules = []
        if drm.get("include_clearkey", True):
            modules.append({"drmKeySystem": "clearkey"})
        if self.playready:
            modules.append({
                "drmKeySystem": "playready",
                "maxSecurityLevel": drm.get("playready_security_level", "sl2000"),
            })
        else:
            modules.append({"drmKeySystem": "widevine"})
        return modules

    def _playback_body(self, edit_id: str) -> dict:
        cfg = self.device["playback"]
        hdr_formats = self.config["hdr_formats"]
        return {
            "appBundle": cfg["appBundle"],
            "consumptionType": "streaming",
            "deviceInfo": {
                "deviceId": cfg["deviceId"] or self.device_id,
                "browser": {"name": cfg["browser"], "version": cfg["browserVersion"]},
                "make": cfg["make"],
                "model": cfg["model"],
                "os": {"name": cfg["osName"], "version": cfg["osVersion"]},
                "platform": cfg["platform"],
                "deviceType": cfg["deviceType"],
                "player": {
                    "sdk": {"name": cfg["playerSdk"], "version": cfg["playerVersion"]},
                    "mediaEngine": {"name": cfg["mediaEngine"], "version": cfg["mediaEngineVersion"]},
                    "playerView": {"height": cfg["height"], "width": cfg["width"]},
                },
            },
            "editId": edit_id,
            "capabilities": {
                "manifests": {"formats": {"dash": {}}},
                "codecs": {
                    "video": {
                        "hdrFormats": hdr_formats,
                        "decoders": [
                            {
                                "maxLevel": "6.2", "codec": "h265",
                                "levelConstraints": {
                                    "width": {"min": 1920, "max": 3840},
                                    "height": {"min": 1080, "max": 2160},
                                    "framerate": {"min": 15, "max": 60},
                                },
                                "profiles": ["main", "main10"],
                            },
                            {
                                "maxLevel": "4.2", "codec": "h264",
                                "levelConstraints": {
                                    "width": {"min": 640, "max": 3840},
                                    "height": {"min": 480, "max": 2160},
                                    "framerate": {"min": 15, "max": 60},
                                },
                                "profiles": ["high", "main", "baseline"],
                            },
                        ],
                    },
                    "audio": {"decoders": [{"codec": "aac", "profiles": ["lc", "he", "hev2", "xhe"]}]},
                },
                "contentProtection": {"contentDecryptionModules": self._drm_modules()},
                "devicePlatform": {
                    "network": {
                        "lastKnownStatus": {"networkTransportType": "unknown"},
                        "capabilities": {"protocols": {"http": {"byteRangeRequests": True}}},
                    },
                    "videoSink": {
                        "lastKnownStatus": {
                            "width": cfg.get("sinkWidth", cfg["width"]),
                            "height": cfg.get("sinkHeight", cfg["height"]),
                        },
                        "capabilities": {
                            "colorGamuts": ["standard", "wide"],
                            "hdrFormats": hdr_formats,
                        },
                    },
                },
            },
            "gdpr": False,
            "firstPlay": False,
            "playbackContext": "watch",
            "playbackSessionId": str(uuid.uuid4()),
            "applicationSessionId": str(uuid.uuid4()),
            "userPreferences": {"videoQuality": "best"},
            "features": self.config.get("playback_features") or [],
        }

    def get_tracks(self, title: Title_T) -> Tracks:
        if self._saved_headers:
            self.session.headers.update(self._saved_headers)
            self._saved_headers = {}

        try:
            edit_id = title.data["relationships"]["edit"]["data"]["id"]
        except (KeyError, TypeError):
            self.log.error(f" - No edit ID on '{title}'.")
            raise SystemExit(1)

        response = self.session.post(self._route(self.config["endpoints"]["playbackInfo"]), json=self._playback_body(edit_id))
        if not response.ok:
            self.log.error(f" - Playback refused ({response.status_code}).")
            for error in self._api_errors(response):
                self.log.error(f"     {error}")
            raise SystemExit(1)
        playback = response.json()

        video_info = next(x for x in playback["videos"] if x["type"] == "main")
        title.language = Language.get(video_info["defaultAudioSelection"]["language"])
        title.data["info"] = video_info

        schemes = ((playback.get("drm") or {}).get("schemes") or {})
        self.wv_license_url = (schemes.get("widevine") or {}).get("licenseUrl") \
            or self.config["license_fallback"]["widevine"]
        self.pr_license_url = (schemes.get("playready") or {}).get("licenseUrl") \
            or self.config["license_fallback"]["playready"]
        if not schemes:
            self.log.debug(" + playbackInfo had no DRM block. Switching endpoints.")

        manifest_url = self._manifest_url(playback)
        self.log.info(f" + Manifest: {manifest_url}")

        dash = DASH.from_url(url=manifest_url, session=self.session)
        tracks = dash.to_tracks(language=title.language)

        tracks.videos = self._dedupe(tracks.videos)
        tracks.audio = self._dedupe(tracks.audio)

        tracks.subtitles.clear()
        for subtitle in self._stitch_subtitles(dash, title.language):
            tracks.add(subtitle)

        if self.vcodec:
            tracks.videos = [x for x in tracks.videos if x.codec in self.VIDEO_CODEC_MAP[self.vcodec]]
        if self.acodec:
            tracks.audio = [x for x in tracks.audio if (x.codec or "")[:4] == self.AUDIO_CODEC_MAP[self.acodec]]

        self._tag_ranges(tracks)
        self._tag_descriptive(tracks)

        if not self.no_attachments:
            self._add_cover(tracks, title)

        self._strip_download_headers()
        return tracks

    def _manifest_url(self, playback: dict) -> str:
        url = playback["fallback"]["manifest"]["url"]
        url = url.replace("fly", "akm").replace("gcp", "akm")
        return url.replace("_fallback", "")

    @staticmethod
    def _tag_ranges(tracks: Tracks) -> None:
        for track in tracks.videos:
            representation = (track.data.get("dash") or {}).get("representation")
            codec = representation.get("codecs", "") if representation is not None else ""
            if codec[:4] in ("dvh1", "dvhe"):
                track.dv = True
                track.range = Video.Range.DV
                parts = codec.split(".")
                track.hdr10 = len(parts) >= 2 and parts[1] == "08"
            else:
                track.dv = False
                track.hdr10 = track.range == Video.Range.HDR10

        dv_resolutions = {(t.width, t.height) for t in tracks.videos if t.dv}
        for track in tracks.videos:
            if track.hdr10 and not track.dv and (track.width, track.height) in dv_resolutions:
                if not getattr(track, "edition", None):
                    track.edition = []
                track.edition.append("DV P8 Hybrid")

        for track in tracks.subtitles:
            if not track.codec:
                track.codec = Subtitle.Codec.WebVTT

    @staticmethod
    def _tag_descriptive(tracks: Tracks) -> None:
        for track in tracks.audio:
            adaptation = (track.data.get("dash", {}) or {}).get("adaptation_set")
            if adaptation is None:
                continue
            role = adaptation.find("Role")
            if role is not None and role.get("value") in ("description", "alternative", "alternate"):
                track.descriptive = True

    def _add_cover(self, tracks: Tracks, title: Title_T) -> None:
        image_url = None
        images = (title.data or {}).get("_images") or []

        by_kind = {}
        for image in images:
            attrs = image.get("attributes") or {}
            source = attrs.get("src") or attrs.get("url")
            kind = str(attrs.get("kind", "")).lower()
            if source and kind not in by_kind:
                by_kind[kind] = source
        for kind in ("default", "tile", "episode", "cover"):
            if kind in by_kind:
                image_url = by_kind[kind]
                break

        if not image_url:
            info = (title.data or {}).get("info") or {}
            image_url = next((info[k] for k in ("thumbnailUrl", "imageUrl", "artworkUrl") if info.get(k)), None)
        if not image_url and by_kind:
            image_url = next(iter(by_kind.values()))
        if not image_url:
            return

        try:
            response = self.session.get(image_url)
            response.raise_for_status()
            mime = response.headers.get("Content-Type", "image/jpeg").split(";")[0].strip()
            extension = {"image/jpeg": ".jpg", "image/png": ".png", "image/webp": ".webp"}.get(mime, ".jpg")
            path = config.directories.temp / f"cover{extension}"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(response.content)
            tracks.add(Attachment(path=path, name="cover", mime_type=mime))
        except Exception as e:
            self.log.warning(f" - Could not attach cover art: {e}")

    def _strip_download_headers(self) -> None:
        headers = self.config["strip_before_download"]
        self._saved_headers = {h: self.session.headers[h] for h in headers if h in self.session.headers}
        for header in headers:
            self.session.headers.pop(header, None)

    def get_chapters(self, title: Title_T) -> Chapters:
        annotations = (title.data.get("info") or {}).get("annotations")
        if not annotations:
            return Chapters()
        return Chapters([
            Chapter(timestamp=0.0, name="Chapter 1"),
            Chapter(timestamp=float(annotations[0]["start"]), name="Credits"),
            Chapter(timestamp=float(annotations[0]["end"]), name="Chapter 2"),
        ])

    def get_widevine_license(self, *, challenge: bytes, title: Title_T, track: AnyTrack) -> Optional[Union[bytes, str]]:
        if not self.wv_license_url:
            return None
        response = self.session.post(
            url=self.wv_license_url,
            data=challenge,
            headers={"Content-Type": "application/octet-stream"},
        )
        response.raise_for_status()
        return response.content

    def get_playready_license(self, *, challenge: bytes, title: Title_T, track: AnyTrack) -> Optional[bytes]:
        if not self.pr_license_url:
            return None
        response = self.session.post(
            url=self.pr_license_url,
            data=challenge.decode("utf-8") if isinstance(challenge, bytes) else str(challenge),
            headers={
                "Content-Type": "text/xml; charset=utf-8",
                "SOAPAction": "http://schemas.microsoft.com/DRM/2007/03/protocols/AcquireLicense",
            },
        )
        response.raise_for_status()
        return response.content

    def _stitch_subtitles(self, dash, language) -> list[Subtitle]:
        groups = defaultdict(list)
        for period in dash.manifest.findall("Period"):
            for adaptation in period.findall("AdaptationSet"):
                if adaptation.get("contentType") != "text":
                    continue
                lang = adaptation.get("lang")
                if not lang:
                    continue
                role = adaptation.find("Role")
                label = adaptation.find("Label")
                key = (
                    lang,
                    role.get("value") if role is not None else "subtitle",
                    label.text if label is not None else "",
                )
                groups[key].append((period, adaptation))

        tracks = []
        for (lang, role_value, label_text), pairs in groups.items():
            first_period, first_adaptation = pairs[0]
            if first_adaptation.find("Representation") is None:
                continue

            combined = deepcopy(first_adaptation)
            representation = combined.find("Representation")
            template = representation.find("SegmentTemplate")
            if template is None:
                template = combined.find("SegmentTemplate")
                if template is None:
                    continue
                combined.remove(template)
                template = deepcopy(template)
                representation.append(template)

            segments = []
            for _, adaptation in pairs:
                rep = adaptation.find("Representation")
                if rep is None:
                    continue
                source = rep.find("SegmentTemplate")
                if source is None:
                    source = adaptation.find("SegmentTemplate")
                if source is None:
                    continue
                start = int(source.get("startNumber", 1))
                timeline = source.find("SegmentTimeline")
                if timeline is None:
                    continue
                for s in timeline.findall("S"):
                    segments.append((start, int(s.get("t", 0)), int(s.get("d", 0)), int(s.get("r", 0))))

            if segments:
                segments.sort(key=lambda x: x[0])
                merged = etree.Element("SegmentTimeline")
                for _, t, d, r in segments:
                    element = etree.SubElement(merged, "S")
                    element.set("t", str(t))
                    element.set("d", str(d))
                    if r > 0:
                        element.set("r", str(r))
                existing = template.find("SegmentTimeline")
                if existing is not None:
                    template.remove(existing)
                template.append(merged)
                template.set("startNumber", "1")
                template.set("endNumber", str(len(segments)))

            tracks.append(Subtitle(
                id_=hex(crc32(f"sub-{lang}-{role_value}-{label_text}".encode()))[2:],
                url=dash.url,
                codec=Subtitle.Codec.WebVTT,
                language=Language.get(lang),
                is_original_lang=bool(language and is_close_match(Language.get(lang), [language])),
                descriptor=Video.Descriptor.DASH,
                sdh=role_value == "caption" or "sdh" in label_text.lower(),
                forced=role_value in ("forced-subtitle", "forced_subtitle") or "forced" in label_text.lower(),
                name=Language.get(lang).display_name(),
                data={"dash": {
                    "manifest": dash.manifest,
                    "period": first_period,
                    "adaptation_set": combined,
                    "representation": representation,
                }},
            ))

        return tracks

    @staticmethod
    def _dedupe(items: list) -> list:
        if not items or isinstance(items[0].url, list):
            return items

        seen = {}
        for item in items:
            if hasattr(item, "width") and hasattr(item, "height"):
                key = f"{item.codec}_{item.width}x{item.height}_{item.bitrate}"
            elif hasattr(item, "channels"):
                key = f"{item.codec}_{item.language}_{item.bitrate}_{item.channels}"
            else:
                key = item.url
            seen.setdefault(key, item)
        return list(seen.values())