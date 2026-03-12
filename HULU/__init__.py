from __future__ import annotations
from http.cookiejar import CookieJar
from typing import Any
import re
import base64
import hashlib
import click
import requests
from click import Context
from pyplayready.cdm import Cdm as PlayReadyCdm
from unshackle.core.credential import Credential
from unshackle.core.manifests import DASH
from unshackle.core.service import Service
from unshackle.core.titles import Episode, Movie, Movies, Series
from unshackle.core.tracks import Chapter, Subtitle, Video


class HULU(Service):
    """
    Service code for the Hulu streaming service (https://hulu.com).

    \b
    Authorization: Cookies
    Security: UHD@L3, UHD@SL3000
    """

    ALIASES = ["HULU"]

    TITLE_RE = (
        r"^(?:https?://(?:www\.)?hulu\.com/(?P<type>movie|series)/)?(?:[a-z0-9-]+-)?"
        r"(?P<id>[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12})"
    )

    AUDIO_CODEC_MAP = {
        "AAC": "mp4a",
        "EC3": "ec-3",
    }

    @staticmethod
    @click.command(name="HULU", short_help="hulu.com")
    @click.argument("title", type=str)
    @click.option(
        "-mt", "--mpd-type",
        type=click.Choice(["new", "old"], case_sensitive=False),
        default="new",
        help="Which MPD type to use.",
    )
    @click.pass_context
    def cli(ctx, **kwargs):
        return HULU(ctx, **kwargs)

    def __init__(self, ctx: Context, title: str, mpd_type: str):
        self.title = title
        self.mpd_type = mpd_type
        self.vcodec = ctx.parent.params.get("vcodec")
        self.acodec = ctx.parent.params["acodec"]
        self.cdm = ctx.obj.cdm
        self.license_url_widevine: str | None = None
        self.license_url_playready: str | None = None
        super().__init__(ctx)

    def _fatal(self, msg: str) -> None:
        self.log.error(msg)
        raise SystemExit(1)

    @staticmethod
    def _safe_json(response) -> dict:
        try:
            return response.json()
        except Exception:
            return {}

    @staticmethod
    def _strip_duplicate_representations(mpd: str) -> str:
        seen: set[str] = set()

        def _dedupe(m: re.Match) -> str:
            rid = m.group(1)
            if rid in seen:
                return ""
            seen.add(rid)
            return m.group(0)

        return re.sub(
            r'<Representation[^>]*\bid="([^"]+)"[^>]*>.*?</Representation>',
            _dedupe,
            mpd,
            flags=re.DOTALL,
        )

    def get_titles(self, *, _force_movie: bool = False):
        if _force_movie:
            return self._get_movie()
        return self._get_series()

    def _get_movie(self):
        resp = self.session.get(self.config["endpoints"]["movie"].format(id=self.title))
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            info = self._safe_json(e.response)
            self._fatal(f" - Failed to get movie {self.title}: {info.get('message', e)}")

        title_data = resp.json()["details"]["vod_items"]["focus"]["entity"]
        return Movies([Movie(
            id_=self.title,
            service=self.__class__,
            name=title_data.get("name"),
            year=int(title_data["premiere_date"][:4]) if title_data.get("premiere_date") else None,
            language="en",
            data=title_data,
        )])

    def _get_series(self):
        resp = self.session.get(self.config["endpoints"]["series"].format(id=self.title))
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            info = self._safe_json(e.response)
            message = info.get("message", "")
            if e.response.status_code == 400 and "entity type" in message.lower():
                self.log.info(" - Detected movie UUID, retrying on movie endpoint.")
                return self.get_titles(_force_movie=True)
            self._fatal(f" - Failed to get series {self.title}: {message} [{info.get('code')}]")

        res = resp.json()
        season_data = next((x for x in res.get("components", []) if x.get("name") == "Episodes"), None)
        if not season_data:
            self._fatal(" - Failed to get episodes.")

        series = Series()
        for season in season_data.get("items", []):
            season_id_part = season.get("id", "").rsplit("::", 1)[-1]
            season_resp = self.session.get(
                self.config["endpoints"]["season"].format(id=self.title, season=season_id_part)
            )
            try:
                season_resp.raise_for_status()
            except requests.HTTPError as e:
                info = self._safe_json(e.response)
                self._fatal(f" - Failed to get season {season_id_part}: {info.get('message', e)}")

            for episode in season_resp.json().get("items", []):
                try:
                    ep_season = int(episode["season"]) if episode.get("season") is not None else None
                    ep_number = int(episode["number"]) if episode.get("number") is not None else None
                except (ValueError, TypeError):
                    ep_season = episode.get("season")
                    ep_number = episode.get("number")

                series.add(Episode(
                    id_=f"{season.get('id')}::{episode.get('season')}::{episode.get('number')}",
                    service=self.__class__,
                    title=episode.get("series_name"),
                    season=ep_season,
                    number=ep_number,
                    name=episode.get("name"),
                    language="en",
                    data=episode,
                ))

        return series

    def get_tracks(self, title):
        if self.vcodec == Video.Codec.HEVC:
            codec = "H265"
        elif self.vcodec == Video.Codec.AVC:
            codec = "H264"
        else:
            self.log.warning(f" - Unrecognised vcodec '{self.vcodec}', defaulting to H264.")
            codec = "H264"

        eab_id = (title.data.get("bundle") or {}).get("eab_id")
        if not eab_id:
            self._fatal(f" - Could not find eab_id in title data for '{title}'.")

        device_cfg = self.config["device_ids"]
        deejay_id = device_cfg["new"] if self.mpd_type == "new" else device_cfg["old"]
        version = 1 if self.mpd_type == "new" else 9999999

        try:
            resp = self.session.post(
                url=self.config["endpoints"]["manifest"],
                json={
                    "deejay_device_id": deejay_id,
                    "version": version,
                    "all_cdn": False,
                    "content_eab_id": eab_id,
                    "region": "US",
                    "language": "en",
                    "unencrypted": True,
                    "network_mode": "wifi",
                    "play_intent": "resume",
                    "playback": {
                        "version": 2,
                        "video": {
                            "dynamic_range": "DOLBY_VISION",
                            "codecs": {
                                "values": [x for x in self.config["codecs"]["video"] if x["type"] == codec],
                                "selection_mode": self.config["codecs"]["video_selection"],
                            },
                        },
                        "audio": {
                            "codecs": {
                                "values": self.config["codecs"]["audio"],
                                "selection_mode": self.config["codecs"]["audio_selection"],
                            },
                        },
                        "drm": {
                            "multi_key": True,
                            "values": (
                                self.config["drm"]["schemas_pr"]
                                if isinstance(self.cdm, PlayReadyCdm)
                                else self.config["drm"]["schemas_wv"]
                            ),
                            "selection_mode": self.config["drm"]["selection_mode"],
                            "hdcp": self.config["drm"]["hdcp"],
                        },
                        "manifest": {
                            "type": "DASH",
                            "https": True,
                            "multiple_cdns": False,
                            "patch_updates": True,
                            "hulu_types": True,
                            "live_dai": True,
                            "secondary_audio": True,
                            "live_fragment_delay": 3,
                        },
                        "segments": {
                            "values": [{
                                "type": "FMP4",
                                "encryption": {"mode": "CENC", "type": "CENC"},
                                "https": True,
                            }],
                            "selection_mode": "ONE",
                        },
                    },
                },
            )
            resp.raise_for_status()
            playlist = resp.json()
        except requests.HTTPError as e:
            info = self._safe_json(e.response)
            self._fatal(f" - Failed to fetch manifest: {info.get('message', e)} ({info.get('code')})")
        except ValueError as e:
            self._fatal(f" - Failed to decode manifest JSON: {e}")

        if "stream_url" not in playlist:
            self._fatal(f" - Manifest response missing 'stream_url'. Keys: {list(playlist.keys())}")

        self.license_url_widevine = playlist.get("wv_server")
        self.license_url_playready = playlist.get("dash_pr_server")

        manifest = playlist["stream_url"]
        self.log.info(f"DASH: {manifest}")

        mpd_resp = self.session.get(manifest)
        mpd_resp.raise_for_status()
        mpd_text = self._strip_duplicate_representations(mpd_resp.text)
        tracks = DASH.from_text(mpd_text, manifest).to_tracks(title.language)

        if self.acodec:
            mapped = self.AUDIO_CODEC_MAP.get(self.acodec)
            if mapped:
                tracks.audio = [x for x in tracks.audio if (x.codec or "").startswith(mapped)]

        for track in tracks.audio:
            if track.bitrate > 768_000:
                track.bitrate = 768_000
            if track.channels == 6.0:
                track.channels = 5.1

        for sub_lang, sub_url in playlist.get("transcripts_urls", {}).get("webvtt", {}).items():
            tracks.add(Subtitle(
                id_=hashlib.md5(sub_url.encode()).hexdigest()[:6],
                url=sub_url,
                codec=Subtitle.Codec.from_mime("vtt"),
                language=sub_lang,
                forced=False,
                sdh=False,
            ))

        return tracks

    def get_chapters(self, title: Movie | Episode) -> list[Chapter]:
        return []

    def get_widevine_license(self, challenge, track, **_: Any):
        try:
            resp = self.session.post(
                url=self.license_url_widevine,
                data=challenge,
                headers={"Content-Type": "application/octet-stream"},
            )
            resp.raise_for_status()
        except requests.HTTPError as e:
            self._fatal(f" - Widevine license request failed: {e}")

        self.log.debug(
            f"License HTTP {resp.status_code}, "
            f"Content-Type: {resp.headers.get('Content-Type')}, "
            f"Body[:80]: {resp.content[:80]}"
        )

        ct = resp.headers.get("Content-Type", "")
        if "json" in ct or resp.content[:1] == b"{":
            try:
                data = resp.json()
                for key in ("license", "license_data", "licenseData"):
                    if key in data:
                        return base64.b64decode(data[key])
            except Exception:
                pass

        return resp.content

    def get_playready_license(self, challenge, track, **_: Any):
        try:
            resp = self.session.post(url=self.license_url_playready, data=challenge)
            resp.raise_for_status()
        except requests.HTTPError as e:
            self._fatal(f" - PlayReady license request failed: {e}")

        self.log.debug(resp.content)
        return resp.content

    def authenticate(self, cookies: CookieJar | None = None, credential: Credential | None = None) -> None:
        if cookies:
            self.session.cookies.update(cookies)
        self.session.headers.update({"User-Agent": self.config["user_agent"]})
