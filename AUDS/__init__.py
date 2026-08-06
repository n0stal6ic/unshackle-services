from __future__ import annotations
import re
from http.cookiejar import CookieJar
from typing import Any, Optional
import click
from unshackle.core.credential import Credential
from unshackle.core.music import MusicTrackOption
from unshackle.core.service import Service
from unshackle.core.titles import Music, Song, Titles_T
from unshackle.core.tracks import Audio, Chapters, Tracks
from unshackle.core.tracks.track import Track


class AUDS(Service):
    """
    Service code for Audius (https://audius.co)
    www.nostalgic.cc
    Authorization: None
    """

    ALIASES = ("AUDS", "audius")
    GROUP_AUDIO_DOWNLOADS = True

    TITLE_RE = r"(?:https?://(?:www\.)?audius\.co/)?(?P<path>[^\s?]+)"
    LOSSLESS_EXTS = ("wav", "flac", "aiff", "aif", "alac")

    @staticmethod
    @click.command(name="AUDS", short_help="https://audius.co", help=__doc__)
    @click.argument("title", type=str)
    @click.option(
        "-q", "--quality", "quality",
        type=click.Choice(["original", "mp3"], case_sensitive=False),
        default=None,
        help="original = The artist's uploaded file when downloads are enabled, else MP3. "
             "mp3 = Always the 320 kb/s transcode.",
    )
    @click.pass_context
    def cli(ctx, **kwargs):
        return AUDS(ctx, **kwargs)

    def __init__(self, ctx, title: str, quality: Optional[str] = None):
        super().__init__(ctx)
        self.title = title.strip()
        self.app_name = self.config.get("app_name", "unshackle")
        self.quality = (quality or self.config.get("default_quality", "original")).lower()

        m = re.fullmatch(self.TITLE_RE, self.title)
        if not m:
            self.log.error(" - Could not parse an Audius URL or track ID.")
            raise SystemExit(1)
        self.item_path = m.group("path").strip("/")

    def authenticate(self, cookies: Optional[CookieJar] = None, credential: Optional[Credential] = None) -> None:
        super().authenticate(cookies, credential)
        self.session.headers.update({"accept": "*/*", "origin": "https://audius.co",
                                     "referer": "https://audius.co/"})
        if cookies or credential:
            self.log.info(" + Authorized")

    def _api(self, path: str, params: Optional[dict] = None) -> Any:
        params = dict(params or {})
        params["app_name"] = self.app_name
        resp = self.session.get(f"{self.config['endpoints']['api']}{path}", params=params)
        if resp.status_code == 404:
            self.log.error(f" - Not found on Audius: {path}")
            raise SystemExit(1)
        if resp.status_code != 200:
            self.log.error(f" - Audius API error on {path}: {resp.status_code} {resp.text[:200]}")
            raise SystemExit(1)
        return resp.json().get("data")

    def get_titles(self) -> Titles_T:
        entity, kind = self._resolve()

        if kind == "track":
            song = self._build_song(entity, number=1, total=1)
            return Music(
                [song], kind="single", title=song.album, artist=song.album_artist or song.artist,
                year=song.year, total_tracks=1, artwork_url=song.artwork_url,
            )

        tracks = self._api(f"/playlists/{entity['id']}/tracks") or []
        if not tracks:
            self.log.error(f" - No playable tracks in '{entity.get('playlist_name')}'.")
            raise SystemExit(1)

        owner = (entity.get("user") or {}).get("name")
        album_title = entity.get("playlist_name") or "Unknown"
        is_album = bool(entity.get("is_album"))
        artwork = self._artwork(entity.get("artwork"))

        songs = [
            self._build_song(
                t, number=i, total=len(tracks),
                album_override=album_title,
                album_artist_override=owner if is_album else None,
                artwork_override=artwork,
            )
            for i, t in enumerate(tracks, start=1)
        ]
        return Music(
            songs, kind="album" if is_album else "playlist", title=album_title,
            artist=owner, year=self._year(entity.get("release_date") or entity.get("created_at")) or None,
            total_tracks=len(songs), artwork_url=artwork,
            owner=owner if not is_album else None,
            description=entity.get("description") or None,
        )

    def _resolve(self) -> tuple[dict, str]:
        path = self.item_path
        if "/" not in path and not path.startswith("http"):
            return self._api(f"/tracks/{path}"), "track"

        data = self._api("/resolve", {"url": f"https://audius.co/{path}"})
        if isinstance(data, list):
            if not data:
                self.log.error(f" - Audius could not resolve '{self.title}'.")
                raise SystemExit(1)
            data = data[0]
        if not isinstance(data, dict):
            self.log.error(f" - Unexpected resolve response for '{self.title}'.")
            raise SystemExit(1)
        return data, "playlist" if data.get("playlist_name") is not None else "track"

    def _build_song(self, track: dict, number: int, total: int,
                    album_override: Optional[str] = None,
                    album_artist_override: Optional[str] = None,
                    artwork_override: Optional[str] = None) -> Song:
        user = track.get("user") or {}
        artist = (user.get("name") or user.get("handle") or "Unknown Artist").strip()
        title = (track.get("title") or "Unknown").strip()
        album = (album_override or track.get("album_backlink") or title).strip()
        artwork = artwork_override or self._artwork(track.get("artwork"))
        year = self._year(track.get("release_date") or track.get("created_at")) or 1
        ext, lossless = self._pick_format(track)

        data = {
            "service": self.ALIASES[0],
            "track_id": track.get("id"),
            "title": title,
            "artist": artist,
            "album": album,
            "album_artist": album_artist_override or artist,
            "track_number": number,
            "total_tracks": total,
            "duration": int(track.get("duration") or 0),
            "genre": track.get("genre") or None,
            "year": year,
            "isrc": (track.get("isrc") or "").strip() or None,
            "copyright": track.get("copyright_line") or None,
            "license": track.get("license") or None,
            "mood": track.get("mood") or None,
            "bpm": track.get("bpm") or None,
            "artwork_url": artwork,
            "comment": f"https://audius.co{track.get('permalink')}" if track.get("permalink") else None,
            "ext": ext,
            "lossless": lossless,
            "is_downloadable": bool(track.get("is_downloadable")),
            "orig_filename": track.get("orig_filename") or None,
            "quality": 6 if lossless else 5,
        }
        return Song(
            id_=track.get("id"),
            service=self.__class__,
            name=title,
            artist=artist,
            album=album,
            track=int(number),
            disc=1,
            year=int(year),
            album_artist=album_artist_override or artist,
            release_type="album" if album_override else "single",
            total_tracks=int(total),
            genre=track.get("genre") or None,
            explicit=self._explicit(track),
            isrc=(track.get("isrc") or "").strip() or None,
            artwork_url=artwork,
            data=data,
        )

    def get_music_track_options(self, song: Song) -> list[MusicTrackOption]:
        data = song.data if isinstance(song.data, dict) else {}
        ext = str(data.get("ext") or "mp3")
        lossless = bool(data.get("lossless"))
        option = MusicTrackOption(
            codec=ext.upper(),
            channels=2.0,
            lossless=lossless,
            **({} if lossless else {"bitrate": 320000}),
        )
        option.duration = int(data.get("duration")) if data.get("duration") else None
        option.quality_label = f"{ext.upper()} (Original)" if lossless else "MP3 320 kb/s"
        option.explicit = bool(song.explicit)
        return [option]

    def get_tracks(self, song: Song) -> Tracks:
        data = song.data if isinstance(song.data, dict) else {}
        track_id = str(song.id)
        ext = str(data.get("ext") or "mp3")

        if data.get("lossless"):
            url = self.config["endpoints"]["download"].format(track_id=track_id)
            url = f"{url}?original=true&app_name={self.app_name}"
        else:
            url = self.config["endpoints"]["stream"].format(track_id=track_id)
            url = f"{url}?app_name={self.app_name}"

        codec = Audio.Codec.FLAC if ext == "flac" else None

        audio = Audio(
            url,
            language=song.language or self.config.get("language", "en"),
            codec=codec,
            bitrate=None if data.get("lossless") else 320000,
            channels=2,
            descriptor=Track.Descriptor.URL,
            id_=track_id,
            data={"ext": ext},
        )
        return Tracks([audio])

    def get_chapters(self, song: Song) -> Chapters:
        return Chapters()

    def on_track_downloaded(self, track: Any) -> None:
        try:
            path = getattr(track, "path", None)
            tdata = getattr(track, "data", None)
            if not path or not path.exists() or not isinstance(tdata, dict):
                return
            ext = tdata.get("ext")
            if not ext or path.suffix.lower() == f".{ext}":
                return
            new_path = path.with_suffix(f".{ext}")
            if new_path.exists():
                new_path.unlink()
            path.rename(new_path)
            track.path = new_path
        except Exception as e:
            self.log.debug(f"Extension rename skipped: {e}")

    def _pick_format(self, track: dict) -> tuple[str, bool]:
        if self.quality != "mp3" and track.get("is_downloadable"):
            ext = ""
            name = track.get("orig_filename") or ""
            if "." in name:
                ext = name.rsplit(".", 1)[-1].lower().strip()
            if not re.fullmatch(r"[a-z0-9]{2,5}", ext or ""):
                ext = "wav"
            return ext, ext in self.LOSSLESS_EXTS
        return "mp3", False

    def _artwork(self, artwork: Any) -> Optional[str]:
        if not isinstance(artwork, dict):
            return None
        for key in ("1000x1000", "480x480", "150x150"):
            url = artwork.get(key)
            if isinstance(url, str) and url:
                return re.sub(r"/[^/]+\.jpg$", "/original.jpg", url)
        return None

    @staticmethod
    def _year(value: Any) -> Optional[int]:
        m = re.match(r"(\d{4})", str(value or ""))
        return int(m.group(1)) if m else None

    @staticmethod
    def _explicit(track: dict) -> bool:
        warning = str(track.get("parental_warning_type") or "").lower()
        return "explicit" in warning