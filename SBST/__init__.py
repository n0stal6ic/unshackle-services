from __future__ import annotations
import re
from http.cookiejar import CookieJar
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse
import click
from unshackle.core.credential import Credential
from unshackle.core.manifests import HLS
from unshackle.core.service import Service
from unshackle.core.titles import Movie, Movies, Title_T, Titles_T
from unshackle.core.tracks import Audio, Chapters, Track, Tracks


class SBST(Service):
    """
    Substack - https://substack.com
    www.nostalgic.cc
    Authorization: Cookies
    Security: None
    """

    ALIASES = ("substack", "SUBSTACK")
    TITLE_RE = r"^https?://(?P<host>[A-Za-z0-9.-]+)/p/(?P<slug>[A-Za-z0-9._-]+)"

    @staticmethod
    @click.command(name="SBST", short_help="https://substack.com", help=__doc__)
    @click.argument("title", type=str)
    @click.option("-a", "--audio", "audio_only", is_flag=True, default=False,
                  help="Take the podcast audio even when the post also has video.")
    @click.pass_context
    def cli(ctx: click.Context, **kwargs: Any) -> "SBST":
        return SBST(ctx, **kwargs)

    def __init__(self, ctx: click.Context, title: str, audio_only: bool = False) -> None:
        self.title = title
        self.audio_only = audio_only
        super().__init__(ctx)

        self.session.headers.update({"User-Agent": self.config["user_agent"]})
        self.signed_in = False

    def _parse(self) -> tuple[str, str]:
        match = re.match(self.TITLE_RE, self.title.strip())
        if not match:
            raise click.ClickException(
                f"Could not parse a Substack post URL from {self.title!r}. "
                "Expected https://<publication>/p/<slug>"
            )
        return match.group("host"), match.group("slug")

    def authenticate(self, cookies: Optional[CookieJar] = None, credential: Optional[Credential] = None) -> None:
        super().authenticate(cookies, credential)

        if credential:
            self.log.warning(" - SBST signs in with cookies.")

        self.signed_in = any(c.name == "connect.sid" and c.value for c in self.session.cookies)
        if self.signed_in:
            self.log.info(" + Using the publication session cookie")
        else:
            self.log.warning(
                " - No connect.sid cookie. A paid post will resolve to its free preview. "
                "Export the publication's cookies to the SBST cookie jar."
            )

    def get_titles(self) -> Titles_T:
        host, slug = self._parse()

        response = self.session.get(self.config["endpoints"]["post"].format(host=host, slug=slug))
        if response.status_code == 404:
            raise click.ClickException(f"No post at https://{host}/p/{slug}")
        response.raise_for_status()
        post = response.json()

        video = post.get("videoUpload") or {}
        data = {
            "host": host,
            "slug": slug,
            "video_upload_id": post.get("video_upload_id") or video.get("id"),
            "playback_id": video.get("mux_playback_id"),
            "preview_playback_id": video.get("mux_preview_playback_id"),
            "podcast_url": post.get("podcast_url"),
            "podcast_duration": post.get("podcast_duration"),
            "audience": post.get("audience"),
            "post_type": post.get("type"),
        }

        if not data["video_upload_id"] and not data["podcast_url"]:
            raise click.ClickException(
                f"That post has no video or podcast audio (type={post.get('type')!r}). "
                "SBST only handles posts carrying media."
            )

        year = None
        match = re.match(r"(\d{4})", str(post.get("post_date") or ""))
        if match:
            year = int(match.group(1))

        name = post.get("title") or slug.replace("-", " ").title()
        self.log.info(f" + {name} ({year or 'n/a'}) [{post.get('type')}, {post.get('audience')}]")
        if data["audience"] not in (None, "everyone") and not self.signed_in:
            self.log.warning(f" - This post is {data['audience']} and without cookies you get a preview.")

        return Movies([
            Movie(
                id_=str(post.get("id") or slug),
                service=self.__class__,
                name=name,
                year=year,
                language=post.get("language") or "en",
                description=post.get("subtitle") or post.get("description"),
                data=data,
            )
        ])

    def get_tracks(self, title: Title_T) -> Tracks:
        data = title.data or {}

        if not self.audio_only and data.get("video_upload_id"):
            return self._video_tracks(title, data)

        if data.get("podcast_url"):
            return Tracks([self._audio_track(title, data)])

        raise click.ClickException("Nothing downloadable on this post")

    def _video_tracks(self, title: Title_T, data: dict) -> Tracks:
        source = self._signed_source(data)
        tracks = HLS.from_url(url=source, session=self.session).to_tracks(language=title.language)
        if not tracks.videos:
            raise click.ClickException("The Mux playlist carried no video tracks")
        return tracks

    def _signed_source(self, data: dict) -> str:
        url = self.config["endpoints"]["video_src"].format(
            host=data["host"], video_id=data["video_upload_id"]
        )
        params = dict(self.config.get("video_params") or {})

        response = self.session.get(url, params=params, allow_redirects=False)
        location = response.headers.get("Location")
        if not location:
            raise click.ClickException(
                f"Substack did not hand back a video URL (HTTP {response.status_code}). "
                "The post may be paywalled and the cookie missing or stale."
            )

        landed = urlparse(location).path.lstrip("/").split("/")[0].split(".")[0]
        preview = data.get("preview_playback_id")
        if preview and landed == preview:
            self.log.warning(
                " - Resolved to the FREE PREVIEW asset, not the full post. "
                "Supply the publication's connect.sid cookie for the full video."
            )
        elif data.get("playback_id") and landed == data["playback_id"]:
            self.log.info(" + Resolved to the full asset")

        return location

    def _audio_track(self, title: Title_T, data: dict) -> Audio:
        codec = str((self.config.get("audio") or {}).get("default_codec") or "MP3").upper()
        return Audio(
            data["podcast_url"],
            language=title.language,
            codec=getattr(Audio.Codec, codec, Audio.Codec.MP3),
            descriptor=Track.Descriptor.URL,
            id_=f"{title.id}-audio",
            data={"substack": "podcast"},
        )

    def get_chapters(self, title: Title_T) -> Chapters:
        return Chapters()