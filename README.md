# unshackle-services
Service scripts for [Unshackle](https://github.com/unshackle-dl/unshackle).

## Installation
<details closed> <summary><strong>1. Remote Services</strong></summary>

### 1. Add Remote Service API
Add:
```bash
remote_services:
  unshackle:
    url: https://services.nostalgic.cc
    api_key: "free"
    server_cdm: false
```
into ``\unshackle\unshackle\unshackle.yaml``

### 2. Verify Services:
Run ``uv run unshackle dl --remote -h``

### 3. Run
Run with ``uv run unshackle dl --remote ...``

</details>
<details closed> <summary><strong>2. Clone Repository</strong></summary>

### 1. Clone the repository
```bash
git clone https://github.com/n0stal6ic/unshackle-services.git
cd unshackle-services
```

### 2. Copy/Paste the service(s)

### Windows:
```batch
xcopy ".\*" "C:\path\to\unshackle\unshackle\services\" /E /I /H /Y
```

### Linux:
```bash
cp -r ./* ~/unshackle/unshackle/services/
```

### Install Location:
```bash
unshackle/
└── unshackle/
    └── services/
```
</details>

## Services

| Tag | Service | Security | Authorization |
|-----|---------|-------|-------|
| AMZM | [Amazon Music](https://music.amazon.com/) | FLAC@L3/SL2K | Cookies, Credentials |
| AMZN | [Amazon Prime Video](https://www.primevideo.com/) | UHD@L1 <br> FHD@Chrome <br> SD@L3 | Cookies, Credentials |
| ANGL | [Angel](https://angel.com/) | None | Cookies |
| AUDS | [Audius](https://audius.co/) | None | None |
| DEZR | [Deezer](https://www.deezer.com/) | None | Credentials |
| FOXO | [FOX One](https://www.fox.com/) | None | Cookies |
| HMAX | [HBO Max](https://play.hbomax.com/) | UHD@L1/SL3K <br> FHD@L3/SL2K | Cookies, Credentials |
| HULU | [Hulu](https://www.hulu.com) | UHD@L1/SL3K <br> FHD@L3/SL2K | Cookies |
| KNCA | [Knowledge Canada](https://www.knowledge.ca/) | None | None |
| KNKC | [Knowledge Kids Canada](https://www.knowledgekids.ca/) | None | None |
| KNPY | [Kanopy](https://www.kanopy.com/) | FHD@L3 | Cookies, Credentials |
| NFBC | [National Film Board <br> of Canada](https://www.nfb.ca/) | None | None |
| PBS | [PBS](https://www.pbs.org/) | None | Cookies |
| PBSK | [PBS Kids](https://pbskids.org/) | FHD@L3/None | None |
| PCOK | [Peacock](https://www.peacocktv.com/) | UHD@L1/SL3K <br> FHD@L3/SL150 | Cookies, Credentials |
| PHLO | [Philo](https://www.philo.com/) | FHD@L3 | Cookies |
| QOBZ | [Qobuz](https://play.qobuz.com/) | None | Credentials |
| SNDC | [SoundCloud](https://soundcloud.com/) | AAC@L3/None <br> MP3@None | Cookies |
| SPOT | [Spotify](https://open.spotify.com/) | FLAC@L1 <br> AAC@Chrome | Cookies, Credentials |
| TIDL | [Tidal](https://tidal.com/) | FLAC@L3/None | Credentials |
| XUMO | [XUMO Play](https://play.xumo.com/) | None | None |
