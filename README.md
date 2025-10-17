# YouTube Downloader Bot — Telegram (Video + Audio)

Fast, cache-aware YouTube downloader bot for Telegram.  
Supports **video** (MP4/MKV/WEBM up to 1080p) and **audio** (MP3/M4A at selectable bitrates), force-join/download-lock, maintenance mode, whitelist, admin panel, broadcast, and smart cache with TTL cleanup.

> Core libs used in code: **pyrogram, tgcrypto, yt-dlp, requests**.

---

## ✨ Features

- Send a YouTube link → get **Video** (quality & format picker) or **Audio** (format & bitrate picker).  
- **Smart cache** (stores converted files; shows cache hits/misses; TTL cleanup on start).  
- **Force-Join / Download-Lock** (multi-channel), **Maintenance mode**, **Whitelist**, **Admin panel**, **Broadcast**.  
- **Progress UI**, cancel during send, file-size guard (2GB).  

---

## 📦 Requirements

### System package (must install on OS)
- **FFmpeg** (used by yt-dlp post-processors to merge/convert video/audio).

### Python (install via pip)
Create `requirements.txt` (or use these directly):

```
pyrogram==2.0.106
tgcrypto==1.2.5
yt-dlp>=2024.4.9
requests>=2.31.0
# optional for .env support
python-dotenv>=1.0.1
```

---

## 🛠 Installation

### Windows (PowerShell)
```ps1
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
winget install Gyan.FFmpeg
```

### Linux (Debian/Ubuntu)
```bash
sudo apt update
sudo apt install -y ffmpeg
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Termux (Android)
```bash
pkg update
pkg install -y ffmpeg python
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
and 

pkg update && pkg upgrade -y
pkg install python ffmpeg -y

cd ~/Youtube-Downloader-Bot
python -m venv .venv
source .venv/bin/activate          # activate
python -m pip install --upgrade pip wheel
pip install -r requirements.txt     # agar file hai
# ya
pip install pyrogram tgcrypto yt-dlp requests

python YouTubeDownloaderBot.py

### macOS
```bash
brew install ffmpeg
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## 🔐 Configuration

The bot reads/writes a persistent `config.json` on first run with defaults for **force_join**, **required_channels**, **admins**, **maintenance_mode**, **whitelist**, **cache_ttl_days**, and **download_lock**.

Example (minimal):
```json
{
  "force_join": true,
  "required_channels": ["@YourChannelUsername"],
  "invite_link": null,
  "admins": [123456789],
  "admin_bypass": true,
  "enforce_for_admins": false,
  "maintenance_mode": false,
  "whitelist": [],
  "stats": {"users":[], "downloads":0, "cache_hits":0, "cache_misses":0, "last_reset": null, "last_seen": {}},
  "cache_ttl_days": 7,
  "download_lock": true
}
```

---

## 🔑 Bot credentials

In script:
```python
API_ID = 23292615
API_HASH = "...."
BOT_TOKEN = "...."
```

**Better:** Load from environment variables.

```python
import os
API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
```

---

## ▶️ Run

```bash
python YouTubeDownloaderBot.py
```

---

## 📁 Repo Structure

```
.
├─ YouTubeDownloaderBot.py
├─ requirements.txt
├─ config.json
├─ downloads/
│  ├─ cache/
│  └─ temp files...
└─ README.md
```

---

## 📝 License & Fair Use

The code is for downloading content **you have rights to**. Respect YouTube/Telegram terms and local laws.
