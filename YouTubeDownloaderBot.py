# -*- coding: utf-8 -*-
# YouTube Downloader Bot (Video: MP4/MKV/WEBM + Audio: MP3/M4A)
# Requires: pyrogram tgcrypto yt-dlp requests
# NOTE: Only download content you have rights for.

import os
import re
import json
import time
import glob
import hashlib
import asyncio
import requests
from datetime import datetime, timedelta
from functools import wraps
from typing import Dict, List, Tuple, Optional

import yt_dlp
from pyrogram import Client, filters
from pyrogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, ForceReply, Message
)
from pyrogram.errors import UserNotParticipant, ChatAdminRequired, ChatWriteForbidden, ChatIdInvalid

# ================== BOT CONFIG (YOUR DATA) ==================
API_ID = 23292615
API_HASH = "fc15ff59f3a1d77e4d86ff6f3ded9d44"
BOT_TOKEN = "8398662962:AAHD2YAC2BEovMdqg6XM-E8CYsez46U_mjY"

bot = Client("yt_quality_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024  # 2GB
os.makedirs("downloads", exist_ok=True)
CACHE_DIR = "downloads/cache"
os.makedirs(CACHE_DIR, exist_ok=True)

# ================== PERSISTENT CONFIG ==================
CONFIG_PATH = "config.json"
DEFAULT_CONFIG = {
    "force_join": True,
    "required_channels": ["@YourChannelUsername"],  # e.g., ["@ChannelOne", "@ChannelTwo"]
    "invite_link": None,                            # set if private channel (permanent invite link)
    "admins": [8070535163],                         # your admin ID(s)
    "admin_bypass": True,                           # admins bypass join checks by default
    "enforce_for_admins": False,                    # if True, even admins must join
    "maintenance_mode": False,                      # blocks non-admins unless whitelisted
    "whitelist": [],                                # testers allowed during maintenance
    "stats": {
        "users": [],                                # known user IDs
        "downloads": 0,                             # total files sent
        "cache_hits": 0,                            # served from cache
        "cache_misses": 0,                          # downloaded fresh
        "last_reset": None,
        "last_seen": {}                             # user_id -> ISO timestamp
    },
    "cache_ttl_days": 7,                            # auto-delete cache older than N days
    "download_lock": True                           # NEW: gate downloads behind channel join
}

def save_config(cfg):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)

def load_config():
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    save_config(DEFAULT_CONFIG)
    return DEFAULT_CONFIG

CONFIG = load_config()

# ================== IN-MEMORY STATE ==================
url_store: Dict[str, str] = {}          # link_uid -> url
active_tasks: Dict[int, Dict[str, Optional[str]]] = {}   # download_msg_id -> {file, thumb}
user_queues: Dict[int, asyncio.Queue] = {}               # user_id -> queue
user_tasks: Dict[int, asyncio.Task] = {}                 # user_id -> current task
user_locks: Dict[int, asyncio.Lock] = {}                 # user_id -> lock
last_click_at: Dict[Tuple[int, str], float] = {}         # (user_id, key) -> ts
ADMIN_WAIT: Dict[int, str] = {}                          # admin pending action
BCAST_DRAFT: Dict[int, dict] = {}                        # admin id -> broadcast draft

# ================== HELPERS ==================
def is_admin(user_id: int) -> bool:
    try:
        return int(user_id) in set(CONFIG.get("admins", []))
    except Exception:
        return False

def is_whitelisted(user_id: int) -> bool:
    try:
        return int(user_id) in set(CONFIG.get("whitelist", []))
    except Exception:
        return False

def add_user_stat(user_id: int):
    users = set(CONFIG["stats"].get("users", []))
    if user_id not in users:
        users.add(user_id)
        CONFIG["stats"]["users"] = list(users)
    # update last_seen
    try:
        CONFIG["stats"]["last_seen"][str(user_id)] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    except Exception:
        pass
    save_config(CONFIG)

def inc_downloads(n=1):
    CONFIG["stats"]["downloads"] = int(CONFIG["stats"].get("downloads", 0)) + n
    save_config(CONFIG)

def inc_cache(hit: bool):
    key = "cache_hits" if hit else "cache_misses"
    CONFIG["stats"][key] = int(CONFIG["stats"].get(key, 0)) + 1
    save_config(CONFIG)

def hash_url(s: str) -> str:
    return hashlib.md5(s.encode()).hexdigest()

def progress_bar(current, total, width=20):
    if total <= 0:
        return "[{}] 0%".format("░" * width)
    p = max(0.0, min(1.0, current / total))
    filled = int(p * width)
    return "[{}{}] {:.1f}%".format("█" * filled, "░" * (width - filled), p * 100.0)

def download_thumbnail(url, path):
    try:
        if not url:
            return None
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            with open(path, "wb") as f:
                f.write(r.content)
            return path
    except Exception:
        pass
    return None

def clean_youtube_url(url: str) -> str:
    # normalize Shorts URLs
    return url.replace("youtube.com/shorts/", "youtube.com/watch?v=")

def retry(times=3, delay=3):
    def dec(fn):
        @wraps(fn)
        async def wrapper(*a, **k):
            for i in range(times):
                try:
                    return await fn(*a, **k)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    if i < times - 1:
                        await asyncio.sleep(delay)
                    else:
                        raise
        return wrapper
    return dec

async def enqueue_task(user_id, task_coro):
    if user_id not in user_queues:
        user_queues[user_id] = asyncio.Queue()
        asyncio.create_task(user_worker(user_id))
    await user_queues[user_id].put(task_coro)

async def user_worker(user_id):
    q = user_queues[user_id]
    lock = user_locks.setdefault(user_id, asyncio.Lock())
    while True:
        task_coro = await q.get()
        async with lock:
            t = asyncio.create_task(task_coro)
            user_tasks[user_id] = t
            try:
                await t
            except asyncio.CancelledError:
                pass
            except Exception as e:
                print("User task error:", e)
            finally:
                user_tasks.pop(user_id, None)
        q.task_done()
        if q.empty():
            user_queues.pop(user_id, None)
            break

async def safe_edit(msg, text=None, reply_markup=None):
    try:
        if text is None:
            await msg.edit_reply_markup(reply_markup=reply_markup)
        else:
            await msg.edit(text, reply_markup=reply_markup)
    except Exception:
        pass  # benign edit races

def is_fast_double_click(user_id: int, key: str, min_gap=0.35) -> bool:
    now = time.monotonic()
    prev = last_click_at.get((user_id, key), 0.0)
    last_click_at[(user_id, key)] = now
    return (now - prev) < min_gap

def parse_buttons_from_text(text: str) -> Optional[InlineKeyboardMarkup]:
    """
    Parse a simple "[Buttons]" block:
    Example:
        New update is live! 🚀

        [Buttons]
        Website | https://example.com
        Join Channel | https://t.me/yourchannel
    """
    if not text or "[Buttons]" not in text:
        return None
    head, btns = text.split("[Buttons]", 1)
    rows = []
    for line in btns.strip().splitlines():
        if "|" in line:
            title, url = [x.strip() for x in line.split("|", 1)]
            if title and url:
                rows.append([InlineKeyboardButton(title, url=url)])
    return InlineKeyboardMarkup(rows) if rows else None

def cleanup_cache(ttl_days: int = None):
    """Remove cache files older than TTL days."""
    ttl = ttl_days or int(CONFIG.get("cache_ttl_days", 7))
    cutoff = time.time() - (ttl * 86400)
    removed = 0
    for path in glob.glob(os.path.join(CACHE_DIR, "*")):
        try:
            if os.path.isfile(path) and os.path.getmtime(path) < cutoff:
                os.remove(path)
                removed += 1
        except Exception:
            pass
    if removed:
        print(f"[CACHE] Cleaned {removed} old file(s)")

# ================== FORCE-JOIN (JOIN GATE) ==================
def join_kb():
    """
    Multi-channel join UI:
    - One "Join <channel>" button per required channel
    - Recheck button at the end
    If channel is '@username' → https://t.me/username
    If '-100...' (ID)       → use invite_link or fallback to https://t.me/
    """
    chans = CONFIG.get("required_channels", [])
    buttons = []
    for ch in chans:
        url = None
        if isinstance(ch, str) and ch.startswith("@"):
            url = "https://t.me/" + ch.lstrip("@")
        elif str(ch).startswith("-100"):
            url = CONFIG.get("invite_link") or "https://t.me/"
        else:
            url = CONFIG.get("invite_link") or "https://t.me/"
        buttons.append([InlineKeyboardButton(f"📢 Join {ch}", url=url)])
    buttons.append([InlineKeyboardButton("✅ I’ve joined, Recheck", callback_data="recheck_join")])
    return InlineKeyboardMarkup(buttons)

async def is_user_member(client: Client, user_id: int) -> bool:
    """
    Existing gate used on /start (respects CONFIG.force_join).
    """
    if not CONFIG.get("force_join", True):
        return True
    chans = CONFIG.get("required_channels", [])
    if not chans:
        return True
    for chan in chans:
        try:
            member = await client.get_chat_member(chan, user_id)
            if member.status not in ("member", "administrator", "creator"):
                return False
        except UserNotParticipant:
            return False
        except (ChatAdminRequired, ChatIdInvalid, ChatWriteForbidden):
            return False
        except Exception:
            return False
    return True

async def is_member_of_required_channels(client: Client, user_id: int) -> bool:
    """
    Download Lock checker with admin bypass.
    Admins bypass if CONFIG.admin_bypass=True and CONFIG.enforce_for_admins=False
    """
    # --- Admin bypass ---
    if is_admin(user_id) and CONFIG.get("admin_bypass", True) and not CONFIG.get("enforce_for_admins", False):
        return True

    chans = CONFIG.get("required_channels", [])
    if not chans:
        return True
    for chan in chans:
        try:
            member = await client.get_chat_member(chan, user_id)
            if member.status not in ("member", "administrator", "creator"):
                return False
        except UserNotParticipant:
            return False
        except (ChatAdminRequired, ChatIdInvalid, ChatWriteForbidden):
            return False
        except Exception:
            return False
    return True

# ================== /start ==================
def _log(msg):
    try:
        print("[START]", msg)
    except Exception:
        pass

@bot.on_message(filters.command("start", prefixes=["/"]) & filters.private)
async def start_cmd(client, message):
    """Clean welcome (no maintenance/force-join badges) with force-join gate applied before."""
    _log(f"/start from {message.from_user.id} ({message.from_user.first_name})")

    uid = message.from_user.id
    is_adm = is_admin(uid)
    is_wl = is_whitelisted(uid)

    # Maintenance Mode gate (admins & whitelist bypass)
    if CONFIG.get("maintenance_mode", False) and not (is_adm or is_wl):
        return await message.reply("🛠 Under Maintenance\n\nWe’re upgrading the bot. Please try again later.")

    add_user_stat(uid)

    # Force-Join gate (unless admin bypass)
    try:
        chans = CONFIG.get("required_channels", [])
        gate = CONFIG.get("force_join", True)
        enforce_admins = CONFIG.get("enforce_for_admins", False)
        admin_bypass = CONFIG.get("admin_bypass", True) and not enforce_admins

        if gate and (not (is_adm and admin_bypass)) and chans:
            ok = True
            for chan in chans:
                try:
                    m = await client.get_chat_member(chan, uid)
                    if m.status not in ("member", "administrator", "creator"):
                        ok = False
                        break
                except Exception:
                    ok = False
                    break
            if not ok:
                req = ", ".join([str(x) for x in chans])
                return await message.reply(
                    f"🔒 Access Locked\n\nPlease join all required channels to use the bot.\nRequired: {req}\n\n👉 After joining, tap “I’ve joined, Recheck”.",
                    reply_markup=join_kb(), disable_web_page_preview=True
                )
    except Exception as e:
        _log(f"force-join check error: {e}")

    # Clean, simple welcome (NO status badges)
    return await message.reply(
        "✨ Welcome to YouTube Downloader\n"
        "🎥 Paste a YouTube link & get your video or audio instantly.\n\n"
        "👇 Send your link below!"
    )

# Fallback for odd clients that send 'start' text
@bot.on_message((filters.regex(r"^/start(@[A-Za-z0-9_]+)?$") | filters.regex(r"^start$", re.IGNORECASE)) & filters.private)
async def start_fallback(client, message):
    return await start_cmd(client, message)

# ================== ADMIN PANEL ==================
def admin_main_kb():
    rows = [
        [InlineKeyboardButton("🔒 Force-Join: " + ("ON ✅" if CONFIG.get("force_join", True) else "OFF ❌"),
                              callback_data="adm_toggle_force")],
        [InlineKeyboardButton("🛠 Maintenance: " + ("ON ✅" if CONFIG.get("maintenance_mode", False) else "OFF ❌"),
                              callback_data="adm_toggle_maint")],
        [InlineKeyboardButton("🔓 Download Lock: " + ("ON ✅" if CONFIG.get("download_lock", True) else "OFF ❌"),
                              callback_data="adm_toggle_dllock")],  # NEW
        [InlineKeyboardButton("👮 Enforce for Admins: " + ("ON ✅" if CONFIG.get("enforce_for_admins", False) else "OFF ❌"),
                              callback_data="adm_toggle_enfadm")],   # NEW
        [InlineKeyboardButton("🧪 Whitelist", callback_data="adm_wl")],
        [InlineKeyboardButton("📢 Required Channels", callback_data="adm_channels"),
         InlineKeyboardButton("🔗 Invite Link", callback_data="adm_invite")],
        [InlineKeyboardButton("📬 Broadcast", callback_data="adm_bcast"),
         InlineKeyboardButton("📊 Stats", callback_data="adm_stats")],
        [InlineKeyboardButton("🔧 Admins", callback_data="adm_admins")]
    ]
    return InlineKeyboardMarkup(rows)

def channels_kb():
    buttons = []
    for ch in CONFIG.get("required_channels", []):
        buttons.append([InlineKeyboardButton(f"❌ Remove {ch}", callback_data=f"adm_remchan|{ch}")])
    buttons.append([InlineKeyboardButton("➕ Add Channel", callback_data="adm_addchan"),
                    InlineKeyboardButton("⬅️ Back", callback_data="adm_back")])
    return InlineKeyboardMarkup(buttons)

def invite_kb():
    cur = CONFIG.get("invite_link")
    rows = [[InlineKeyboardButton("✏️ Set/Change Invite Link", callback_data="adm_setinv")]]
    if cur:
        rows.append([InlineKeyboardButton("🗑️ Clear Invite Link", callback_data="adm_clrinv")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="adm_back")])
    return InlineKeyboardMarkup(rows)

def admins_kb():
    rows = []
    for uid in CONFIG.get("admins", []):
        rows.append([InlineKeyboardButton(f"❌ Remove {uid}", callback_data=f"adm_remadmin|{uid}")])
    rows.append([InlineKeyboardButton("➕ Add Admin", callback_data="adm_addadmin"),
                 InlineKeyboardButton("⬅️ Back", callback_data="adm_back")])
    return InlineKeyboardMarkup(rows)

def whitelist_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add User ID", callback_data="adm_wl_add"),
         InlineKeyboardButton("❌ Remove User ID", callback_data="adm_wl_rem")],
        [InlineKeyboardButton("⬅️ Back", callback_data="adm_back")]
    ])

def bcast_segment_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("All Users", callback_data="bseg_all")],
        [InlineKeyboardButton("Active (7d)", callback_data="bseg_active7")],
        [InlineKeyboardButton("Whitelisted", callback_data="bseg_wl")],
        [InlineKeyboardButton("❌ Cancel", callback_data="bseg_cancel")]
    ])

def bcast_schedule_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Send Now", callback_data="bsched_now")],
        [InlineKeyboardButton("In 15 min", callback_data="bsched_15")],
        [InlineKeyboardButton("In 1 hour", callback_data="bsched_60")],
        [InlineKeyboardButton("❌ Cancel", callback_data="bsched_cancel")]
    ])

@bot.on_message(filters.command("admin") & filters.private)
async def admin_cmd(client, message):
    if not is_admin(message.from_user.id):
        return await message.reply("🚫 You are not an admin.")
    users = CONFIG["stats"].get("users", [])
    dls = CONFIG["stats"].get("downloads", 0)
    hits = CONFIG["stats"].get("cache_hits", 0)
    misses = CONFIG["stats"].get("cache_misses", 0)
    last = CONFIG["stats"].get("last_reset") or "Never"
    text = (
        "Admin Panel\n\n"
        f"• Force-Join: {'ON' if CONFIG.get('force_join', True) else 'OFF'}\n"
        f"• Maintenance: {'ON' if CONFIG.get('maintenance_mode', False) else 'OFF'}\n"
        f"• Download Lock: {'ON' if CONFIG.get('download_lock', True) else 'OFF'}\n"
        f"• Enforce for Admins: {'ON' if CONFIG.get('enforce_for_admins', False) else 'OFF'}\n"
        f"• Whitelist: {len(CONFIG.get('whitelist', []))} user(s)\n"
        f"• Channels: {', '.join([str(x) for x in CONFIG.get('required_channels', [])]) or 'None'}\n"
        f"• Invite: {CONFIG.get('invite_link') or 'None'}\n\n"
        f"Stats\n• Users: {len(users)}\n• Downloads: {dls}\n• Cache: {hits} hit(s), {misses} miss(es)\n• Last Reset: {last}"
    )
    await message.reply(text, reply_markup=admin_main_kb(), disable_web_page_preview=True)

@bot.on_message(filters.private & (filters.text | filters.media))
async def catch_admin_inputs(client, message: Message):
    """Captures ForceReply inputs for admin actions, then falls through to user flow."""
    uid = message.from_user.id
    intent = ADMIN_WAIT.get(uid)

    # Maintenance mode block for non-admins (whitelist allowed)
    if CONFIG.get("maintenance_mode", False) and not (is_admin(uid) or is_whitelisted(uid)):
        return await message.reply("🛠 Under Maintenance\n\nPlease try again later.")

    add_user_stat(uid)

    # ====== ADMIN INTENTS ======
    if intent and is_admin(uid):
        txt = (message.text or message.caption or "").strip()

        if intent == "add_channel":
            arr = CONFIG.get("required_channels", [])
            if txt.startswith("@") or txt.startswith("-100"):
                if txt not in arr:
                    arr.append(txt)
                    CONFIG["required_channels"] = arr
                    save_config(CONFIG)
                    await message.reply(f"✅ Added: {txt}")
                else:
                    await message.reply("ℹ️ Already present.")
            else:
                await message.reply("❌ Invalid. Use @username or -100ID.")
            ADMIN_WAIT.pop(uid, None)
            return

        if intent == "set_invite":
            if txt.startswith("http://") or txt.startswith("https://"):
                CONFIG["invite_link"] = txt
                save_config(CONFIG)
                await message.reply("✅ Invite link saved.")
            else:
                await message.reply("❌ Invalid link.")
            ADMIN_WAIT.pop(uid, None)
            return

        if intent == "broadcast":
            # Accept text or media; store draft; ask for segment
            draft = {
                "type": "text",
                "text": txt,
                "buttons": parse_buttons_from_text(txt),
                "media": None,        # {'kind': 'photo'|'video'|'doc', 'file_id': ..., 'caption': str}
            }
            if message.photo:
                draft["type"] = "photo"
                draft["media"] = {"kind": "photo", "file_id": message.photo[-1].file_id, "caption": txt}
            elif message.video:
                draft["type"] = "video"
                draft["media"] = {"kind": "video", "file_id": message.video.file_id, "caption": txt}
            elif message.document:
                draft["type"] = "doc"
                draft["media"] = {"kind": "doc", "file_id": message.document.file_id, "caption": txt}

            BCAST_DRAFT[uid] = draft
            ADMIN_WAIT[uid] = "broadcast_segment"
            return await message.reply("Select audience for this broadcast:", reply_markup=bcast_segment_kb())

        if intent == "add_admin":
            if txt.isdigit():
                uid2 = int(txt)
                arr = set(CONFIG.get("admins", []))
                arr.add(uid2)
                CONFIG["admins"] = list(arr)
                save_config(CONFIG)
                await message.reply(f"✅ Admin added: {uid2}")
            else:
                await message.reply("❌ Send numeric Telegram ID.")
            ADMIN_WAIT.pop(uid, None)
            return

        if intent == "wl_add":
            if txt.isdigit():
                u = int(txt)
                arr = set(CONFIG.get("whitelist", []))
                if u in arr:
                    await message.reply("ℹ️ Already whitelisted.")
                else:
                    arr.add(u)
                    CONFIG["whitelist"] = list(arr)
                    save_config(CONFIG)
                    await message.reply(f"✅ Whitelisted: {u}")
            else:
                await message.reply("❌ Send a numeric Telegram User ID.")
            ADMIN_WAIT.pop(uid, None)
            return

        if intent == "wl_rem":
            if txt.isdigit():
                u = int(txt)
                arr = [x for x in CONFIG.get("whitelist", []) if int(x) != u]
                CONFIG["whitelist"] = arr
                save_config(CONFIG)
                await message.reply(f"✅ Removed from whitelist: {u}")
            else:
                await message.reply("❌ Send a numeric Telegram User ID.")
            ADMIN_WAIT.pop(uid, None)
            return

    # ====== FALL THROUGH: Normal user flow ======
    raw = (message.text or message.caption or "").strip()
    if not raw:
        return
    url = clean_youtube_url(raw)
    if "youtube.com/watch?v=" not in url and "youtu.be" not in url:
        return  # ignore non-YouTube text/media

    link_uid = hash_url(url)[:10]
    url_store[link_uid] = url

    await message.reply(
        "What would you like to download?",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🎥 Download Video", callback_data=f"video|{link_uid}"),
                InlineKeyboardButton("🎧 Download Audio", callback_data=f"audio|{link_uid}")
            ]
        ])
    )

# ================== CALLBACKS ==================
@bot.on_callback_query()
async def handle_callback(client, cb):
    try:
        await cb.answer(cache_time=0)
    except Exception:
        pass

    data = cb.data or ""
    user_id = cb.from_user.id

    # Maintenance mode block for non-admins (whitelist allowed)
    if CONFIG.get("maintenance_mode", False) and not (is_admin(user_id) or is_whitelisted(user_id)):
        try:
            return await cb.message.reply("🛠 Under Maintenance\n\nPlease try again later.")
        except Exception:
            return

    # debounce ultra-fast taps
    parts = data.split("|", 2)
    key = parts[0] + (parts[1] if len(parts) > 1 else "")
    if is_fast_double_click(user_id, key):
        return

    # ---- FORCE JOIN recheck ----
    if data == "recheck_join":
        # reuse the regular gate for recheck UX
        if await is_user_member(client, cb.from_user.id):
            return await cb.message.edit_text("✅ Access Unlocked\nYou’re good to go!")
        else:
            return await cb.message.edit_text(
                "❌ Still not detected. Please join and tap Recheck again.",
                reply_markup=join_kb(), disable_web_page_preview=True
            )

    # ---- ADMIN PANEL ----
    if data.startswith("adm_"):
        if not is_admin(cb.from_user.id):
            return await cb.answer("Not allowed.", show_alert=True)

        key = data

        if key == "adm_toggle_force":
            CONFIG["force_join"] = not CONFIG.get("force_join", True)
            save_config(CONFIG)
            return await cb.message.edit_text("Admin Panel (updated)", reply_markup=admin_main_kb())

        if key == "adm_toggle_maint":
            CONFIG["maintenance_mode"] = not CONFIG.get("maintenance_mode", False)
            save_config(CONFIG)
            return await cb.message.edit_text("Admin Panel (updated)", reply_markup=admin_main_kb())

        if key == "adm_toggle_dllock":  # NEW
            CONFIG["download_lock"] = not CONFIG.get("download_lock", True)
            save_config(CONFIG)
            return await cb.message.edit_text("Admin Panel (updated)", reply_markup=admin_main_kb())

        if key == "adm_toggle_enfadm":  # NEW
            CONFIG["enforce_for_admins"] = not CONFIG.get("enforce_for_admins", False)
            save_config(CONFIG)
            return await cb.message.edit_text("Admin Panel (updated)", reply_markup=admin_main_kb())

        if key == "adm_channels":
            chs = CONFIG.get("required_channels", [])
            desc = ("Required Channels\n" + ("\n".join(f"- {c}" for c in chs) if chs else "None"))
            return await cb.message.edit_text(desc, reply_markup=channels_kb())

        if key == "adm_addchan":
            ADMIN_WAIT[cb.from_user.id] = "add_channel"
            return await cb.message.reply("Send channel @username or -100ID to add:",
                                          reply_markup=ForceReply(selective=True, placeholder="@YourChannel or -100..."))

        if key.startswith("adm_remchan|"):
            _, ch = key.split("|", 1)
            arr = CONFIG.get("required_channels", [])
            arr = [x for x in arr if str(x) != str(ch)]
            CONFIG["required_channels"] = arr
            save_config(CONFIG)
            await cb.answer("Removed.")
            chs = CONFIG.get("required_channels", [])
            desc = ("Required Channels\n" + ("\n".join(f"- {c}" for c in chs) if chs else "None"))
            return await cb.message.edit_text(desc, reply_markup=channels_kb())

        if key == "adm_invite":
            cur = CONFIG.get("invite_link") or "None"
            return await cb.message.edit_text(f"Invite Link: {cur}", reply_markup=invite_kb(), disable_web_page_preview=True)

        if key == "adm_setinv":
            ADMIN_WAIT[cb.from_user.id] = "set_invite"
            return await cb.message.reply("Send the invite link (e.g., https://t.me/+abcdef…):",
                                          reply_markup=ForceReply(selective=True, placeholder="https://t.me/+..."))

        if key == "adm_clrinv":
            CONFIG["invite_link"] = None
            save_config(CONFIG)
            await cb.answer("Cleared.")
            cur = CONFIG.get("invite_link") or "None"
            return await cb.message.edit_text(f"Invite Link: {cur}", reply_markup=invite_kb(), disable_web_page_preview=True)

        if key == "adm_bcast":
            ADMIN_WAIT[cb.from_user.id] = "broadcast"
            return await cb.message.reply(
                "Send the broadcast content:\n• Text OR Photo/Video/Document (with caption)\n\n"
                "Optional buttons: add a block in the text/caption like:\n\n"
                "[Buttons]\nTitle 1 | https://example.com\nTitle 2 | https://t.me/yourchannel",
                reply_markup=ForceReply(selective=True, placeholder="Your announcement…")
            )

        if key == "adm_stats":
            users = CONFIG["stats"].get("users", [])
            dls = CONFIG["stats"].get("downloads", 0)
            hits = CONFIG["stats"].get("cache_hits", 0)
            misses = CONFIG["stats"].get("cache_misses", 0)
            last = CONFIG["stats"].get("last_reset") or "Never"
            txt = (
                f"Stats\n• Users: {len(users)}\n• Downloads: {dls}\n"
                f"• Cache: {hits} hit(s), {misses} miss(es)\n• Last Reset: {last}"
            )
            return await cb.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🔄 Reset Counters", callback_data="adm_resetdls")],
                 [InlineKeyboardButton("⬅️ Back", callback_data="adm_back")]]
            ))

        if key == "adm_resetdls":
            CONFIG["stats"]["downloads"] = 0
            CONFIG["stats"]["cache_hits"] = 0
            CONFIG["stats"]["cache_misses"] = 0
            CONFIG["stats"]["last_reset"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
            save_config(CONFIG)
            return await cb.message.edit_text("✅ Counters reset.", reply_markup=admin_main_kb())

        if key == "adm_admins":
            return await cb.message.edit_text("Admins", reply_markup=admins_kb())

        if key == "adm_addadmin":
            ADMIN_WAIT[cb.from_user.id] = "add_admin"
            return await cb.message.reply("Send numeric Telegram ID to add as admin:",
                                          reply_markup=ForceReply(selective=True, placeholder="123456789"))

        if key.startswith("adm_remadmin|"):
            _, uid = key.split("|", 1)
            arr = [x for x in CONFIG.get("admins", []) if str(x) != str(uid)]
            CONFIG["admins"] = arr
            save_config(CONFIG)
            await cb.answer("Removed.")
            return await cb.message.edit_text("Admins (updated)", reply_markup=admins_kb())

        if key == "adm_wl":
            lst = CONFIG.get("whitelist", [])
            view = "None" if not lst else "\n".join(f"- {u}" for u in lst)
            return await cb.message.edit_text(f"Whitelist Users (IDs):\n{view}", reply_markup=whitelist_kb())

        if key == "adm_wl_add":
            ADMIN_WAIT[cb.from_user.id] = "wl_add"
            return await cb.message.reply("Send numeric Telegram **User ID** to add to whitelist:",
                                          reply_markup=ForceReply(selective=True, placeholder="123456789"))

        if key == "adm_wl_rem":
            ADMIN_WAIT[cb.from_user.id] = "wl_rem"
            return await cb.message.reply("Send numeric Telegram **User ID** to remove from whitelist:",
                                          reply_markup=ForceReply(selective=True, placeholder="123456789"))

        if key == "adm_back":
            return await cb.message.edit_text("Admin Panel", reply_markup=admin_main_kb())

    # ---- BROADCAST SEGMENT & SCHEDULE ----
    if data.startswith("bseg_") and is_admin(user_id):
        draft = BCAST_DRAFT.get(user_id)
        if not draft:
            return await cb.answer("No draft. Start again.", show_alert=True)
        if data == "bseg_cancel":
            BCAST_DRAFT.pop(user_id, None)
            ADMIN_WAIT.pop(user_id, None)
            return await cb.message.edit_text("❌ Broadcast cancelled.")

        users_all = [int(u) for u in CONFIG["stats"].get("users", [])]
        last_seen = CONFIG["stats"].get("last_seen", {})
        now = datetime.utcnow()

        if data == "bseg_all":
            target = users_all
            label = "All Users"
        elif data == "bseg_active7":
            target = []
            for u in users_all:
                iso = last_seen.get(str(u))
                if not iso:
                    continue
                try:
                    seen = datetime.fromisoformat(iso.replace("Z", ""))
                    if (now - seen) <= timedelta(days=7):
                        target.append(u)
                except Exception:
                    continue
            label = "Active (7d)"
        elif data == "bseg_wl":
            target = [int(x) for x in CONFIG.get("whitelist", [])]
            label = "Whitelisted"
        else:
            return

        BCAST_DRAFT[user_id]["target"] = target
        BCAST_DRAFT[user_id]["segment_label"] = label
        ADMIN_WAIT[user_id] = "broadcast_schedule"
        return await cb.message.edit_text(
            f"Segment selected: {label}\n\nChoose when to send:",
            reply_markup=bcast_schedule_kb()
        )

    if data.startswith("bsched_") and is_admin(user_id):
        draft = BCAST_DRAFT.get(user_id)
        if not draft:
            return await cb.answer("No draft. Start again.", show_alert=True)
        if data == "bsched_cancel":
            BCAST_DRAFT.pop(user_id, None)
            ADMIN_WAIT.pop(user_id, None)
            return await cb.message.edit_text("❌ Broadcast cancelled.")

        # Schedule delta
        delay = 0
        if data == "bsched_15":
            delay = 15 * 60
        elif data == "bsched_60":
            delay = 60 * 60
        # "bsched_now" => delay = 0

        ADMIN_WAIT.pop(user_id, None)
        when = "Now" if delay == 0 else f"in {delay//60} min"

        async def run_broadcast():
            if delay > 0:
                await asyncio.sleep(delay)
            targets = draft.get("target", [])
            ok, fail = 0, 0
            start_ts = time.time()
            markup = draft.get("buttons")
            try:
                for u in targets:
                    try:
                        if draft["type"] == "text":
                            txt_clean = draft["text"]
                            if txt_clean and "[Buttons]" in txt_clean:
                                txt_clean = txt_clean.split("[Buttons]")[0].strip()
                            await bot.send_message(u, txt_clean or "", reply_markup=markup, disable_web_page_preview=True)
                        else:
                            media = draft["media"]
                            cap = (media.get("caption") or "")
                            if "[Buttons]" in cap:
                                cap = cap.split("[Buttons]")[0].strip()
                            if media["kind"] == "photo":
                                await bot.send_photo(u, media["file_id"], caption=cap, reply_markup=markup)
                            elif media["kind"] == "video":
                                await bot.send_video(u, media["file_id"], caption=cap, reply_markup=markup)
                            elif media["kind"] == "doc":
                                await bot.send_document(u, media["file_id"], caption=cap, reply_markup=markup)
                            else:
                                await bot.send_message(u, draft.get("text") or "")
                        ok += 1
                    except Exception:
                        fail += 1
            finally:
                dur = time.time() - start_ts
                try:
                    await bot.send_message(
                        user_id,
                        f"📬 Broadcast Summary ({draft.get('segment_label','')}, {when})\n"
                        f"✅ Delivered: {ok}\n⚠️ Failed: {fail}\n⏱ Duration: {int(dur)}s"
                    )
                except Exception:
                    pass
                BCAST_DRAFT.pop(user_id, None)

        asyncio.create_task(run_broadcast())
        return await cb.message.edit_text(f"📢 Scheduled: {when}. You’ll get a summary after delivery.")

    # ---- VIDEO: fetch info ----
    if data.startswith("video|"):
        _, link_uid = data.split("|", 1)
        url = url_store.get(link_uid)
        if not url:
            return await cb.message.reply("❌ Session expired. Send the link again.")

        info_msg = await cb.message.reply("🔎 Fetching video info…")

        async def job():
            try:
                with yt_dlp.YoutubeDL({'quiet': True, 'no_warnings': True}) as ydl:
                    info = ydl.extract_info(url, download=False)
                title = info.get("title", "N/A")
                duration = info.get("duration", 0)
                channel = info.get("uploader", "Unknown")
                views = info.get("view_count", 0)
                upload_date = info.get("upload_date", "")
                thumbnail = info.get("thumbnail", "")

                upload_date = f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:]}" if upload_date else "N/A"
                mins, secs = divmod(int(duration or 0), 60)
                caption = (
                    f"📺 Title: {title}\n"
                    f"👤 Channel: {channel}\n"
                    f"⏱ Duration: {mins}:{secs:02d}\n"
                    f"👁 Views: {views:,}\n"
                    f"📅 Uploaded: {upload_date}\n\n"
                    f"Select a quality:"
                )
                buttons = InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton("144p", callback_data=f"res|144p|{link_uid}"),
                        InlineKeyboardButton("240p", callback_data=f"res|240p|{link_uid}"),
                        InlineKeyboardButton("360p", callback_data=f"res|360p|{link_uid}")
                    ],
                    [
                        InlineKeyboardButton("480p", callback_data=f"res|480p|{link_uid}"),
                        InlineKeyboardButton("720p", callback_data=f"res|720p|{link_uid}"),
                        InlineKeyboardButton("1080p", callback_data=f"res|1080p|{link_uid}")
                    ]
                ])

                if thumbnail:
                    tpath = f"downloads/thumb_{link_uid}.jpg"
                    download_thumbnail(thumbnail, tpath)
                    await cb.message.reply_photo(tpath, caption=caption, reply_markup=buttons)
                    try:
                        if os.path.exists(tpath):
                            os.remove(tpath)
                    except Exception:
                        pass
                else:
                    await cb.message.reply(caption, reply_markup=buttons)
            except Exception:
                await cb.message.reply("❌ Error fetching video info. Please try again later.")
            finally:
                try:
                    await info_msg.delete()
                except Exception:
                    pass

        await enqueue_task(user_id, job())

    # ---- VIDEO: user picked quality -> choose format ----
    elif data.startswith("res|"):
        try:
            _, quality, link_uid = data.split("|", 2)
        except ValueError:
            return
        await cb.message.reply(
            f"💽 Selected {quality}\nChoose file format:",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("MP4 (Recommended)", callback_data=f"download|{quality}|mp4|{link_uid}"),
                    InlineKeyboardButton("MKV (Alternative)", callback_data=f"download|{quality}|mkv|{link_uid}")
                ],
                [
                    InlineKeyboardButton("WEBM (VP9/Opus)", callback_data=f"download|{quality}|webm|{link_uid}")
                ]
            ])
        )

    # ---- VIDEO: download (with Smart Cache) + DOWNLOAD LOCK ----
    elif data.startswith("download|"):
        try:
            _, quality, fmt, link_uid = data.split("|", 3)
        except ValueError:
            return

        url = url_store.get(link_uid)
        if not url:
            return await cb.message.reply("❌ Session expired. Send the link again.")

        # 🔒 DOWNLOAD LOCK (only if enabled)
        if CONFIG.get("download_lock", True):
            if not await is_member_of_required_channels(client, user_id):
                req = ", ".join([str(x) for x in CONFIG.get("required_channels", [])]) or "None"
                return await cb.message.reply(
                    "🚫 Download Locked\n\n"
                    "Please join the required channel(s) to unlock this download.\n"
                    f"Required: {req}\n\n"
                    "👉 After joining, tap “I’ve joined, Recheck”.",
                    reply_markup=join_kb(),
                    disable_web_page_preview=True
                )

        height_map = {"144p": 144, "240p": 240, "360p": 360, "480p": 480, "720p": 720, "1080p": 1080}
        max_h = height_map.get(quality, 720)

        # Smart cache key for video
        vkey = hash_url(f"{url}|{quality}|{fmt}")
        cached_path = os.path.join(CACHE_DIR, f"{vkey}.{fmt}")

        # If cached, send instantly
        if os.path.exists(cached_path):
            inc_cache(True)
            cancel_markup = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Download", callback_data=f"cancel|{link_uid}")]])
            download_msg = await cb.message.reply("✅ Found in cache. Sending…\n" + progress_bar(1, 1), reply_markup=cancel_markup)

            last = 0.0
            async def on_progress(cur, total):
                nonlocal last
                now = time.time()
                if now - last > 0.25:
                    last = now
                    await safe_edit(download_msg, f"📥 Sending…\n{progress_bar(cur, total)}", reply_markup=cancel_markup)

            await cb.message.reply_video(
                cached_path,
                caption=f"✅ {quality} {fmt.upper()} ready (from cache)",
                supports_streaming=True,
                progress=on_progress
            )
            inc_downloads(1)
            try:
                await download_msg.delete()
            except Exception:
                pass
            return

        inc_cache(False)
        ydl_format = f"bestvideo[height<={max_h}]+bestaudio/best[height<={max_h}]"
        status = await cb.message.reply(f"🔄 Preparing {quality} {fmt.upper()}…")

        async def job():
            thumb_path = None
            file_path = None
            try:
                ydl_opts = {
                    "format": ydl_format,
                    "merge_output_format": fmt,  # mp4/mkv/webm
                    "outtmpl": "downloads/%(title)s.%(ext)s",
                    "quiet": True,
                    "no_warnings": True,
                    "http_headers": {"User-Agent": "Mozilla/5.0"},
                    "postprocessors": [{"key": "FFmpegVideoConvertor", "preferedformat": fmt}],
                }

                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(url, download=True)
                    file_path = ydl.prepare_filename(info)
                    # ensure extension
                    if not file_path.lower().endswith(f".{fmt}"):
                        base = file_path.rsplit(".", 1)[0]
                        alt = base + f".{fmt}"
                        if os.path.exists(alt):
                            file_path = alt

                    # size guard
                    try:
                        if os.path.getsize(file_path) > MAX_FILE_SIZE:
                            await status.edit("⚠️ File is larger than 2 GB. Please try a lower resolution.")
                            return
                    except Exception:
                        pass

                    # move to cache for future hits
                    try:
                        if not os.path.exists(cached_path):
                            os.replace(file_path, cached_path)
                            file_path = cached_path
                    except Exception:
                        pass

                    thumb_url = info.get("thumbnail")
                    if thumb_url:
                        tpath = f"downloads/thumb_{link_uid}.jpg"
                        if download_thumbnail(thumb_url, tpath):
                            thumb_path = tpath

                cancel_markup = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Download", callback_data=f"cancel|{link_uid}")]])
                download_msg = await cb.message.reply("📥 Downloading…\n" + progress_bar(0, 1), reply_markup=cancel_markup)
                task_id = download_msg.id
                active_tasks[task_id] = {"file": file_path, "thumb": thumb_path}

                last = 0.0
                async def on_progress(cur, total):
                    nonlocal last
                    now = time.time()
                    if now - last > 0.25:
                        last = now
                        await safe_edit(download_msg, f"📥 Downloading…\n{progress_bar(cur, total)}", reply_markup=cancel_markup)

                await cb.message.reply_video(
                    file_path,
                    caption=f"✅ {quality} {fmt.upper()} ready",
                    supports_streaming=True,
                    thumb=thumb_path if (thumb_path and os.path.exists(thumb_path)) else None,
                    progress=on_progress
                )

                inc_downloads(1)
                try:
                    await download_msg.delete()
                except Exception:
                    pass
                try:
                    await status.delete()
                except Exception:
                    pass

                active_tasks.pop(task_id, None)

            except Exception:
                try:
                    await status.edit("❌ Something went wrong. Please try again.")
                except Exception:
                    pass
            finally:
                if thumb_path and os.path.exists(thumb_path):
                    try:
                        os.remove(thumb_path)
                    except Exception:
                        pass

        await enqueue_task(user_id, job())

    # ---- AUDIO: menu ----
    elif data.startswith("audio|"):
        try:
            _, link_uid = data.split("|", 1)
        except ValueError:
            return
        await cb.message.reply(
            "🎧 Pick your audio format & bitrate:",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("🎵 MP3 — 128 kbps", callback_data=f"audio_dl|mp3|128|{link_uid}"),
                    InlineKeyboardButton("🎵 MP3 — 192 kbps", callback_data=f"audio_dl|mp3|192|{link_uid}")
                ],
                [
                    InlineKeyboardButton("🎵 MP3 — 320 kbps", callback_data=f"audio_dl|mp3|320|{link_uid}"),
                    InlineKeyboardButton("🎵 M4A — 128 kbps", callback_data=f"audio_dl|m4a|128|{link_uid}")
                ]
            ])
        )

    # ---- AUDIO: download (with Smart Cache) + DOWNLOAD LOCK ----
    elif data.startswith("audio_dl|"):
        try:
            _, fmt, bitrate, link_uid = data.split("|", 3)
        except ValueError:
            return
        url = url_store.get(link_uid)
        if not url:
            return await cb.message.reply("❌ Session expired. Send the link again.")

        # 🔒 DOWNLOAD LOCK (only if enabled)
        if CONFIG.get("download_lock", True):
            if not await is_member_of_required_channels(client, user_id):
                req = ", ".join([str(x) for x in CONFIG.get("required_channels", [])]) or "None"
                return await cb.message.reply(
                    "🚫 Download Locked\n\n"
                    "Please join the required channel(s) to unlock this download.\n"
                    f"Required: {req}\n\n"
                    "👉 After joining, tap “I’ve joined, Recheck”.",
                    reply_markup=join_kb(),
                    disable_web_page_preview=True
                )

        @retry()
        async def process_audio():
            status = await cb.message.reply("🔄 Preparing audio…")
            file_id = hash_url(url + fmt + bitrate)
            cached_path = os.path.join(CACHE_DIR, f"{file_id}.{fmt}")
            thumb_path = None

            try:
                if os.path.exists(cached_path):
                    inc_cache(True)
                    await safe_edit(status, "✅ Found in cache. Sending…")
                    file_path = cached_path
                else:
                    inc_cache(False)
                    ydl_opts = {
                        "format": "bestaudio/best",
                        "outtmpl": "downloads/%(title)s.%(ext)s",
                        "quiet": True,
                        "no_warnings": True,
                        "http_headers": {"User-Agent": "Mozilla/5.0"},
                        "postprocessors": [{
                            "key": "FFmpegExtractAudio",
                            "preferredcodec": fmt,
                            "preferredquality": bitrate
                        }]
                    }
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        info = ydl.extract_info(url, download=True)
                        base = ydl.prepare_filename(info).rsplit(".", 1)[0]
                        src = f"{base}.{fmt}"
                        if os.path.exists(src):
                            os.replace(src, cached_path)
                        file_path = cached_path
                        thumb_url = info.get("thumbnail")

                    tpath = f"downloads/thumb_{link_uid}.jpg"
                    if download_thumbnail(thumb_url, tpath):
                        thumb_path = tpath

                cancel_markup = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Download", callback_data=f"cancel|{link_uid}")]])
                download_msg = await cb.message.reply("📥 Downloading…\n" + progress_bar(0, 1), reply_markup=cancel_markup)
                task_id = download_msg.id
                active_tasks[task_id] = {"file": file_path, "thumb": thumb_path}

                last = 0.0
                async def on_progress(current, total):
                    nonlocal last
                    now = time.time()
                    if now - last > 0.25:
                        last = now
                        await safe_edit(download_msg, f"📥 Downloading…\n{progress_bar(current, total)}", reply_markup=cancel_markup)

                await cb.message.reply_audio(
                    file_path,
                    caption=f"✅ Here’s your {fmt.upper()} ({bitrate} kbps)",
                    thumb=thumb_path if (thumb_path and os.path.exists(thumb_path)) else None,
                    progress=on_progress
                )

                inc_downloads(1)
                try:
                    await download_msg.delete()
                except Exception:
                    pass
                try:
                    await status.delete()
                except Exception:
                    pass

                active_tasks.pop(task_id, None)

            except Exception:
                try:
                    await status.edit("❌ Something went wrong. Please try again.")
                except Exception:
                    pass
            finally:
                if thumb_path and os.path.exists(thumb_path):
                    try:
                        os.remove(thumb_path)
                    except Exception:
                        pass

        await enqueue_task(user_id, process_audio())

    # ---- CANCEL download ----
    elif data.startswith("cancel|"):
        msg_id = cb.message.id
        task = active_tasks.pop(msg_id, None)

        if user_id in user_tasks:
            try:
                user_tasks[user_id].cancel()
            except Exception:
                pass

        if task:
            try:
                f = task.get("file")
                if f and os.path.exists(f):
                    os.remove(f)
            except Exception:
                pass
            try:
                t = task.get("thumb")
                if t and os.path.exists(t):
                    os.remove(t)
            except Exception:
                pass

        await safe_edit(cb.message, "❌ Download cancelled.")

# ================== BOOT TASKS ==================
async def on_startup():
    cleanup_cache(int(CONFIG.get("cache_ttl_days", 7)))
    print("Bot ready. Cache cleanup done.")

# ================== RUN ==================
if __name__ == "__main__":
    print("Bot starting…")
    try:
        cleanup_cache(int(CONFIG.get("cache_ttl_days", 7)))
    except Exception:
        pass
    bot.run()
