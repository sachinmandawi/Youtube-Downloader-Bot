# -*- coding: utf-8 -*-
# YouTube Downloader Bot (Video: MP4/MKV/WEBM + Audio: MP3/M4A)
# Requires: pyrogram tgcrypto yt-dlp requests
# NOTE: Only download content you have rights for.

# --- Windows + Python 3.14: ensure an event loop exists BEFORE importing pyrogram
import sys, asyncio
if sys.platform.startswith("win"):
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except Exception:
        pass
try:
    asyncio.get_running_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

import os
import re
import time
import glob
import hashlib
import requests
from datetime import datetime, timezone
from functools import wraps
from typing import Dict, Tuple, Optional
from collections import deque

import yt_dlp
from pyrogram import Client, filters, enums
from pyrogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, ForceReply, Message
)
from pyrogram.errors import (
    UserNotParticipant, ChatAdminRequired, ChatWriteForbidden, ChatIdInvalid,
    ChannelPrivate, MessageNotModified
)

# ================== BOT CONFIG (YOUR DATA) ==================
API_ID = 23292615
API_HASH = "fc15ff59f3a1d77e4d86ff6f3ded9d44"
BOT_TOKEN = "8398662962:AAHD2YAC2BEovMdqg6XM-E8CYsez46U_mjY"

bot = Client("yt_quality_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024  # 2GB
os.makedirs("downloads", exist_ok=True)
CACHE_DIR = "downloads/cache"
os.makedirs(CACHE_DIR, exist_ok=True)

# ================== CONFIG (direct in Python; no JSON file) ==================
CONFIG = {
    "force_join": False,
    "required_channels": [],
    "invite_link": None,

    "admins": [8070535163],
    "admin_bypass": True,
    "enforce_for_admins": False,

    "maintenance_mode": False,
    "whitelist": [],

    "stats": {
        "users": [],
        "downloads": 0,
        "cache_hits": 0,
        "cache_misses": 0,
        "last_reset": None,
        "last_seen": {}
    },

    "cache_ttl_days": 7,
    "download_lock": False,

    # exposed in /admin → Parallel Limit
    "global_parallel_limit": 5,

    # ===== Credits + Referral + Gift Codes =====
    "user_credits": {},       # {user_id(str): int}
    "ref_credit_reward": 1,   # referral: credits to referrer
    "self_credit_reward": 1,  # referral: credits to new user
    "fastpass_cost": 1,       # cost per Fast Pass
    "gift_codes": {},         # "CODE": {"credits": int, "max_uses": int, "used_by": [user_id(str)]}
}

def save_config(_cfg):
    # No persistence (add JSON save if you want persistence across restarts)
    pass

# ================== IN-MEMORY STATE ==================
url_store: Dict[str, str] = {}                       # link_uid -> url
active_tasks: Dict[int, Dict[str, Optional[str]]] = {}  # msg_id -> {file, thumb}
user_queues: Dict[int, asyncio.Queue] = {}           # user_id -> queue
user_tasks: Dict[int, asyncio.Task] = {}             # user_id -> current task
user_locks: Dict[int, asyncio.Lock] = {}             # user_id -> lock
last_click_at: Dict[Tuple[int, str], float] = {}     # (user_id, key) -> ts
ADMIN_WAIT: Dict[int, str] = {}                      # pending action (admin + some user waits)
BCAST_DRAFT: Dict[int, dict] = {}                    # admin id -> broadcast draft
REDEEM_WAIT = set()                                  # user_ids waiting to send redeem code

# --- Global N-slot download gate + live queue with priority lanes ---
GLOBAL_DL_SEM = asyncio.Semaphore(int(CONFIG.get("global_parallel_limit", 5)))
FAST_QUEUE = deque()     # (job_id, user_id, link_uid, kind)
NORMAL_QUEUE = deque()
NEXT_JOB_ID = 0

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
    CONFIG["stats"]["last_seen"][str(user_id)] = datetime.now(timezone.utc).isoformat(timespec="seconds")
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
    # normalize Shorts
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

# ---- Credits helpers ----
def get_credits(uid: int) -> int:
    """
    Return credits for a user.
    Admins have effectively 'unlimited' credits represented by a very large integer.
    """
    try:
        if is_admin(uid):
            return 10**9
        return int(CONFIG["user_credits"].get(str(uid), 0))
    except Exception:
        return 0

def add_credits(uid: int, amount: int):
    """
    Add credits to a user. For admins this is a no-op.
    """
    try:
        if is_admin(uid):
            return
        CONFIG["user_credits"][str(uid)] = get_credits(uid) + int(amount)
        save_config(CONFIG)
    except Exception:
        pass

def spend_credit(uid: int, n: int = None) -> bool:
    """
    Spend credits from a user. Admins always succeed.
    Returns True if spent (or admin), False if insufficient.
    """
    try:
        if is_admin(uid):
            return True
        cost = int(CONFIG.get("fastpass_cost", 1)) if n is None else int(n)
        cur = get_credits(uid)
        if cur >= cost:
            CONFIG["user_credits"][str(uid)] = cur - cost
            save_config(CONFIG)
            return True
        return False
    except Exception:
        return False

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

async def safe_edit(msg: Message, text: Optional[str] = None, reply_markup: Optional[InlineKeyboardMarkup] = None, **kwargs):
    try:
        current_text = getattr(msg, "text", None) or getattr(msg, "caption", None)
        if text is None:
            return await msg.edit_reply_markup(reply_markup=reply_markup)
        if current_text == text:
            if reply_markup is not None:
                try:
                    return await msg.edit_reply_markup(reply_markup=reply_markup)
                except MessageNotModified:
                    return msg
            return msg
        return await msg.edit_text(text, reply_markup=reply_markup, **kwargs)
    except MessageNotModified:
        return msg
    except Exception:
        return msg

def is_fast_double_click(user_id: int, key: str, min_gap=0.35) -> bool:
    now = time.monotonic()
    prev = last_click_at.get((user_id, key), 0.0)
    last_click_at[(user_id, key)] = now
    return (now - prev) < min_gap

def parse_buttons_from_text(text: str) -> Optional[InlineKeyboardMarkup]:
    if not text or "[Buttons]" not in text:
        return None
    _, btns = text.split("[Buttons]", 1)
    rows = []
    for line in btns.strip().splitlines():
        if "|" in line:
            title, url = [x.strip() for x in line.split("|", 1)]
            if title and url:
                rows.append([InlineKeyboardButton(title, url=url)])
    return InlineKeyboardMarkup(rows) if rows else None

def cleanup_cache(ttl_days: int = None):
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

# ========== GLOBAL QUEUE WITH LIVE POSITION (PRIORITY) ==========
async def enter_global_queue_live(user_id: int, link_uid: str, kind: str, make_msg, priority: bool = False):
    """Place a job in the global queue (fast or normal). Show live position until a slot is acquired."""
    global NEXT_JOB_ID, FAST_QUEUE, NORMAL_QUEUE, GLOBAL_DL_SEM
    NEXT_JOB_ID += 1
    job_id = NEXT_JOB_ID
    lane = FAST_QUEUE if priority else NORMAL_QUEUE
    lane.append((job_id, user_id, link_uid, kind))

    def pos_of(jid: int) -> str:
        for idx, (j, *_rest) in enumerate(FAST_QUEUE, start=1):
            if j == jid:
                return f"FAST #{idx}"
        base = len(FAST_QUEUE)
        for idx, (j, *_rest) in enumerate(NORMAL_QUEUE, start=1):
            if j == jid:
                return f"#{base + idx}"
        return "—"

    queued_msg = await make_msg(f"⏳ Added to queue… Position: {pos_of(job_id)}")

    try:
        while True:
            try:
                await asyncio.wait_for(GLOBAL_DL_SEM.acquire(), timeout=0.05)
                target_queue = FAST_QUEUE if FAST_QUEUE else NORMAL_QUEUE
                if target_queue and target_queue[0][0] == job_id:
                    target_queue.popleft()
                    await safe_edit(queued_msg, "✅ Your turn! Starting…")
                    return job_id, queued_msg
                else:
                    GLOBAL_DL_SEM.release()
            except asyncio.TimeoutError:
                pass

            await asyncio.sleep(1.0)
            try:
                await safe_edit(queued_msg, f"⏳ Queue… Position: {pos_of(job_id)}")
            except Exception:
                pass
    except asyncio.CancelledError:
        try:
            FAST_QUEUE = deque([x for x in FAST_QUEUE if x[0] != job_id])
            NORMAL_QUEUE = deque([x for x in NORMAL_QUEUE if x[0] != job_id])
        except Exception:
            pass
        raise

# ================== FORCE-JOIN (JOIN GATE) ==================
def join_kb():
    chans = CONFIG.get("required_channels", [])
    buttons = []
    for ch in chans:
        if isinstance(ch, str) and ch.startswith("@"):
            url = "https://t.me/" + ch.lstrip("@")
        elif str(ch).startswith("-100"):
            url = CONFIG.get("invite_link") or "https://t.me/"
        else:
            url = CONFIG.get("invite_link") or "https://t.me/"
        buttons.append([InlineKeyboardButton(f"📢 Join {ch}", url=url)])
    buttons.append([InlineKeyboardButton("✅ I’ve joined, Recheck", callback_data="recheck_join")])
    return InlineKeyboardMarkup(buttons)

VALID_STATUSES = {
    enums.ChatMemberStatus.MEMBER,
    enums.ChatMemberStatus.ADMINISTRATOR,
    enums.ChatMemberStatus.OWNER,
}

async def _joined_in_chat(client: Client, chat: str, user_id: int) -> bool:
    try:
        m = await client.get_chat_member(chat, user_id)
        if (m.status in VALID_STATUSES) or (m.status == enums.ChatMemberStatus.RESTRICTED and getattr(m, "is_member", False)):
            return True
        return False
    except UserNotParticipant:
        return False
    except (ChannelPrivate, ChatAdminRequired, ChatWriteForbidden, ChatIdInvalid):
        return False
    except Exception:
        return False

async def is_user_member(client: Client, user_id: int) -> bool:
    if not CONFIG.get("force_join", True):
        return True
    chans = CONFIG.get("required_channels", [])
    if not chans:
        return True
    for chan in chans:
        if not await _joined_in_chat(client, chan, user_id):
            return False
    return True

async def is_member_of_required_channels(client: Client, user_id: int) -> bool:
    if is_admin(user_id) and CONFIG.get("admin_bypass", True) and not CONFIG.get("enforce_for_admins", False):
        return True
    chans = CONFIG.get("required_channels", [])
    if not chans:
        return True
    for chan in chans:
        if not await _joined_in_chat(client, chan, user_id):
            return False
    return True

# ================== /start ==================
def _log(msg):
    try:
        print("[START]", msg)
    except Exception:
        pass

def welcome_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💳 My Credits", callback_data="credits_menu")],
        [InlineKeyboardButton("🔗 Get Referral Link", callback_data="ref_menu")]
    ])

@bot.on_message(filters.command("start", prefixes=["/"]) & filters.private)
async def start_cmd(client, message):
    _log(f"/start from {message.from_user.id} ({message.from_user.first_name})")

    uid = message.from_user.id
    is_adm = is_admin(uid)
    is_wl = is_whitelisted(uid)

    if CONFIG.get("maintenance_mode", False) and not (is_adm or is_wl):
        return await message.reply("🛠 Under Maintenance\n\nWe’re upgrading the bot. Please try again later.")

    # Parse referral BEFORE add_user_stat to detect first-time
    parts = (message.text or "").split(maxsplit=1)
    param = parts[1] if len(parts) > 1 else None
    was_new = uid not in set(CONFIG["stats"].get("users", []))

    # Referral deep-link
    if param and param.startswith("ref_"):
        try:
            ref_id = int(param.split("_", 1)[1])
            if ref_id != uid and was_new:
                add_credits(uid, CONFIG.get("self_credit_reward", 1))
                add_credits(ref_id, CONFIG.get("ref_credit_reward", 1))
                try:
                    await message.reply(
                        f"🎉 Referral applied!\n"
                        f"• You got +{CONFIG.get('self_credit_reward',1)} credit.\n"
                        f"• Referrer {ref_id} got +{CONFIG.get('ref_credit_reward',1)} credit."
                    )
                except Exception:
                    pass
        except Exception:
            pass

    add_user_stat(uid)

    try:
        chans = CONFIG.get("required_channels", [])
        gate = CONFIG.get("force_join", True)
        enforce_admins = CONFIG.get("enforce_for_admins", False)
        admin_bypass = CONFIG.get("admin_bypass", True) and not enforce_admins

        if gate and (not (is_adm and admin_bypass)) and chans:
            ok = True
            for chan in chans:
                if not await _joined_in_chat(client, chan, uid):
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

    return await message.reply(
        "✨ Welcome to YouTube Downloader\n"
        "🎥 Paste a YouTube link & get your video or audio instantly.\n\n"
        "👇 Send your link below!",
        reply_markup=welcome_kb()
    )

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
                              callback_data="adm_toggle_dllock")],
        [InlineKeyboardButton("👮 Enforce for Admins: " + ("ON ✅" if CONFIG.get("enforce_for_admins", False) else "OFF ❌"),
                              callback_data="adm_toggle_enfadm")],
        [InlineKeyboardButton("⏫ Parallel Limit", callback_data="adm_conc")],
        [InlineKeyboardButton("🧪 Whitelist", callback_data="adm_wl")],
        [InlineKeyboardButton("📢 Required Channels", callback_data="adm_channels"),
         InlineKeyboardButton("🔗 Invite Link", callback_data="adm_invite")],
        [InlineKeyboardButton("📬 Broadcast", callback_data="adm_bcast"),
         InlineKeyboardButton("📊 Stats", callback_data="adm_stats")],
        [InlineKeyboardButton("🔧 Admins", callback_data="adm_admins")],
        [InlineKeyboardButton("🎁 Gift Codes", callback_data="adm_gift")]
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

def concurrency_kb():
    cur = int(CONFIG.get("global_parallel_limit", 5))
    choices = [2, 3, 5, 8, 10]
    rows = []
    for n in choices:
        label = f"{'• ' if n == cur else ''}{n}"
        rows.append([InlineKeyboardButton(label, callback_data=f"adm_setconc|{n}")])
    rows.append([InlineKeyboardButton("✏️ Custom…", callback_data="adm_setconc_custom")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="adm_back")])
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
        f"• Parallel Limit: {int(CONFIG.get('global_parallel_limit', 5))}\n"
        f"• Whitelist: {len(CONFIG.get('whitelist', []))} user(s)\n"
        f"• Channels: {', '.join([str(x) for x in CONFIG.get('required_channels', [])]) or 'None'}\n"
        f"• Invite: {CONFIG.get('invite_link') or 'None'}\n\n"
        f"Stats\n• Users: {len(users)}\n• Downloads: {dls}\n"
        f"• Cache: {hits} hit(s), {misses} miss(es)\n• Last Reset: {last}"
    )
    await message.reply(text, reply_markup=admin_main_kb(), disable_web_page_preview=True)

@bot.on_message(filters.private & (filters.text | filters.media))
async def catch_admin_inputs(client, message: Message):
    uid = message.from_user.id
    intent = ADMIN_WAIT.get(uid)

    if CONFIG.get("maintenance_mode", False) and not (is_admin(uid) or is_whitelisted(uid)):
        return await message.reply("🛠 Under Maintenance\n\nPlease try again later.")

    add_user_stat(uid)

    # ====== USER REDEEM WAIT ======
    if uid in REDEEM_WAIT:
        code_text = (message.text or message.caption or "").strip().upper()
        REDEEM_WAIT.discard(uid)
        if not code_text:
            return await message.reply("❌ Please send a valid code (e.g., WELCOME2025).")
        gift = CONFIG.get("gift_codes", {}).get(code_text)
        if not gift:
            return await message.reply("❌ Invalid or expired code.")
        if str(uid) in gift.get("used_by", []):
            return await message.reply("⚠️ You already used this code.")
        max_uses = int(gift.get("max_uses", 0))
        if max_uses and len(gift.get("used_by", [])) >= max_uses:
            return await message.reply("❌ This code has reached its maximum uses.")
        credits = int(gift.get("credits", 0))
        add_credits(uid, credits)
        gift.setdefault("used_by", []).append(str(uid))
        save_config(CONFIG)
        remaining = (gift["max_uses"] - len(gift["used_by"])) if max_uses else "∞"
        return await message.reply(
            f"🎉 Code Redeemed!\nYou received +{credits} credits 💳\nCode: {code_text}\nRemaining Uses: {remaining}"
        )

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
            draft = {
                "type": "text",
                "text": txt,
                "buttons": parse_buttons_from_text(txt),
                "media": None,
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

        if intent == "set_conc":
            txt_clean = (message.text or "").strip()
            try:
                val = int(txt_clean)
                if val <= 0:
                    raise ValueError
                CONFIG["global_parallel_limit"] = val
                save_config(CONFIG)
                global GLOBAL_DL_SEM
                GLOBAL_DL_SEM = asyncio.Semaphore(val)
                await message.reply(f"✅ Parallel limit updated to: {val}")
            except Exception:
                await message.reply("❌ Invalid value. Please send a positive integer (e.g., 5).")
            ADMIN_WAIT.pop(uid, None)
            return

        if intent == "gift_code":
            try:
                code, credits, max_uses = [x.strip() for x in txt.split("|")]
                code = code.upper()
                CONFIG["gift_codes"][code] = {"credits": int(credits), "max_uses": int(max_uses), "used_by": []}
                save_config(CONFIG)
                await message.reply(
                    f"✅ Gift Code Generated:\n"
                    f"Code: {code}\nCredits: {credits}\nMax Uses: {max_uses}"
                )
            except Exception:
                await message.reply("❌ Invalid format. Example: WELCOME2025|5|10")
            ADMIN_WAIT.pop(uid, None)
            return

        if intent == "gift_revoke":
            code = txt.strip().upper()
            codes = CONFIG.get("gift_codes", {})
            if code in codes:
                codes.pop(code, None)
                save_config(CONFIG)
                await message.reply(f"✅ Gift code revoked: {code}")
            else:
                await message.reply("❌ Code not found.")
            ADMIN_WAIT.pop(uid, None)
            return

    # ====== FALL THROUGH: Normal user flow (link handling) ======
    raw = (message.text or message.caption or "").strip()
    if not raw:
        return
    url = clean_youtube_url(raw)
    if "youtube.com/watch?v=" not in url and "youtu.be" not in url:
        return

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

    if CONFIG.get("maintenance_mode", False) and not (is_admin(user_id) or is_whitelisted(user_id)):
        try:
            return await cb.message.reply("🛠 Under Maintenance\n\nPlease try again later.")
        except Exception:
            return

    parts = data.split("|", 3)
    key = parts[0] + (parts[1] if len(parts) > 1 else "")
    if is_fast_double_click(user_id, key):
        return

    # ---- FORCE JOIN recheck ----
    if data == "recheck_join":
        if await is_user_member(client, cb.from_user.id):
            return await safe_edit(cb.message, "✅ Access Unlocked\nYou’re good to go!")
        else:
            return await safe_edit(
                cb.message,
                "❌ Still not detected. Please join and tap Recheck again.",
                reply_markup=join_kb(),
                disable_web_page_preview=True
            )

    # ---- PUBLIC MENUS ----
    if data == "credits_menu":
        bal = get_credits(user_id)
        bal_str = "∞" if is_admin(user_id) else str(bal)
        return await safe_edit(cb.message,
            f"💳 Your Credits: {bal_str}\n"
            "⚡ Each Fast Pass costs 1 credit.\n"
            "🕒 Use Normal Queue (free) if you don’t want to spend credits.\n",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔗 Get Referral Link", callback_data="ref_menu")],
                [InlineKeyboardButton("🎟️ Redeem Code", callback_data="redeem_open")],
                [InlineKeyboardButton("⬅️ Back to Menu", callback_data="back_menu")]
            ])
        )

    if data == "ref_menu":
        me = await client.get_me()
        link = f"https://t.me/{me.username}?start=ref_{user_id}" if me.username else "Bot username not set."
        return await safe_edit(cb.message,
            "🎁 Earn Free Credits!\n\n"
            f"🔗 Your Personal Invite Link:\n{link}\n\n"
            "👥 When a friend joins using your link:\n"
            f"• You earn +{CONFIG.get('ref_credit_reward',1)} credit 🎉\n"
            f"• Your friend gets +{CONFIG.get('self_credit_reward',1)} credit 🆓\n\n"
            "⚡ More credits = Faster downloads with Fast Pass!",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💳 My Credits", callback_data="credits_menu")],
                [InlineKeyboardButton("🎟️ Redeem Code", callback_data="redeem_open")],
                [InlineKeyboardButton("⬅️ Back to Menu", callback_data="back_menu")]
            ])
        )

    if data == "redeem_open":
        REDEEM_WAIT.add(user_id)
        return await cb.message.reply(
            "🎟️ Enter your gift code to redeem:\n💡 Example: WELCOME2025",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Menu", callback_data="back_menu")]])
        )

    if data == "back_menu":
        return await safe_edit(cb.message,
            "✨ Welcome to YouTube Downloader\n"
            "🎥 Paste a YouTube link & get your video or audio instantly.\n\n"
            "👇 Send your link below!",
            reply_markup=welcome_kb()
        )

    # ---- ADMIN PANEL ----
    if data.startswith("adm_"):
        if not is_admin(cb.from_user.id):
            return await cb.answer("Not allowed.", show_alert=True)

        key = data

        if key == "adm_toggle_force":
            CONFIG["force_join"] = not CONFIG.get("force_join", True)
            save_config(CONFIG)
            return await safe_edit(cb.message, "Admin Panel (updated)", reply_markup=admin_main_kb())

        if key == "adm_toggle_maint":
            CONFIG["maintenance_mode"] = not CONFIG.get("maintenance_mode", False)
            save_config(CONFIG)
            return await safe_edit(cb.message, "Admin Panel (updated)", reply_markup=admin_main_kb())

        if key == "adm_toggle_dllock":
            CONFIG["download_lock"] = not CONFIG.get("download_lock", True)
            save_config(CONFIG)
            return await safe_edit(cb.message, "Admin Panel (updated)", reply_markup=admin_main_kb())

        if key == "adm_toggle_enfadm":
            CONFIG["enforce_for_admins"] = not CONFIG.get("enforce_for_admins", False)
            save_config(CONFIG)
            return await safe_edit(cb.message, "Admin Panel (updated)", reply_markup=admin_main_kb())

        if key == "adm_conc":
            return await safe_edit(cb.message, "Set max parallel downloads:", reply_markup=concurrency_kb())

        if key == "adm_setconc_custom":
            ADMIN_WAIT[cb.from_user.id] = "set_conc"
            return await cb.message.reply(
                "Send the maximum number of parallel downloads (integer > 0):",
                reply_markup=ForceReply(selective=True, placeholder="e.g., 6")
            )

        if key.startswith("adm_setconc|"):
            try:
                _, n = key.split("|", 1)
                n = int(n)
                if n <= 0:
                    raise ValueError
                CONFIG["global_parallel_limit"] = n
                save_config(CONFIG)
                global GLOBAL_DL_SEM
                GLOBAL_DL_SEM = asyncio.Semaphore(n)
                await cb.answer(f"Updated: {n} parallel downloads")
                return await safe_edit(cb.message, "Admin Panel (updated)", reply_markup=admin_main_kb())
            except Exception:
                return await cb.answer("Invalid value", show_alert=True)

        if key == "adm_channels":
            chs = CONFIG.get("required_channels", [])
            desc = ("Required Channels\n" + ("\n".join(f"- {c}" for c in chs) if chs else "None"))
            return await safe_edit(cb.message, desc, reply_markup=channels_kb())

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
            return await safe_edit(cb.message, desc, reply_markup=channels_kb())

        if key == "adm_invite":
            cur = CONFIG.get("invite_link") or "None"
            return await safe_edit(cb.message, f"Invite Link: {cur}", reply_markup=invite_kb(), disable_web_page_preview=True)

        if key == "adm_setinv":
            ADMIN_WAIT[cb.from_user.id] = "set_invite"
            return await cb.message.reply("Send the invite link (e.g., https://t.me/+abcdef…):",
                                          reply_markup=ForceReply(selective=True, placeholder="https://t.me/+..."))

        if key == "adm_clrinv":
            CONFIG["invite_link"] = None
            save_config(CONFIG)
            await cb.answer("Cleared.")
            cur = CONFIG.get("invite_link") or "None"
            return await safe_edit(cb.message, f"Invite Link: {cur}", reply_markup=invite_kb(), disable_web_page_preview=True)

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
                "Stats\n"
                f"• Users: {len(users)}\n"
                f"• Downloads: {dls}\n"
                f"• Parallel Limit: {int(CONFIG.get('global_parallel_limit', 5))}\n"
                f"• Cache: {hits} hit(s), {misses} miss(es)\n"
                f"• Last Reset: {last}"
            )
            return await safe_edit(cb.message, txt, reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🔄 Reset Counters", callback_data="adm_resetdls")],
                 [InlineKeyboardButton("⬅️ Back", callback_data="adm_back")]]
            ))

        if key == "adm_resetdls":
            CONFIG["stats"]["downloads"] = 0
            CONFIG["stats"]["cache_hits"] = 0
            CONFIG["stats"]["cache_misses"] = 0
            CONFIG["stats"]["last_reset"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
            save_config(CONFIG)
            return await safe_edit(cb.message, "✅ Counters reset.", reply_markup=admin_main_kb())

        if key == "adm_admins":
            return await safe_edit(cb.message, "Admins", reply_markup=admins_kb())

        if key == "adm_addadmin":
            ADMIN_WAIT[cb.from_user.id] = "add_admin"
            return await cb.message.reply("Send numeric Telegram ID to add as admin:",
                                          reply_markup=ForceReply(selective=True, placeholder="123456789"))

        if key.startswith("adm_remadmin|"):
            _, uid_str = key.split("|", 1)
            arr = [x for x in CONFIG.get("admins", []) if str(x) != str(uid_str)]
            CONFIG["admins"] = arr
            save_config(CONFIG)
            await cb.answer("Removed.")
            return await safe_edit(cb.message, "Admins (updated)", reply_markup=admins_kb())

        if key == "adm_wl":
            lst = CONFIG.get("whitelist", [])
            view = "None" if not lst else "\n".join(f"- {u}" for u in lst)
            return await safe_edit(cb.message, f"Whitelist Users (IDs):\n{view}", reply_markup=whitelist_kb())

        if key == "adm_wl_add":
            ADMIN_WAIT[cb.from_user.id] = "wl_add"
            return await cb.message.reply("Send numeric Telegram **User ID** to add to whitelist:",
                                          reply_markup=ForceReply(selective=True, placeholder="123456789"))

        if key == "adm_wl_rem":
            ADMIN_WAIT[cb.from_user.id] = "wl_rem"
            return await cb.message.reply("Send numeric Telegram **User ID** to remove from whitelist:",
                                          reply_markup=ForceReply(selective=True, placeholder="123456789"))

        if key == "adm_back":
            return await safe_edit(cb.message, "Admin Panel", reply_markup=admin_main_kb())

        # === Gift codes admin ===
        if key == "adm_gift":
            codes = CONFIG.get("gift_codes", {})
            if not codes:
                desc = "No active gift codes."
            else:
                desc = "Active Gift Codes:\n"
                for code, data in codes.items():
                    used = len(data.get("used_by", []))
                    maxu = data.get("max_uses", '∞')
                    desc += f"- {code} → {data.get('credits',0)} credits ({maxu} max, {used} used)\n"
            return await safe_edit(cb.message, desc, reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Generate New", callback_data="adm_gift_new")],
                [InlineKeyboardButton("❌ Revoke Code", callback_data="adm_gift_revoke")],
                [InlineKeyboardButton("⬅️ Back", callback_data="adm_back")]
            ]))

        if key == "adm_gift_new":
            ADMIN_WAIT[user_id] = "gift_code"
            return await cb.message.reply(
                "Send gift code and credits in format:\nCODE|CREDITS|MAX_USES\nExample:\nWELCOME2025|5|10",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⬅️ Back to Admin Menu", callback_data="adm_back")],
                    [InlineKeyboardButton("❌ Cancel", callback_data="adm_cancel")]
                ])
            )

        if key == "adm_gift_revoke":
            ADMIN_WAIT[user_id] = "gift_revoke"
            return await cb.message.reply(
                "Send the code you want to revoke.\nExample: WELCOME2025",
                reply_markup=ForceReply(selective=True, placeholder="WELCOME2025")
            )

        if key == "adm_cancel":
            ADMIN_WAIT.pop(user_id, None)
            return await safe_edit(cb.message, "❌ Cancelled.", reply_markup=admin_main_kb())

    # ---- VIDEO: fetch info ----
    if data.startswith("video|"):
        _, link_uid = data.split("|", 1)
        url = url_store.get(link_uid)
        if not url:
            return await cb.message.reply("❌ Session expired. Send the link again.")

        info_msg = await cb.message.reply("🔎 Fetching video info…")

        async def job():
            try:
                def _get_info():
                    with yt_dlp.YoutubeDL({'quiet': True, 'no_warnings': True}) as ydl:
                        return ydl.extract_info(url, download=False)

                info = await asyncio.to_thread(_get_info)
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

    # ---- VIDEO: choose format after quality ----
    elif data.startswith("res|"):
        try:
            _, quality, link_uid = data.split("|", 2)
        except ValueError:
            return
        await cb.message.reply(
            f"💽 Selected {quality}\nChoose file format:",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("MP4 (Recommended)", callback_data=f"fmt|{quality}|mp4|{link_uid}"),
                    InlineKeyboardButton("MKV (Alternative)", callback_data=f"fmt|{quality}|mkv|{link_uid}")
                ],
                [
                    InlineKeyboardButton("WEBM (VP9/Opus)", callback_data=f"fmt|{quality}|webm|{link_uid}")
                ]
            ])
        )

    # ---- VIDEO: after format, let user choose Fast/Normal ----
    elif data.startswith("fmt|"):
        _, quality, fmt, link_uid = data.split("|", 3)
        bal = get_credits(user_id)
        return await cb.message.reply(
            f"🎯 {quality.upper()} · {fmt.upper()}\nChoose queue mode:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(f"⚡ Fast Pass (use {CONFIG.get('fastpass_cost',1)} credit)", callback_data=f"vfast|{quality}|{fmt}|{link_uid}")],
                [InlineKeyboardButton("🕒 Normal Queue (free)", callback_data=f"vnorm|{quality}|{fmt}|{link_uid}")]
            ])
        )

    # ---- VIDEO: download (fast) ----
    elif data.startswith("vfast|") or data.startswith("vnorm|"):
        fast = data.startswith("vfast|")
        _, quality, fmt, link_uid = data.split("|", 3)
        url = url_store.get(link_uid)
        if not url:
            return await cb.message.reply("❌ Session expired. Send the link again.")

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

        cache_key = hash_url(f"{url}|{quality}|{fmt}")
        cached_path = os.path.join(CACHE_DIR, f"{cache_key}.{fmt}")

        # Spend credit if Fast
        priority = False
        if fast:
            if spend_credit(user_id, CONFIG.get("fastpass_cost", 1)):
                await cb.message.reply(f"⚡ Fast Pass used (−{CONFIG.get('fastpass_cost',1)} credit). Jumping the queue…")
                priority = True
            else:
                # Not enough credits
                return await cb.message.reply(
                    "❌ You don’t have enough credits.\nUse Normal Queue (free) or invite friends with /ref to earn credits.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🕒 Normal Queue (free)", callback_data=f"vnorm|{quality}|{fmt}|{link_uid}")],
                        [InlineKeyboardButton("🔗 Get Referral Link", callback_data="ref_menu")]
                    ])
                )

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

        # queue + acquire a slot
        job_id, queued_msg = await enter_global_queue_live(
            user_id, link_uid, kind="video",
            make_msg=lambda text: cb.message.reply(text),
            priority=priority
        )

        async def job():
            thumb_path = None
            try:
                ydl_opts = {
                    "format": ydl_format,
                    "merge_output_format": fmt,
                    "outtmpl": "downloads/%(title)s.%(ext)s",
                    "quiet": True,
                    "no_warnings": True,
                    "http_headers": {"User-Agent": "Mozilla/5.0"},
                    "postprocessors": [{"key": "FFmpegVideoConvertor", "preferedformat": fmt}],
                }

                def _download_video():
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        info = ydl.extract_info(url, download=True)
                        fp = ydl.prepare_filename(info)
                        if not fp.lower().endswith(f".{fmt}"):
                            base = fp.rsplit(".", 1)[0]
                            alt = base + f".{fmt}"
                            if os.path.exists(alt):
                                fp = alt
                        try:
                            if os.path.getsize(fp) > MAX_FILE_SIZE:
                                return {"oversize": True}, fp, None
                        except Exception:
                            pass
                        try:
                            if not os.path.exists(cached_path):
                                os.replace(fp, cached_path)
                                fp = cached_path
                        except Exception:
                            pass
                        return info, fp, info.get("thumbnail")

                info, file_path, thumb_url = await asyncio.to_thread(_download_video)

                # release slot after heavy work
                try:
                    GLOBAL_DL_SEM.release()
                except Exception:
                    pass

                if isinstance(info, dict) and info.get("oversize"):
                    await safe_edit(status, "⚠️ File is larger than 2 GB. Please try a lower resolution.")
                    return

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
                    await safe_edit(status, "❌ Something went wrong. Please try again.")
                except Exception:
                    pass
            finally:
                if thumb_path and os.path.exists(thumb_path):
                    try:
                        os.remove(thumb_path)
                    except Exception:
                        pass
                try:
                    if GLOBAL_DL_SEM.locked():
                        GLOBAL_DL_SEM.release()
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

    # ---- AUDIO: download (cache + queue + lock) ----
    elif data.startswith("audio_dl|"):
        try:
            _, fmt, bitrate, link_uid = data.split("|", 3)
        except ValueError:
            return
        url = url_store.get(link_uid)
        if not url:
            return await cb.message.reply("❌ Session expired. Send the link again.")

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

            # Ask for Fast/Normal
            bal = get_credits(user_id)
            ask = await cb.message.reply(
                f"🎯 {fmt.upper()} · {bitrate} kbps\nChoose queue mode:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"⚡ Fast Pass (use {CONFIG.get('fastpass_cost',1)} credit)", callback_data=f"afast|{fmt}|{bitrate}|{link_uid}")],
                    [InlineKeyboardButton("🕒 Normal Queue (free)", callback_data=f"anorm|{fmt}|{bitrate}|{link_uid}")]
                ])
            )
            return  # stop here; next callback continues

        await enqueue_task(user_id, process_audio())

    elif data.startswith("afast|") or data.startswith("anorm|"):
        fast = data.startswith("afast|")
        _, fmt, bitrate, link_uid = data.split("|", 3)
        url = url_store.get(link_uid)
        if not url:
            return await cb.message.reply("❌ Session expired. Send the link again.")

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

        priority = False
        if fast:
            if spend_credit(user_id, CONFIG.get("fastpass_cost", 1)):
                await cb.message.reply(f"⚡ Fast Pass used (−{CONFIG.get('fastpass_cost',1)} credit). Jumping the queue…")
                priority = True
            else:
                return await cb.message.reply(
                    "❌ You don’t have enough credits.\nUse Normal Queue (free) or invite friends with /ref to earn credits.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🕒 Normal Queue (free)", callback_data=f"anorm|{fmt}|{bitrate}|{link_uid}")],
                        [InlineKeyboardButton("🔗 Get Referral Link", callback_data="ref_menu")]
                    ])
                )

        status = await cb.message.reply("🔄 Preparing audio…")

        job_id, queued_msg = await enter_global_queue_live(
            user_id, link_uid, kind="audio",
            make_msg=lambda text: cb.message.reply(text),
            priority=priority
        )

        cache_id = hash_url(url + fmt + bitrate)
        cached_path = os.path.join(CACHE_DIR, f"{cache_id}.{fmt}")
        thumb_path = None

        try:
            if os.path.exists(cached_path):
                inc_cache(True)
                await safe_edit(status, "✅ Found in cache. Sending…")
                file_path = cached_path
                thumb_url = None
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
                def _download_audio():
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        info = ydl.extract_info(url, download=True)
                        base = ydl.prepare_filename(info).rsplit(".", 1)[0]
                        src = f"{base}.{fmt}"
                        if os.path.exists(src):
                            os.replace(src, cached_path)
                        return info

                info = await asyncio.to_thread(_download_audio)
                file_path = cached_path
                thumb_url = info.get("thumbnail")

            # release slot after heavy work
            try:
                GLOBAL_DL_SEM.release()
            except Exception:
                pass

            if thumb_url:
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
                await safe_edit(status, "❌ Something went wrong. Please try again.")
            except Exception:
                pass
        finally:
            if thumb_path and os.path.exists(thumb_path):
                try:
                    os.remove(thumb_path)
                except Exception:
                    pass
            try:
                if GLOBAL_DL_SEM.locked():
                    GLOBAL_DL_SEM.release()
            except Exception:
                pass

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
