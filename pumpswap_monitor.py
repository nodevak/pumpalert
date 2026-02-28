"""
PumpSwap Monitor — Dual Channel Telegram Notifier

Channel 1: real-time new tokens via PumpSwap WebSocket → saved to NeonDB
Channel 2: periodically checks DB tokens against DexScreener filter criteria
           /status command → lists tokens currently passing the filter

Setup:
  1. pip install requests websocket-client psycopg2-binary python-dotenv
  2. Create bot via @BotFather → get token
  3. Get chat_ids via https://api.telegram.org/bot<TOKEN>/getUpdates
  4. Create NeonDB project → copy connection string
  5. Fill in .env
  6. python3 pumpswap_monitor.py
"""

import json
import os
import re
import threading
import time
from datetime import datetime
from pathlib import Path

import requests
import websocket
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# ─── LOAD CONFIG FROM .env ────────────────────────────────────────────────────
load_dotenv(Path(__file__).parent / ".env")

BOT_TOKEN_NEW_PAIRS = os.getenv("BOT_TOKEN_NEW_PAIRS", "")
BOT_TOKEN_FILTER2   = os.getenv("BOT_TOKEN_FILTER2", "")
BOT_TOKEN_FILTER3   = os.getenv("BOT_TOKEN_FILTER3", "")
CHAT_ID             = os.getenv("CHAT_ID", "")
DATABASE_URL        = os.getenv("DATABASE_URL", "")

FILTER_POLL_SEC = int(os.getenv("FILTER_POLL_SEC", 90))

# ── Filter 2: MCap $200K-$1M ──────────────────────────────────────────────────
F2 = {
    "key":       "f2",
    "min_mcap":  float(os.getenv("F2_MIN_MCAP",   200_000)),
    "max_mcap":  float(os.getenv("F2_MAX_MCAP", 1_000_000)),
    "min_age_h": float(os.getenv("F2_MIN_AGE_H",       24)),
    "max_age_h": float(os.getenv("F2_MAX_AGE_H",      720)),
    "min_vol":   float(os.getenv("F2_MIN_VOL",    100_000)),
    "min_chg":   float(os.getenv("F2_MIN_CHG",        10)),
    "label":     "💎 Gem Alert — MCap $200K-$1M",
}

# ── Filter 3: MCap >$1M ───────────────────────────────────────────────────────
F3 = {
    "key":       "f3",
    "min_mcap":  float(os.getenv("F3_MIN_MCAP", 1_000_000)),
    "max_mcap":  float("inf"),
    "min_age_h": float(os.getenv("F3_MIN_AGE_H",      24)),
    "max_age_h": float(os.getenv("F3_MAX_AGE_H",     720)),
    "min_vol":   float(os.getenv("F3_MIN_VOL",   100_000)),
    "min_chg":   float(os.getenv("F3_MIN_CHG",        10)),
    "label":     "🚀 Moonshot Alert — MCap >$1M",
}

PUMPPORTAL_WS    = "wss://pumpportal.fun/api/data"
DEXSCREENER_API  = "https://api.dexscreener.com/latest/dex/tokens/{}"
TELEGRAM_API_NEW = f"https://api.telegram.org/bot{BOT_TOKEN_NEW_PAIRS}"
TELEGRAM_API_F2  = f"https://api.telegram.org/bot{BOT_TOKEN_FILTER2}"
TELEGRAM_API_F3  = f"https://api.telegram.org/bot{BOT_TOKEN_FILTER3}"

# Shared in-memory state
lock                 = threading.Lock()
seen_tokens_set: set = set()   # fast lookup, loaded from DB on startup
filter_state: dict   = {}
update_offsets: dict = {"f2": 0, "f3": 0, "new": 0}


# ─── DATABASE ─────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def init_db():
    """Create tables if they don't exist."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            # All graduated token addresses
            cur.execute("""
                CREATE TABLE IF NOT EXISTS seen_tokens (
                    address       TEXT PRIMARY KEY,
                    created_at_ms BIGINT DEFAULT 0,
                    added_at      TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            # Per-filter state: which tokens are currently passing / expired
            cur.execute("""
                CREATE TABLE IF NOT EXISTS filter_state (
                    filter_key TEXT NOT NULL,
                    address    TEXT NOT NULL,
                    state      TEXT NOT NULL,  -- 'currently' or 'expired'
                    PRIMARY KEY (filter_key, address)
                )
            """)
        conn.commit()
    print("  [DB] Tables ready.")


def db_load_seen_tokens() -> set:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT address FROM seen_tokens")
            return {row[0] for row in cur.fetchall()}


def db_add_seen_token(address: str, created_at_ms: int = 0):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO seen_tokens (address, created_at_ms)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                """, (address, created_at_ms))
            conn.commit()
    except Exception as e:
        print(f"  [DB] db_add_seen_token error: {e}")


def db_load_filter_state(filter_key: str, state_type: str) -> set:
    """Load 'currently' or 'expired' set for a filter from DB."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT address FROM filter_state
                WHERE filter_key = %s AND state = %s
            """, (filter_key, state_type))
            return {row[0] for row in cur.fetchall()}


def db_save_filter_state(filter_key: str, state_type: str, addresses: set):
    """Replace the filter state set in DB."""
    if not addresses:
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Delete old entries for this filter+state
                cur.execute("""
                    DELETE FROM filter_state
                    WHERE filter_key = %s AND state = %s
                """, (filter_key, state_type))
                # Insert new
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO filter_state (filter_key, address, state) VALUES %s ON CONFLICT DO NOTHING",
                    [(filter_key, addr, state_type) for addr in addresses]
                )
            conn.commit()
    except Exception as e:
        print(f"  [DB] db_save_filter_state error: {e}")


def db_cleanup_seen_tokens(max_age_hours: float):
    """Remove tokens older than max_age_hours from seen_tokens."""
    cutoff_ms = int(time.time() * 1000 - max_age_hours * 3600 * 1000)
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM seen_tokens
                    WHERE created_at_ms > 0 AND created_at_ms < %s
                """, (cutoff_ms,))
                removed = cur.rowcount
            conn.commit()
        if removed:
            print(f"  [DB] Cleaned up {removed} expired tokens from seen_tokens")
        return removed
    except Exception as e:
        print(f"  [DB] cleanup error: {e}")
        return 0


# ─── TELEGRAM ─────────────────────────────────────────────────────────────────

def send_telegram(api_url: str, message: str):
    try:
        r = requests.post(f"{api_url}/sendMessage", json={
            "chat_id": CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }, timeout=10)
        if not r.ok:
            print(f"  TG error {r.status_code}: {r.json().get('description')}")
            requests.post(f"{api_url}/sendMessage", json={
                "chat_id": CHAT_ID,
                "text": re.sub(r"<[^>]+>", "", message),
                "disable_web_page_preview": True,
            }, timeout=10)
    except Exception as e:
        print(f"  TG error: {e}")


def handle_seen(api_url: str):
    with lock:
        tokens = list(seen_tokens_set)
    total = len(tokens)
    if not total:
        send_telegram(api_url, "<b>No tokens tracked yet.</b>")
        return
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = ["<b>Tracked: " + str(total) + " tokens</b>  |  <i>" + now + "</i>\n"]
    for i, addr in enumerate(tokens, 1):
        lines.append(str(i) + ". <code>" + addr + "</code>")
    chunk = ""
    for line in lines:
        if len(chunk) + len(line) + 1 > 3800:
            send_telegram(api_url, chunk)
            chunk = line
            time.sleep(0.3)
        else:
            chunk = chunk + "\n" + line if chunk else line
    if chunk.strip():
        send_telegram(api_url, chunk)


def handle_missing(api_url: str):
    with lock:
        tokens = list(seen_tokens_set)
    if not tokens:
        send_telegram(api_url, "<b>No tokens tracked yet.</b>")
        return
    send_telegram(api_url, "Checking " + str(len(tokens)) + " tokens, please wait...")
    pairs = fetch_token_data(tokens)
    returned_addrs = {(p.get("baseToken") or {}).get("address") for p in pairs}
    missing = [addr for addr in tokens if addr not in returned_addrs]
    if not missing:
        send_telegram(api_url, "<b>All " + str(len(tokens)) + " tokens returned data.</b>")
        return
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = ["<b>No data for " + str(len(missing)) + "/" + str(len(tokens)) + " tokens</b>  |  <i>" + now + "</i>",
             "<i>(likely dead/rugged or not yet indexed)</i>"]
    for i, addr in enumerate(missing, 1):
        lines.append(str(i) + ". <code>" + addr + "</code>")
    chunk = ""
    for line in lines:
        if len(chunk) + len(line) + 1 > 3800:
            send_telegram(api_url, chunk)
            chunk = line
            time.sleep(0.3)
        else:
            chunk = chunk + "\n" + line if chunk else line
    if chunk.strip():
        send_telegram(api_url, chunk)


def check_commands():
    bots = [
        ("f2",  TELEGRAM_API_F2,  F2),
        ("f3",  TELEGRAM_API_F3,  F3),
        ("new", TELEGRAM_API_NEW, None),
    ]
    for key, api_url, flt in bots:
        try:
            r = requests.get(f"{api_url}/getUpdates",
                             params={"offset": update_offsets.get(key, 0) + 1, "timeout": 0},
                             timeout=10)
            if not r.ok:
                continue
            for update in r.json().get("result", []):
                update_offsets[key] = update["update_id"]
                text = update.get("message", {}).get("text", "").strip().lower()
                if text.startswith("/status") and flt:
                    handle_status(api_url, flt)
                elif text.startswith("/missing"):
                    handle_missing(api_url)
                elif text.startswith("/seen"):
                    handle_seen(api_url)
        except Exception as e:
            print(f"  Command poll error ({key}): {e}")


def handle_status(api_url: str, flt: dict):
    with lock:
        tokens_to_check = list(seen_tokens_set)

    if not tokens_to_check:
        send_telegram(api_url, "📭 <b>No tokens tracked yet.</b>")
        return

    send_telegram(api_url, f"⏳ Checking {len(tokens_to_check)} tokens, please wait...")
    pairs = fetch_token_data(tokens_to_check)
    if not pairs:
        send_telegram(api_url, "⚠️ Could not fetch data from DexScreener.")
        return

    passing = [p for p in pairs if passes_filter(p, flt)]
    if not passing:
        send_telegram(api_url,
            f"📭 <b>No tokens currently passing the filter.</b>\n"
            f"Checked {len(pairs)} tokens.")
        return

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = []
    for pair in passing:
        token_addr = pair.get("baseToken", {}).get("address", "?")
        symbol = esc(pair.get("baseToken", {}).get("symbol", "?"))
        mcap   = pair.get("marketCap", 0)
        chg24  = pair.get("priceChange", {}).get("h24", 0)
        vol24  = pair.get("volume", {}).get("h24", 0)
        ds_url = pair.get("url", f"https://dexscreener.com/solana/{token_addr}")
        lines.append(
            f"🪙 <b>{symbol}</b>  ({time_ago(pair.get('pairCreatedAt', 0))})\n"
            f"   MCap: {fmt_usd(mcap)}  |  24h: {fmt_pct(chg24)}  |  Vol: {fmt_usd(vol24)}\n"
            f"   <a href='{ds_url}'>DexScreener</a>\n"
            f"   <code>{token_addr}</code>"
        )

    header = f"📊 <b>{flt['label']} — Status ({len(lines)})</b>\n{now}\n━━━━━━━━━━━━━━━━━━━━\n\n"
    chunk = header
    for line in lines:
        if len(chunk) + len(line) > 3800:
            send_telegram(api_url, chunk)
            chunk = line + "\n\n"
        else:
            chunk += line + "\n\n"
    if chunk.strip():
        send_telegram(api_url, chunk)


# ─── HELPERS ──────────────────────────────────────────────────────────────────

def esc(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def fmt_usd(v): return f"${v:,.0f}" if v else "N/A"
def fmt_pct(v): return f"+{v:.1f}%" if v and v > 0 else (f"{v:.1f}%" if v else "N/A")

def time_ago(created_ms: int) -> str:
    if not created_ms: return "unknown"
    secs = int(time.time() - created_ms / 1000)
    if secs < 60:     return f"{secs}s ago"
    elif secs < 3600: return f"{secs//60}m ago"
    elif secs < 86400:
        h = secs // 3600; m = (secs % 3600) // 60
        return f"{h}h {m}m ago" if m else f"{h}h ago"
    else:
        d = secs // 86400; h = (secs % 86400) // 3600
        return f"{d}d {h}h ago" if h else f"{d}d ago"

def age_hours(pair: dict) -> float:
    created_ms = pair.get("pairCreatedAt", 0)
    if not created_ms: return 0
    return (time.time() * 1000 - created_ms) / 3_600_000

def fetch_token_data(token_addresses: list) -> list:
    all_pairs = []
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    for i in range(0, len(token_addresses), 30):
        batch = token_addresses[i:i+30]
        url = DEXSCREENER_API.format(",".join(batch))
        try:
            r = requests.get(url, headers=headers, timeout=15)
            if r.ok:
                pairs = r.json().get("pairs") or []
                filtered = [p for p in pairs
                            if p.get("chainId") == "solana"
                            and "pump" in p.get("dexId", "").lower()]
                if not filtered and pairs:
                    filtered = [p for p in pairs if p.get("chainId") == "solana"]
                all_pairs.extend(filtered)
        except Exception as e:
            print(f"  [API] exception: {e}")
        time.sleep(0.3)
    return all_pairs

def passes_filter(pair: dict, flt: dict) -> bool:
    mcap  = pair.get("marketCap", 0) or 0
    vol   = pair.get("volume", {}).get("h24", 0) or 0
    chg   = pair.get("priceChange", {}).get("h24", 0) or 0
    age_h = age_hours(pair)
    return (
        flt["min_mcap"]  <= mcap  <= flt["max_mcap"] and
        flt["min_age_h"] <= age_h <= flt["max_age_h"] and
        vol >= flt["min_vol"] and
        chg >= flt["min_chg"]
    )

def build_alert_ch1(token: dict) -> str:
    symbol      = esc(token.get("symbol", "?"))
    name        = esc(token.get("name", "?"))
    mint        = token.get("mint", "?")
    now         = datetime.now().strftime("%H:%M:%S")
    created_ms  = token.get("created_ms", 0)
    created_ago = time_ago(created_ms) if created_ms else "unknown"
    created_str = datetime.fromtimestamp(created_ms / 1000).strftime("%Y-%m-%d %H:%M:%S") if created_ms else "?"
    return (
        f"🎓 <b>Token Graduated to PumpSwap!</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🪙 <b>{symbol}</b>  |  {name}\n"
        f"🕐 Graduated at: <b>{now}</b>\n"
        f"📅 Token created: <b>{created_ago}</b>  (<i>{created_str}</i>)\n\n"
        f"📋 <b>Contract:</b>\n"
        f"<code>{mint}</code>\n\n"
        f"🔗 <a href='https://dexscreener.com/solana/{mint}'>DexScreener</a>  |  "
        f"<a href='https://solscan.io/token/{mint}'>Solscan</a>  |  "
        f"<a href='https://pump.fun/coin/{mint}'>Pump.fun</a>"
    )

def build_alert_ch2(pair: dict, label: str = "💎 Gem Alert") -> str:
    base        = pair.get("baseToken", {})
    price_usd   = pair.get("priceUsd", "?")
    liquidity   = pair.get("liquidity", {}).get("usd", 0)
    mcap        = pair.get("marketCap", 0)
    volume      = pair.get("volume", {}).get("h24", 0)
    txns_5m     = pair.get("txns", {}).get("m5", {})
    txns_1h     = pair.get("txns", {}).get("h1", {})
    change      = pair.get("priceChange", {})
    created_ms  = pair.get("pairCreatedAt", 0)
    pair_addr   = pair.get("pairAddress", "")
    ds_url      = pair.get("url", f"https://dexscreener.com/solana/{pair_addr}")
    symbol      = esc(base.get("symbol", "?"))
    name        = esc(base.get("name", "?"))
    token_addr  = base.get("address", "?")
    created_str = datetime.fromtimestamp(created_ms / 1000).strftime("%Y-%m-%d %H:%M:%S") if created_ms else "?"
    return (
        f"💎 <b>{label} — Entered Filter!</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🪙 <b>{symbol}</b>  |  {name}\n"
        f"🕐 Created: <b>{created_str}</b>  ({time_ago(created_ms)})\n\n"
        f"💰 Price:      <code>${price_usd}</code>\n"
        f"💧 Liquidity:  <code>{fmt_usd(liquidity)}</code>\n"
        f"📊 Market Cap: <code>{fmt_usd(mcap)}</code>\n"
        f"📈 Volume 24h: <code>{fmt_usd(volume)}</code>\n\n"
        f"🔄 Txns  5m: {txns_5m.get('buys',0)}B / {txns_5m.get('sells',0)}S\n"
        f"🔄 Txns  1h: {txns_1h.get('buys',0)}B / {txns_1h.get('sells',0)}S\n\n"
        f"📉 Price Change:\n"
        f"   5m: {fmt_pct(change.get('m5'))}  |  1h: {fmt_pct(change.get('h1'))}\n"
        f"   6h: {fmt_pct(change.get('h6'))}  |  24h: {fmt_pct(change.get('h24'))}\n\n"
        f"📋 <b>Contract:</b>\n"
        f"<code>{token_addr}</code>\n\n"
        f"🔗 <a href='{ds_url}'>DexScreener</a>  |  "
        f"<a href='https://solscan.io/token/{token_addr}'>Solscan</a>  |  "
        f"<a href='https://pump.fun/coin/{token_addr}'>Pump.fun</a>"
    )


# ─── CHANNEL 1 — WEBSOCKET ───────────────────────────────────────────────────

def fetch_token_meta(mint: str, retries: int = 6, delay: float = 5.0) -> dict:
    for attempt in range(retries):
        try:
            r = requests.get(
                f"https://api.dexscreener.com/latest/dex/tokens/{mint}",
                headers={"User-Agent": "Mozilla/5.0"}, timeout=10
            )
            if r.ok:
                pairs = r.json().get("pairs") or []
                for pair in sorted(pairs, key=lambda p: p.get("dexId", "") != "pumpswap"):
                    base = pair.get("baseToken", {})
                    if base.get("symbol") and base.get("name"):
                        return {
                            "symbol":     base.get("symbol", "?"),
                            "name":       base.get("name", "?"),
                            "created_ms": pair.get("pairCreatedAt", 0),
                        }
        except Exception:
            pass
        if attempt < retries - 1:
            print(f"  [Meta] not indexed yet, retrying in {delay}s ({attempt+1}/{retries})")
            time.sleep(delay)
    print(f"  [Meta] Could not fetch meta for {mint[:12]}... after {retries} attempts")
    return {"symbol": "?", "name": "?", "created_ms": 0}


def on_ws_open(ws):
    print("  [WS] Connected to PumpPortal")
    ws.send(json.dumps({"method": "subscribeMigration"}))

def on_ws_message(ws, message):
    try:
        data = json.loads(message)
        mint = data.get("mint") or data.get("token") or data.get("address")
        if not mint: return

        with lock:
            already_seen = mint in seen_tokens_set
        if already_seen: return

        # Fetch meta (with retry)
        meta = fetch_token_meta(mint)
        data["symbol"]     = meta["symbol"]
        data["name"]       = meta["name"]
        data["created_ms"] = meta["created_ms"]
        data["mint"]       = mint

        # Add to DB and in-memory set
        db_add_seen_token(mint, meta["created_ms"])
        with lock:
            seen_tokens_set.add(mint)

        print(f"  [GRADUATED] {data['symbol']:12} | {mint}")
        send_telegram(TELEGRAM_API_NEW, build_alert_ch1(data))

    except Exception as e:
        print(f"  [WS] Message error: {e}")

def on_ws_error(ws, error):
    print(f"  [WS] Error: {error}")

def on_ws_close(ws, close_status_code, close_msg):
    print(f"  [WS] Closed. Reconnecting in 5s...")
    time.sleep(5)
    start_websocket()

def start_websocket():
    ws = websocket.WebSocketApp(
        PUMPPORTAL_WS,
        on_open=on_ws_open,
        on_message=on_ws_message,
        on_error=on_ws_error,
        on_close=on_ws_close,
    )
    threading.Thread(target=ws.run_forever, daemon=True).start()


# ─── INITIAL SCAN ─────────────────────────────────────────────────────────────

def initial_scan(key: str, flt: dict):
    """Pre-populate state["currently"] on startup to avoid re-alerting."""
    state = filter_state[key]
    with lock:
        all_tokens = list(seen_tokens_set)
    if not all_tokens:
        return
    print(f"  [{key.upper()}] Initial scan of {len(all_tokens)} tokens...")
    pairs = fetch_token_data(all_tokens)
    for pair in pairs:
        addr = (pair.get("baseToken") or {}).get("address")
        if addr and passes_filter(pair, flt):
            state["currently"].add(addr)
    # Persist to DB
    db_save_filter_state(key, "currently", state["currently"])
    print(f"  [{key.upper()}] {len(state['currently'])} tokens currently passing (won't re-alert these)")


# ─── FILTER LOOP ─────────────────────────────────────────────────────────────

def filter_loop_for(key: str, api_url: str, flt: dict, initial_delay: float = 0):
    state = filter_state[key]
    if initial_delay:
        print(f"  [{key.upper()}] Waiting {initial_delay}s before first scan (stagger)...")
        time.sleep(initial_delay)
    print(f"  [{key.upper()}] Filter loop started (every {FILTER_POLL_SEC}s) — {flt['label']}")

    while True:
        time.sleep(FILTER_POLL_SEC)
        try:
            with lock:
                all_tokens = list(seen_tokens_set)

            tokens_to_check = [t for t in all_tokens if t not in state["expired"]]
            pairs = fetch_token_data(tokens_to_check) if tokens_to_check else []

            new_passing = set()
            for pair in pairs:
                token_addr = pair.get("baseToken", {}).get("address", "?")
                age_h = age_hours(pair)
                if age_h > flt["max_age_h"]:
                    state["expired"].add(token_addr)
                    continue
                if passes_filter(pair, flt):
                    new_passing.add(token_addr)

            just_entered = new_passing - state["currently"]
            if just_entered:
                print(f"  [{key.upper()}] {len(new_passing)} passing, {len(just_entered)} just entered")

            for token_addr in just_entered:
                pair = next((p for p in pairs
                             if p.get("baseToken", {}).get("address") == token_addr), None)
                if not pair: continue
                symbol = pair.get("baseToken", {}).get("symbol", "?")
                print(f"  [{key.upper()}] ALERT: {symbol:12} | {token_addr}")
                send_telegram(api_url, build_alert_ch2(pair, label=flt["label"]))

            state["currently"] = new_passing

            # Persist state to DB every cycle so restarts don't re-alert
            db_save_filter_state(key, "currently", state["currently"])
            db_save_filter_state(key, "expired",   state["expired"])

            # Cleanup seen_tokens older than max_age + 48h buffer
            db_cleanup_seen_tokens(flt["max_age_h"] + 48)
            # Sync in-memory set after cleanup
            with lock:
                seen_tokens_set.clear()
                seen_tokens_set.update(db_load_seen_tokens())

        except Exception as e:
            print(f"  [{key.upper()}] Error: {e}")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    global seen_tokens_set, filter_state

    print("="*60)
    print("  🚀 PumpSwap Monitor — Triple Channel (NeonDB)")
    print("="*60)

    for var, name in [
        (BOT_TOKEN_NEW_PAIRS, "BOT_TOKEN_NEW_PAIRS"),
        (BOT_TOKEN_FILTER2,   "BOT_TOKEN_FILTER2"),
        (BOT_TOKEN_FILTER3,   "BOT_TOKEN_FILTER3"),
        (CHAT_ID,             "CHAT_ID"),
        (DATABASE_URL,        "DATABASE_URL"),
    ]:
        if not var:
            print(f"\n❌  {name} missing in .env\n"); return

    # Init DB tables
    init_db()

    # Load seen tokens from DB into memory
    seen_tokens_set = db_load_seen_tokens()
    print(f"\n📂 Loaded {len(seen_tokens_set)} tokens from DB")

    # Load persisted filter state from DB
    filter_state["f2"] = {
        "currently": db_load_filter_state("f2", "currently"),
        "expired":   db_load_filter_state("f2", "expired"),
    }
    filter_state["f3"] = {
        "currently": db_load_filter_state("f3", "currently"),
        "expired":   db_load_filter_state("f3", "expired"),
    }
    print(f"   F2: {len(filter_state['f2']['currently'])} currently passing, {len(filter_state['f2']['expired'])} expired")
    print(f"   F3: {len(filter_state['f3']['currently'])} currently passing, {len(filter_state['f3']['expired'])} expired")

    # If no persisted state, do initial scan to avoid re-alerts on first run
    if not filter_state["f2"]["currently"] and not filter_state["f2"]["expired"]:
        initial_scan("f2", F2)
    if not filter_state["f3"]["currently"] and not filter_state["f3"]["expired"]:
        initial_scan("f3", F3)

    # Start WebSocket (Bot 1)
    print("\n🔌 Connecting to PumpSwap WebSocket...")
    start_websocket()

    # Start filter threads — staggered by half interval
    threading.Thread(target=filter_loop_for, args=("f2", TELEGRAM_API_F2, F2), kwargs={"initial_delay": 0}, daemon=True).start()
    threading.Thread(target=filter_loop_for, args=("f3", TELEGRAM_API_F3, F3), kwargs={"initial_delay": FILTER_POLL_SEC / 2}, daemon=True).start()

    # Startup pings
    send_telegram(TELEGRAM_API_NEW,
        "✅ <b>PumpSwap Monitor Started</b>\n"
        "Listening for tokens graduating from pump.fun → PumpSwap")

    def flt_msg(flt):
        max_mcap = f"${flt['max_mcap']/1e6:.0f}M" if flt['max_mcap'] != float('inf') else "unlimited"
        return (
            f"✅ <b>{flt['label']} Started</b>\n"
            f"MCap: ${flt['min_mcap']/1e3:.0f}K–{max_mcap} | "
            f"Age: {flt['min_age_h']:.0f}-{flt['max_age_h']:.0f}h | "
            f"Vol: &gt;${flt['min_vol']/1e3:.0f}K | "
            f"24h: &gt;+{flt['min_chg']:.0f}%\n"
            f"Type /status to see passing tokens."
        )

    send_telegram(TELEGRAM_API_F2, flt_msg(F2))
    send_telegram(TELEGRAM_API_F3, flt_msg(F3))

    print("\n✅ Running. Press Ctrl+C to stop.\n")
    while True:
        try:
            check_commands()
            time.sleep(3)
        except KeyboardInterrupt:
            print("\n👋 Stopped.")
            break


if __name__ == "__main__":
    main()