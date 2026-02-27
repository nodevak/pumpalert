"""
DexScreener Memecoin Alert Bot #1

Filters:
  - DEX: PumpSwap (Solana)
  - Market Cap: $200K - $1M
  - Age: 24h - 720h
  - 24H Volume: >= $100K
  - 24H Change: >= +10%
  - Has profile

Commands: /status | /count | /help
"""

import os
import sys
import time
import logging
import threading
import requests
from dotenv import load_dotenv
from db import (
    init_db, load_seen_tokens, save_seen_tokens,
    load_known_tokens, save_known_tokens, delete_expired_known_tokens
)

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")
POLL_INTERVAL      = int(os.getenv("POLL_INTERVAL", "180"))

BOT_ID = "bot1"  # unique ID for this bot in the DB

MIN_MARKET_CAP = 200_000
MAX_MARKET_CAP = 1_000_000
MIN_24H_VOL    = 100_000
MIN_24H_CHG    = 10.0
MIN_AGE_HOURS  = 24
MAX_AGE_HOURS  = 720
CLEANUP_BUFFER_HOURS = 48

SEARCH_QUERIES = [
    "pumpswap", "pump fun", "pump sol", "pump.fun", "pump swap",
]

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(
            stream=open(sys.stdout.fileno(), mode="w", encoding="utf-8", buffering=1)
        ),
    ],
)
log = logging.getLogger(__name__)

HEADERS      = {"User-Agent": "Mozilla/5.0 (compatible; DexBot/1.0)"}
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

state_lock       = threading.Lock()
current_matching : list[dict] = []
last_cycle_time  : str        = "never"

# ── DexScreener ───────────────────────────────────────────────────────────────
def is_pumpswap_solana(p):
    return (
        p.get("chainId") == "solana"
        and "pump" in (p.get("dexId") or "").lower()
    )

def search_pairs(query: str) -> list:
    try:
        r = requests.get(
            "https://api.dexscreener.com/latest/dex/search",
            params={"q": query}, headers=HEADERS, timeout=15,
        )
        if r.status_code == 400: return []
        r.raise_for_status()
        return [p for p in (r.json().get("pairs") or []) if is_pumpswap_solana(p)]
    except Exception as e:
        log.warning(f"Search error ({query!r}): {e}"); return []

def batch_fetch_by_address(addresses: list) -> list:
    results = []
    for i in range(0, len(addresses), 30):
        batch = addresses[i:i+30]
        url = f"https://api.dexscreener.com/latest/dex/tokens/{','.join(batch)}"
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            r.raise_for_status()
            results.extend(p for p in (r.json().get("pairs") or []) if is_pumpswap_solana(p))
        except Exception as e:
            log.warning(f"Batch fetch error: {e}")
        time.sleep(0.3)
    return results

def fetch_profile_addresses():
    try:
        r = requests.get("https://api.dexscreener.com/token-profiles/latest/v1",
                         headers=HEADERS, timeout=15)
        r.raise_for_status()
        return [i["tokenAddress"] for i in r.json()
                if i.get("chainId") == "solana" and i.get("tokenAddress")]
    except Exception as e:
        log.warning(f"Profiles error: {e}"); return []

def fetch_boost_addresses():
    try:
        r = requests.get("https://api.dexscreener.com/token-boosts/latest/v1",
                         headers=HEADERS, timeout=15)
        r.raise_for_status()
        return [i["tokenAddress"] for i in r.json()
                if i.get("chainId") == "solana" and i.get("tokenAddress")]
    except Exception as e:
        log.warning(f"Boosts error: {e}"); return []

def deduplicate(pairs: list) -> list:
    seen, out = set(), []
    for p in pairs:
        key = p.get("pairAddress") or (p.get("baseToken") or {}).get("address")
        if key and key not in seen:
            seen.add(key); out.append(p)
    return out

# ── Discovery ─────────────────────────────────────────────────────────────────
def discover_all_pairs(known: dict):
    all_pairs = []

    addrs = fetch_profile_addresses()
    log.info(f"[Profiles]  {len(addrs)} addresses")
    if addrs: all_pairs.extend(batch_fetch_by_address(addrs))

    addrs = fetch_boost_addresses()
    log.info(f"[Boosts]    {len(addrs)} addresses")
    if addrs: all_pairs.extend(batch_fetch_by_address(addrs))

    log.info(f"[Search]    Running {len(SEARCH_QUERIES)} queries...")
    search_total = 0
    for q in SEARCH_QUERIES:
        found = search_pairs(q)
        all_pairs.extend(found); search_total += len(found)
        time.sleep(0.2)
    log.info(f"[Search]    {search_total} pairs")

    all_pairs = deduplicate(all_pairs)

    # Find new addresses and update known dict
    new_entries = {}
    for p in all_pairs:
        addr = (p.get("baseToken") or {}).get("address")
        created_at = p.get("pairCreatedAt") or 0
        if addr and addr not in known:
            known[addr] = created_at
            new_entries[addr] = created_at
        elif addr and known.get(addr) == 0 and created_at:
            known[addr] = created_at
            new_entries[addr] = created_at

    if new_entries:
        log.info(f"[Discover]  +{len(new_entries)} new  |  Total known: {len(known)}")

    # Cleanup expired from DB
    now_ms    = time.time() * 1000
    cutoff_ms = int(now_ms - (MAX_AGE_HOURS + CLEANUP_BUFFER_HOURS) * 3600 * 1000)
    delete_expired_known_tokens(BOT_ID, cutoff_ms)
    # Remove from local dict too
    known = {k: v for k, v in known.items() if v == 0 or v > cutoff_ms}
    log.info(f"[Cleanup]   Known after cleanup: {len(known)}")

    log.info(f"[Recheck]   {len(known)} known addresses...")
    all_pairs.extend(batch_fetch_by_address(list(known.keys())))
    all_pairs = deduplicate(all_pairs)
    log.info(f"[Total]     {len(all_pairs)} unique PumpSwap pairs")

    return all_pairs, known, new_entries

# ── Filters ───────────────────────────────────────────────────────────────────
def apply_filters(pairs: list) -> list:
    out = []
    now_ms = time.time() * 1000
    for p in pairs:
        try:
            mc = p.get("marketCap") or p.get("fdv") or 0
            if not (MIN_MARKET_CAP <= mc <= MAX_MARKET_CAP): continue
            vol24h = (p.get("volume") or {}).get("h24") or 0
            if vol24h < MIN_24H_VOL: continue
            chg24h = (p.get("priceChange") or {}).get("h24") or 0
            if chg24h < MIN_24H_CHG: continue
            created_at = p.get("pairCreatedAt")
            if created_at:
                age_h = (now_ms - created_at) / (1000 * 3600)
                if not (MIN_AGE_HOURS <= age_h <= MAX_AGE_HOURS): continue
            info = p.get("info") or {}
            if not (info.get("imageUrl") or info.get("websites") or info.get("socials")): continue
            out.append(p)
        except Exception as e:
            log.warning(f"Filter error: {e}")
    return out

# ── Age formatter ─────────────────────────────────────────────────────────────
def format_age(created_at_ms) -> str:
    if not created_at_ms: return "N/A"
    total_minutes = int((time.time() * 1000 - created_at_ms) / 60000)
    if total_minutes < 60: return f"{total_minutes}m"
    total_hours = total_minutes // 60
    if total_hours < 24:
        mins = total_minutes % 60
        return f"{total_hours}h{mins}m" if mins else f"{total_hours}h"
    days = total_hours // 24; hours = total_hours % 24
    return f"{days}d{hours}h" if hours else f"{days}d"

# ── Telegram ──────────────────────────────────────────────────────────────────
def tg_send(chat_id, text, parse_mode="HTML", preview=False):
    try:
        r = requests.post(f"{TELEGRAM_API}/sendMessage",
            json={"chat_id": chat_id, "text": text,
                  "parse_mode": parse_mode, "disable_web_page_preview": not preview},
            timeout=10)
        return r.status_code == 200
    except Exception as e:
        log.error(f"tg_send: {e}"); return False

def social_url(platform, handle):
    if not handle: return None
    if "twitter"  in platform: return f"https://twitter.com/{handle}"
    if "telegram" in platform: return f"https://t.me/{handle}"
    return None

def send_alert(pair):
    token     = pair.get("baseToken", {})
    name      = token.get("name", "Unknown")
    symbol    = token.get("symbol", "???")
    address   = token.get("address", "")
    pair_addr = pair.get("pairAddress", "")
    price_usd = pair.get("priceUsd") or "N/A"
    mc        = pair.get("marketCap") or pair.get("fdv") or 0
    vol24h    = (pair.get("volume") or {}).get("h24") or 0
    chg24h    = (pair.get("priceChange") or {}).get("h24") or 0
    liquidity = (pair.get("liquidity") or {}).get("usd") or 0
    age_str   = format_age(pair.get("pairCreatedAt"))
    dex_url   = f"https://dexscreener.com/solana/{pair_addr}"
    pump_url  = f"https://pump.fun/{address}"
    info      = pair.get("info") or {}
    sites     = info.get("websites") or []
    socials   = info.get("socials") or []
    website   = sites[0].get("url") if sites else None
    twitter   = next((social_url(s.get("platform",""), s.get("handle",""))
                      for s in socials if "twitter"  in (s.get("platform") or "").lower()), None)
    tg_link   = next((social_url(s.get("platform",""), s.get("handle",""))
                      for s in socials if "telegram" in (s.get("platform") or "").lower()), None)
    links = []
    if website: links.append(f"<a href='{website}'>Website</a>")
    if twitter: links.append(f"<a href='{twitter}'>Twitter</a>")
    if tg_link: links.append(f"<a href='{tg_link}'>Telegram</a>")
    chg_sign = "+" if chg24h >= 0 else ""
    msg = (
        f"<b>New Token Alert!</b>\n\n"
        f"<b>{name}</b> <code>${symbol}</code>\n"
        f"------------------------\n"
        f"Price:       <b>${price_usd}</b>\n"
        f"Market Cap:  <b>${mc:,.0f}</b>\n"
        f"Liquidity:   <b>${liquidity:,.0f}</b>\n"
        f"24H Vol:     <b>${vol24h:,.0f}</b>\n"
        f"24H Change:  <b>{chg_sign}{chg24h:.1f}%</b>\n"
        f"Age:         <b>{age_str}</b>\n"
        f"------------------------\n"
        f"<a href='{dex_url}'>DexScreener</a>  |  <a href='{pump_url}'>Pump.fun</a>\n"
        f"{'  |  '.join(links) or 'No socials'}\n\n"
        f"<code>{address}</code>"
    )
    if tg_send(TELEGRAM_CHAT_ID, msg, preview=True):
        log.info(f"[OK] Alert: {name} ({symbol})")

# ── Commands ──────────────────────────────────────────────────────────────────
def handle_status(chat_id):
    with state_lock:
        pairs = list(current_matching); cycle = last_cycle_time
    if not pairs:
        tg_send(chat_id, f"No tokens currently match filters.\n<i>Last checked: {cycle}</i>"); return
    pairs.sort(key=lambda p: p.get("marketCap") or 0, reverse=True)
    lines = [f"<b>Matching tokens: {len(pairs)}</b>  |  <i>{cycle}</i>\n"]
    for i, p in enumerate(pairs, 1):
        token = p.get("baseToken", {})
        mc    = p.get("marketCap") or p.get("fdv") or 0
        vol24h = (p.get("volume") or {}).get("h24") or 0
        chg24h = (p.get("priceChange") or {}).get("h24") or 0
        chg_sign = "+" if chg24h >= 0 else ""
        lines.append(
            f"{i}. <a href='https://dexscreener.com/solana/{p.get(\"pairAddress\",\"\")}'>"
            f"<b>{token.get('name','?')}</b> ${token.get('symbol','?')}</a>\n"
            f"   MC ${mc:,.0f}  |  Vol ${vol24h:,.0f}  |  {chg_sign}{chg24h:.1f}%  |  {format_age(p.get('pairCreatedAt'))} old"
        )
    chunks, cur = [], lines[0]
    for line in lines[1:]:
        if len(cur) + len(line) + 1 > 4000: chunks.append(cur); cur = line
        else: cur += "\n" + line
    chunks.append(cur)
    for chunk in chunks: tg_send(chat_id, chunk, preview=False); time.sleep(0.3)

def handle_count(chat_id):
    with state_lock: n, cycle = len(current_matching), last_cycle_time
    tg_send(chat_id, f"<b>{n} token(s)</b> matching filters.\n<i>Last checked: {cycle}</i>")

def handle_help(chat_id):
    tg_send(chat_id,
        "<b>Commands:</b>\n/status — full list\n/count — quick count\n/help — this\n\n"
        f"<i>MC ${MIN_MARKET_CAP/1000:.0f}K-${MAX_MARKET_CAP/1000:.0f}K | "
        f"Vol >=${MIN_24H_VOL/1000:.0f}K | 24H >={MIN_24H_CHG}% | "
        f"Age {MIN_AGE_HOURS}-{MAX_AGE_HOURS}h | Has profile</i>")

def poll_commands():
    offset = None
    log.info("Command listener started.")
    while True:
        try:
            params = {"timeout": 30, "allowed_updates": ["message"]}
            if offset: params["offset"] = offset
            resp = requests.get(f"{TELEGRAM_API}/getUpdates", params=params, timeout=40)
            if resp.status_code != 200: time.sleep(5); continue
            for update in (resp.json().get("result") or []):
                offset  = update["update_id"] + 1
                msg     = update.get("message") or {}
                text    = (msg.get("text") or "").strip().lower()
                chat_id = (msg.get("chat") or {}).get("id")
                if not text or not chat_id: continue
                if str(chat_id) != str(TELEGRAM_CHAT_ID): continue
                log.info(f"Command: {text!r}")
                if   text.startswith("/status"): handle_status(chat_id)
                elif text.startswith("/count"):  handle_count(chat_id)
                elif text.startswith("/help"):   handle_help(chat_id)
        except Exception as e:
            log.warning(f"Poll error: {e}"); time.sleep(5)

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    global current_matching, last_cycle_time

    log.info("Bot 1 starting...")
    assert TELEGRAM_BOT_TOKEN, "Set TELEGRAM_BOT_TOKEN in .env"
    assert TELEGRAM_CHAT_ID,   "Set TELEGRAM_CHAT_ID in .env"

    init_db()
    alerted_tokens = load_seen_tokens(BOT_ID)
    known_tokens   = load_known_tokens(BOT_ID)
    log.info(f"Alerted: {len(alerted_tokens)}  |  Known: {len(known_tokens)}")

    tg_send(TELEGRAM_CHAT_ID,
        "<b>DexScreener Bot #1 Started!</b>\n\n"
        "Monitoring <b>PumpSwap (Solana)</b>:\n"
        f"  - Market Cap: ${MIN_MARKET_CAP/1000:.0f}K - ${MAX_MARKET_CAP/1000:.0f}K\n"
        f"  - 24H Volume: >= ${MIN_24H_VOL/1000:.0f}K\n"
        f"  - 24H Change: >= +{MIN_24H_CHG}%\n"
        f"  - Age: {MIN_AGE_HOURS}h - {MAX_AGE_HOURS}h\n"
        f"  - Has profile: Yes\n\n"
        f"Polling every <b>{POLL_INTERVAL}s</b>\n"
        "Commands: /status | /count | /help"
    )

    threading.Thread(target=poll_commands, daemon=True).start()

    while True:
        try:
            log.info("=== New cycle ===")
            pairs, known_tokens, new_entries = discover_all_pairs(known_tokens)

            # Persist new known tokens to DB
            if new_entries:
                save_known_tokens(BOT_ID, new_entries)

            matching = apply_filters(pairs)
            log.info(f"[Filter]    {len(matching)} pairs pass all filters")

            with state_lock:
                current_matching = matching
                last_cycle_time  = time.strftime("%Y-%m-%d %H:%M:%S")

            newly_alerted = set()
            for pair in matching:
                addr = (pair.get("baseToken") or {}).get("address", "")
                if addr and addr not in alerted_tokens:
                    alerted_tokens.add(addr)
                    newly_alerted.add(addr)
                    send_alert(pair)
                    time.sleep(0.5)

            if newly_alerted:
                save_seen_tokens(BOT_ID, newly_alerted)
                log.info(f"[Alerts]    Sent {len(newly_alerted)} new alert(s)")
            else:
                log.info("[Alerts]    No new tokens this cycle")

        except Exception as e:
            log.exception(f"Unexpected error: {e}")

        log.info(f"Sleeping {POLL_INTERVAL}s...\n")
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
