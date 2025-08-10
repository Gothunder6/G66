
# === IMPORT & CONFIG ===
import MetaTrader5 as mt5
import time
import requests
import numpy as np
import gspread
from datetime import datetime, timedelta, timezone
from config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
from oauth2client.service_account import ServiceAccountCredentials
import logging
import traceback
import threading
import random
import os

# --- Plot backend for headless environments ---
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# === CONFIG ===
SERVICE_ACCOUNT_FILE = r'F:\N8N\ForexSignal\Sheet\gsheet_creds.json'
SHEET_URL = "https://docs.google.com/spreadsheets/d/1L1ZUwF5jD-_e8SrUkkmu_xLWxdhIDC7nr5T4S3S56Pw/edit#gid=543657138"
SHEET_NAME = "Signal"
EMA_PERIOD = 50

SYMBOLS = [
    "EURUSD.A", "GBPUSD.A", "AUDUSD.A", "USDJPY.A", "USDCAD.A", "NZDUSD.A", "EURGBP.A", "USDCHF.A",
    "EURJPY.A", "GBPJPY.A", "AUDJPY.A", "CADJPY.A", "NZDJPY.A",
    "US30.A", "NAS100.A", "US500.A", "JPN225.A",
    "XAUUSD.A", "XAGUSD.A",
    "BTCUSD.A", "ETHUSD.A"
]

# map -> number of digits for formatting
symbol_digits = {
    "EURUSD.A": 5, "GBPUSD.A": 5, "AUDUSD.A": 5, "NZDUSD.A": 5, "USDCAD.A": 5, "USDCHF.A": 5,
    "EURGBP.A": 5, "EURJPY.A": 3, "GBPJPY.A": 3, "AUDJPY.A": 3, "CADJPY.A": 3, "NZDJPY.A": 3,
    "US30.A": 1, "NAS100.A": 2, "US500.A": 2, "JPN225.A": 1,
    "XAUUSD.A": 2, "XAGUSD.A": 3,
    "BTCUSD.A": 2, "ETHUSD.A": 2,
}

# --- Logic 1 — BLOCK_NEW_WHEN_RUNNING ---
BLOCK_NEW_WHEN_RUNNING_PER_SYMBOL = True   # ล็อกเฉพาะสัญลักษณ์นั้นๆ
BLOCK_NEW_WHEN_RUNNING_GLOBAL      = False # ล็อกทั้งระบบ

# --- Logic 2 — MARKET GUARD (weekend + fresh tick + new bar only) ---
MARKET_GUARD_ENABLED = True
FOREX_WEEKEND_BLOCK  = True           # บล็อกเสาร์-อาทิตย์สำหรับ Forex เท่านั้น
ACTIVE_WEEKDAYS_LOCAL= {0,1,2,3,4}    # จันทร์=0 ... ศุกร์=4 (Asia/Bangkok)TICK_MAX_AGE_SEC     = 900            # 15 นาทีถ้าไม่มี tick ถือว่าไม่สด

# --- Weekend policy ---
# อนุญาตให้เทรดเฉพาะสัญลักษณ์เหล่านี้ในเสาร์–อาทิตย์ (วันอื่นทำงานตามปกติ)
WEEKEND_ALLOWED_SYMBOLS = {"BTCUSD.A", "ETHUSD.A"}

# --- Logic 3 — Robust execution guards (spread/session/ATR fallback/RR guards) ---
SPREAD_MAX_MAP = {
    "US30.A":   6.0,   # points
    "NAS100.A": 4.0,
    "US500.A":  1.5,
    "BTCUSD.A": 25.0,
    "ETHUSD.A": 8.0,
}

# เวลาตลาด US (โดยประมาณ, เวลาไทย UTC+7)
SESSION_WINDOWS_LOCAL = {
    "US30.A":   [(20, 30, 23, 59), (0, 0, 3, 0)],
    "NAS100.A": [(20, 30, 23, 59), (0, 0, 3, 0)],
    "US500.A":  [(20, 30, 23, 59), (0, 0, 3, 0)],
}

# ATR fallback controls
FALLBACK_USE_ATR = True
ATR_MULT = {
    "US30.A":   0.8,
    "NAS100.A": 0.9,
    "US500.A":  0.7,
    "BTCUSD.A": 1.2,
    "ETHUSD.A": 1.2,
}

# Trailing to Break-even after TP1 (disabled by default)
TRAIL_TO_BE_AFTER_TP1 = False

# Grouping for guard logic
FOREX_SYMBOLS = {
    "EURUSD.A", "GBPUSD.A", "AUDUSD.A", "USDJPY.A", "USDCAD.A", "NZDUSD.A", "EURGBP.A", "USDCHF.A",
    "EURJPY.A", "GBPJPY.A", "AUDJPY.A", "CADJPY.A", "NZDJPY.A",
}
CRYPTO_SYMBOLS = {"BTCUSD.A", "ETHUSD.A"}
INDEX_SYMBOLS  = {"US30.A", "NAS100.A", "US500.A", "JPN225.A"}

# --- Google Sheet auth ---
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credentials   = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
gsheet_client = gspread.authorize(credentials)
sheet         = gsheet_client.open_by_url(SHEET_URL).worksheet(SHEET_NAME)

logging.basicConfig(
    filename="signal_system.log",
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO
)

# Cache: remember last root Telegram message id per symbol (only in-memory)
LAST_SIGNAL_MSG_ID = {}

# === UTILITY ===
def log(msg, level="info"):
    print(msg)
    if level == "error":
        logging.error(msg)
    elif level == "warning":
        logging.warning(msg)
    else:
        logging.info(msg)

# --- Cached Google Sheet fetch with exponential backoff & jitter ---
_SHEET_CACHE_TS = 0.0
_SHEET_CACHE_DATA = None
_SHEET_CACHE_TTL = 45.0  # seconds
_SHEET_MAX_RETRIES = 6
_SHEET_BASE_BACKOFF = 1.0

def get_all_sheet_records_with_retry():
    global _SHEET_CACHE_TS, _SHEET_CACHE_DATA
    now_monotonic = time.monotonic()
    # return cached data if still fresh
    if _SHEET_CACHE_DATA is not None and (now_monotonic - _SHEET_CACHE_TS) < _SHEET_CACHE_TTL:
        return _SHEET_CACHE_DATA

    last_err = None
    backoff = _SHEET_BASE_BACKOFF
    for i in range(1, _SHEET_MAX_RETRIES + 1):
        try:
            records = sheet.get_all_records()
            _SHEET_CACHE_TS = time.monotonic()
            _SHEET_CACHE_DATA = records
            return records
        except Exception as e:
            last_err = e
            msg = str(e)
            # Detect rate limit
            is_rate = ("429" in msg) or ("Quota exceeded" in msg) or ("Rate Limit" in msg)
            # log and backoff
            log(f"[GoogleSheet] Retry {i}: {e}", "warning")
            if i >= _SHEET_MAX_RETRIES:
                break
            # exponential backoff with jitter; heavier if rate-limited
            sleep_s = backoff + (random.uniform(0, 0.5))
            if is_rate:
                sleep_s = max(sleep_s, backoff)  # keep same
            if sleep_s > 64.0:
                sleep_s = 64.0
            time.sleep(sleep_s)
            backoff *= 2.0
    raise Exception("GoogleSheet: Failed to get all records after retry.") from last_err
def append_row_with_retry(row, max_retry=5):
    for i in range(max_retry):
        try:
            sheet.append_row(row)
            return
        except Exception as e:
            log(f"[GoogleSheet] append_row Retry {i+1}: {e}", "warning")
            time.sleep(2)
    raise Exception("GoogleSheet: Failed to append row after retry.")

def update_cell_with_retry(row, col, value, max_retry=5):
    for i in range(max_retry):
        try:
            sheet.update_cell(row, col, value)
            return
        except Exception as e:
            log(f"[GoogleSheet] update_cell Retry {i+1}: {e}", "warning")
            time.sleep(2)
    raise Exception("GoogleSheet: Failed to update cell after retry.")

def log_daily_summary_to_sheet(date, total, tp, sl, expired):
    try:
        sheet_summary = gsheet_client.open_by_url(SHEET_URL).worksheet("DailySummary")
        sheet_summary.append_row([date, total, tp, sl, expired])
    except Exception as e:
        log(f"[GoogleSheet] Log Daily Summary Fail: {e}", "warning")

# Telegram
def send_telegram_message(text, reply_to_message_id=None, parse_mode="Markdown"):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": parse_mode}
    if reply_to_message_id:
        payload["reply_to_message_id"] = reply_to_message_id
        payload["allow_sending_without_reply"] = True
    try:
        r = requests.post(url, data=payload)
        print(f"[Telegram Debug] status_code={r.status_code}, response={r.text}")
        if not r.ok:
            log(f"Telegram API Error: {r.text}", "warning")
        else:
            data = r.json()
            return data.get("result", {}).get("message_id")
    except Exception as e:
        log(f"Telegram Error: {e}", "error")
    return None

def send_telegram_photo(image_path, caption=None, parse_mode=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
    message_id = None
    try:
        with open(image_path, "rb") as photo:
            data = {"chat_id": TELEGRAM_CHAT_ID}
            if caption:
                data["caption"] = caption
            if parse_mode:
                data["parse_mode"] = parse_mode
            r = requests.post(url, data=data, files={"photo": photo})
        print(f"[Telegram Debug] (photo) status_code={r.status_code}, response={r.text}")
        if not r.ok:
            log(f"Telegram API Error (photo): {r.text}", "warning")
        else:
            try:
                js = r.json()
                message_id = js.get("result", {}).get("message_id")
            except Exception:
                pass
    except Exception as e:
        log(f"Telegram Photo Error: {e}", "error")
    finally:
        # Always try to remove local file
        try:
            if os.path.exists(image_path):
                os.remove(image_path)
        except Exception as e:
            log(f"Warning: cannot delete temp chart {image_path}: {e}", "warning")
    return message_id

# Helpers
def format_price(value, digits):
    try:
        return f"{value:.{digits}f}"
    except Exception:
        return str(value)

def get_float_safe(d, key):
    try:
        v = d.get(key, "")
        if v is None or v == "" or str(v).strip() == "":
            return None
        v = str(v).replace(',', '').replace('\\xa0', '').strip()
        result = float(v)
        return result
    except Exception as e:
        print(f"get_float_safe error: {key}={v} ({e})")
        return None

# === unified open/closed checks ===
CLOSED_RESULTS = {"TP1", "TP2", "TP3", "SL", "Expired"}

def is_closed_result(res):
    return str(res).strip() in CLOSED_RESULTS

def find_open_orders():
    records = get_all_sheet_records_with_retry()
    open_orders = []
    for i, r in enumerate(records, start=2):
        if not is_closed_result(r.get('Result', '')):
            open_orders.append((i, r))
    return open_orders

# === ORDER EXPIRY ===
def order_expired(order, expire_hr=4):
    dt_str = order.get('Date', '')
    try:
        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
        expire_dt = dt + timedelta(hours=expire_hr)
        return datetime.now() > expire_dt
    except Exception:
        return False

# === SHEET UPDATE WRAPPERS ===
def update_order_result_in_sheet(row_idx, result, note=None):
    # Result at column 9, Note at column 12
    update_cell_with_retry(row_idx, 9, result)
    if note:
        update_cell_with_retry(row_idx, 12, note)

def update_order_sl_in_sheet(row_idx, new_sl):
    update_cell_with_retry(row_idx, 5, new_sl)  # SL at column 5

# === MESSAGE BUILDERS ===
def build_tp_sl_message(order, result):
    symbol = order.get('Symbol', '')
    direction = order.get('Direction', '').upper()
    entry = float(order.get('Entry', 0))
    sl    = float(order.get('SL', 0))
    tp1   = float(order.get('TP1', 0))
    tp2   = float(order.get('TP2', 0))
    tp3   = float(order.get('TP3', 0))
    digits = symbol_digits.get(symbol, 2)

    order_ref = f"{symbol} {direction} @{order.get('Date', '')}"

    if result in ["TP1", "TP2", "TP3"]:
        price_close = {"TP1": tp1, "TP2": tp2, "TP3": tp3}[result]
        pip = (price_close - entry) if direction == "BUY" else (entry - price_close)
        header = f"🎯 *{result}!* {symbol} +{round(pip / (0.01 if digits==3 else 0.0001), 1)} pip  \n(Order: {order_ref})"
        footer = "ออเดอร์นี้ปิดกำไรสำเร็จ\nเทรดตามแผน รักษาวินัย บริหารพอร์ตต่อเนื่อง"
    elif result == "SL":
        pip = (sl - entry) if direction == "BUY" else (entry - sl)
        header = f"⚠️ *SL!* {symbol} {round(pip / (0.01 if digits==3 else 0.0001), 1)} pip  \n(Order: {order_ref})"
        footer = "ออเดอร์นี้ปิดขาดทุน\nวางแผน บริหารพอร์ต เดินหน้าสู่โอกาสครั้งต่อไป"
    elif result == "Expired":
        header = f"⌛ *Expired*: {symbol}  \n(Order: {order_ref})"
        footer = "ออเดอร์หมดอายุ ปิดตามแผนการบริหารความเสี่ยง"
    else:
        return ""
    return f"{header}\n\n{footer}"

def build_entry_signal_message(order):
    symbol    = order.get('Symbol', '')
    direction = order.get('Direction', '').upper()
    entry = format_price(order.get('Entry', 0), symbol_digits.get(symbol, 2))
    sl    = format_price(order.get('SL', 0), symbol_digits.get(symbol, 2))
    tp1   = format_price(order.get('TP1', 0), symbol_digits.get(symbol, 2))
    tp2   = format_price(order.get('TP2', 0), symbol_digits.get(symbol, 2))
    tp3   = format_price(order.get('TP3', 0), symbol_digits.get(symbol, 2))
    time_open = order.get('Date', '')
    pattern   = order.get('Pattern', '')
    note      = order.get('Note', '')

    reason = ""
    if pattern and note:
        reason = f"{pattern} ({note})"
    elif pattern:
        reason = pattern
    elif note:
        reason = note

    msg = (
        f"🚦 *New Trade Signal*\n\n"
        f"[{symbol}] {'📈 *BUY*' if direction == 'BUY' else '📉 *SELL*'}\n"
        f"Entry: `{entry}`\n"
        f"SL: `{sl}`\n"
        f"TP1: `{tp1}` | TP2: `{tp2}` | TP3: `{tp3}`\n"
    )
    if reason:
        msg += f"\n📋 เหตุผล: _{reason}_\n"
    msg += (
        f"\n⏱ ส่งเมื่อ: `{time_open}`\n"
        f"\n🚨 *อย่าลืมบริหารพอร์ต และจัดการความเสี่ยงทุกครั้ง!*\n"
        f"#BegintoPro"
    )
    return msg

# === PRICE / MT5 UTILS ===
def get_candles(symbol, timeframe, count):
    if not mt5.initialize():
        log(f"❌ MT5 Init Fail: {symbol}", "error")
        return []
    rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, count)
    mt5.shutdown()
    if rates is None or len(rates) == 0:
        log(f"❌ MT5 Get Rates Fail: {symbol}", "error")
        return []
    return [{'open': r['open'], 'high': r['high'], 'low': r['low'], 'close': r['close']} for r in rates]

def get_tick(symbol):
    if not mt5.initialize():
        log(f"❌ MT5 Init Fail: {symbol}", "error")
        return None
    tick = mt5.symbol_info_tick(symbol)
    mt5.shutdown()
    return tick

# Ensure all required symbols are visible in MT5 Market Watch
def mt5_select_symbols(symbols):
    if not mt5.initialize():
        log("❌ MT5 Init Fail in mt5_select_symbols", "warning")
        return
    try:
        for s in symbols:
            info = mt5.symbol_info(s)
            if info is None or not info.visible:
                mt5.symbol_select(s, True)
    finally:
        try:
            mt5.shutdown()
        except Exception:
            pass

# === Chart capture ===
def capture_chart(symbol, entry, sl, tp1, tp2, tp3, bars=100):
    candles = get_candles(symbol, mt5.TIMEFRAME_M15, bars)
    if not candles:
        return None

    opens  = [c['open'] for c in candles]
    closes = [c['close'] for c in candles]
    highs  = [c['high'] for c in candles]
    lows   = [c['low']  for c in candles]
    n = len(candles)

    fig, ax = plt.subplots(figsize=(10,5))

    for i in range(n):
        color = 'green' if closes[i] >= opens[i] else 'red'
        ax.plot([i, i], [lows[i], highs[i]], color=color, linewidth=1)
        lower = min(opens[i], closes[i])
        height = abs(opens[i] - closes[i])
        height = height if height > 1e-6 else 0.0001
        ax.add_patch(plt.Rectangle((i-0.35, lower), 0.7, height, facecolor=color, edgecolor=color))

    levels = {'Entry': entry, 'SL': sl, 'TP1': tp1, 'TP2': tp2, 'TP3': tp3}
    colors = {'Entry': 'orange', 'SL': 'red', 'TP1': 'green', 'TP2': 'green', 'TP3': 'green'}
    for label, price in levels.items():
        ax.axhline(price, color=colors[label], linestyle='--', linewidth=1)
        ax.text(n+0.8, price, f"{label} {price:.2f}", va='center', color=colors[label], fontsize=10)

    ax.text(0.5, 0.5, "BTP", transform=ax.transAxes, fontsize=90, color='gray', alpha=0.15, ha='center', va='center', fontweight='bold')

    for side in ['left','bottom','right','top']:
        ax.spines[side].set_visible(False)
    ax.get_xaxis().set_visible(False)
    ax.get_yaxis().set_visible(False)

    ax.set_xlim(-1, n+6)
    ax.set_title(f"{symbol} M15 (Entry/SL/TP)")

    img_path = f"chart_{symbol.replace('.', '_')}_{int(time.time())}.png"
    plt.tight_layout()
    plt.savefig(img_path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    return img_path

# === Trend/Pattern detectors ===
def ema(arr, period):
    """True EMA (exponential moving average)."""
    arr = np.asarray(arr, dtype=float)
    if len(arr) == 0:
        return np.array([])
    # Seed with the first value to avoid lookback bias for short history
    alpha = 2.0 / (period + 1.0)
    out = [arr[0]]
    for x in arr[1:]:
        out.append(alpha * x + (1 - alpha) * out[-1])
    return np.array(out)

def is_uptrend(closes):
    if len(closes) < EMA_PERIOD:
        return False
    ema_val = ema(np.array(closes), EMA_PERIOD)
    return closes[-1] > ema_val[-1]

def is_downtrend(closes):
    if len(closes) < EMA_PERIOD:
        return False
    ema_val = ema(np.array(closes), EMA_PERIOD)
    return closes[-1] < ema_val[-1]

def detect_engulfing(c):
    if len(c) < 2: return None
    prev, curr = c[-2], c[-1]
    if prev['close'] < prev['open'] and curr['close'] > curr['open'] and curr['close'] > prev['open'] and curr['open'] < prev['close']:
        return "Bullish Engulfing"
    if prev['close'] > prev['open'] and curr['close'] < curr['open'] and curr['close'] < prev['open'] and curr['open'] > prev['close']:
        return "Bearish Engulfing"
    return None

def is_pinbar(c):
    last = c[-1]
    body = abs(last['close'] - last['open'])
    upper_wick = last['high'] - max(last['close'], last['open'])
    lower_wick = min(last['close'], last['open']) - last['low']
    if upper_wick > 2 * body and upper_wick > lower_wick:
        return "Pinbar Top"
    if lower_wick > 2 * body and lower_wick > upper_wick:
        return "Pinbar Bottom"
    return None

def is_double_top(c):
    if len(c) < 5: return False
    a, b, c1, d, e = c[-5], c[-4], c[-3], c[-2], c[-1]
    return (a['high'] < b['high'] and b['high'] > c1['high'] and d['high'] < b['high'] and abs(b['high'] - d['high']) < 0.002 * b['high'] and e['close'] < d['low'])

def is_double_bottom(c):
    if len(c) < 5: return False
    a, b, c1, d, e = c[-5], c[-4], c[-3], c[-2], c[-1]
    return (a['low'] > b['low'] and b['low'] < c1['low'] and d['low'] > b['low'] and abs(b['low'] - d['low']) < 0.002 * b['low'] and e['close'] > d['high'])

def is_morning_star(c):
    if len(c) < 3: return False
    return (c[-3]['close'] < c[-3]['open'] and c[-2]['low'] < c[-3]['close'] and abs(c[-2]['close'] - c[-2]['open']) < abs(c[-1]['close'] - c[-1]['open']) and c[-1]['close'] > c[-1]['open'] and c[-1]['close'] > c[-3]['open'])

def is_evening_star(c):
    if len(c) < 3: return False
    return (c[-3]['close'] > c[-3]['open'] and c[-2]['high'] > c[-3]['close'] and abs(c[-2]['close'] - c[-2]['open']) < abs(c[-1]['close'] - c[-1]['open']) and c[-1]['close'] < c[-1]['open'] and c[-1]['close'] < c[-3]['open'])

def detect_qm(c):
    if len(c) < 5: return None
    h1 = c[-5]['high']; l1 = c[-4]['low']; h2 = c[-3]['high']; l2 = c[-2]['low']; h3 = c[-1]['high']
    if l1 < l2 and h2 > h1 and l2 < l1 and h3 > h2: return "QM Buy"
    if h1 > h2 and l2 > l1 and h3 < h2 and l2 > l1: return "QM Sell"
    return None

def detect_imbalance(c):
    last = c[-1]
    body = abs(last['close'] - last['open'])
    wick = last['high'] - last['low']
    return body / wick > 0.7 if wick > 0 else False

def detect_demand_zone(candles):
    if len(candles) < 10: return False
    base = candles[-6:-1]
    last = candles[-1]
    base_high = max(c['high'] for c in base)
    base_low  = min(c['low']  for c in base)
    is_base = all(abs(c['close'] - c['open']) < (base_high - base_low)/2 for c in base)
    return is_base and last['close'] > base_high and last['close'] > last['open']

def detect_supply_zone(candles):
    if len(candles) < 10: return False
    base = candles[-6:-1]
    last = candles[-1]
    base_high = max(c['high'] for c in base)
    base_low  = min(c['low']  for c in base)
    is_base = all(abs(c['close'] - c['open']) < (base_high - base_low)/2 for c in base)
    return is_base and last['close'] < base_low and last['close'] < last['open']

def find_zone_levels(candles, entry, direction):
    highs, lows = [], []
    for i in range(2, len(candles)-2):
        if (candles[i]['high'] > candles[i-2]['high'] and candles[i]['high'] > candles[i-1]['high'] and candles[i]['high'] > candles[i+1]['high'] and candles[i]['high'] > candles[i+2]['high']):
            highs.append(candles[i]['high'])
        if (candles[i]['low'] < candles[i-2]['low'] and candles[i]['low'] < candles[i-1]['low'] and candles[i]['low'] < candles[i+1]['low'] and candles[i]['low'] < candles[i+2]['low']):
            lows.append(candles[i]['low'])
    dmz, spz = [], []
    for i in range(6, len(candles)):
        base = candles[i-6:i-1]
        last = candles[i-1]
        base_high = max(c['high'] for c in base)
        base_low  = min(c['low']  for c in base)
        is_base = all(abs(c['close']-c['open']) < (base_high-base_low)/2 for c in base)
        if is_base and last['close'] > base_high and last['close'] > last['open']:
            dmz.append(base_low)
        if is_base and last['close'] < base_low and last['close'] < last['open']:
            spz.append(base_high)
    if direction == "Buy":
        levels = sorted([z for z in highs + dmz if z > entry])
    else:
        levels = sorted([z for z in lows + spz if z < entry], reverse=True)
    return levels

# === MARKET GUARD HELPERS ===
LAST_BAR_TIME = {}  # key=(symbol, timeframe) -> epoch time of last bar

def is_forex_symbol(symbol: str) -> bool:
    return symbol in FOREX_SYMBOLS

def is_crypto_symbol(symbol: str) -> bool:
    return symbol in CRYPTO_SYMBOLS

def is_market_open(symbol: str) -> bool:

    if not MARKET_GUARD_ENABLED:
        return True

    # Weekend policy:
    # - ถ้าเป็นเสาร์/อาทิตย์ -> อนุญาตเฉพาะสัญลักษณ์ใน WEEKEND_ALLOWED_SYMBOLS
    wd = datetime.now().weekday()  # Mon=0 ... Sun=6
    if wd not in ACTIVE_WEEKDAYS_LOCAL:
        if symbol not in WEEKEND_ALLOWED_SYMBOLS:
            return False

    # เดิม: บล็อกเสาร์–อาทิตย์สำหรับ Forex (คงไว้เพื่อความเข้มงวดซ้อนทับ)
    if FOREX_WEEKEND_BLOCK and is_forex_symbol(symbol):
        if wd not in ACTIVE_WEEKDAYS_LOCAL:
            return False

    # Tick freshness guard (all symbols)
# Tick freshness guard (all symbols)
    if not mt5.initialize():
        log("❌ MT5 Init Fail in is_market_open", "warning")
        return False
    tick = mt5.symbol_info_tick(symbol)
    mt5.shutdown()
    if not tick:
        return False
    age_sec = datetime.now(timezone.utc).timestamp() - float(tick.time)
    if age_sec > globals().get("TICK_MAX_AGE_SEC", 900):
        return False

    return True

def has_new_bar(symbol: str, timeframe) -> bool:
    """Return True only when a new bar appears in MT5 for the symbol/timeframe."""
    if not mt5.initialize():
        log("❌ MT5 Init Fail in has_new_bar", "warning")
        return False
    rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, 2)
    mt5.shutdown()
    if rates is None or len(rates) < 2:
        return False
    last_time = int(rates[-1]['time'])  # epoch seconds
    k = (symbol, timeframe)
    prev = LAST_BAR_TIME.get(k)
    if prev is None:
        LAST_BAR_TIME[k] = last_time
        return False  # first call -> prime memory
    if last_time <= prev:
        return False
    LAST_BAR_TIME[k] = last_time
    return True

# === SPREAD & SESSION GUARDS ===
def _point_from_digits(symbol: str) -> float:
    d = symbol_digits.get(symbol, 2)
    return 10 ** (-d)

def spread_ok(symbol: str, tick) -> bool:
    # convert to points based on digits
    point = _point_from_digits(symbol)
    spr_points = (float(tick.ask) - float(tick.bid)) / point
    limit = SPREAD_MAX_MAP.get(symbol, None)
    if limit is None:
        return True
    return spr_points <= limit

def in_session_local(symbol: str, dt_local: datetime) -> bool:
    wins = SESSION_WINDOWS_LOCAL.get(symbol)
    if not wins:
        return True
    h, m = dt_local.hour, dt_local.minute
    for (sh, sm, eh, em) in wins:
        start = (sh, sm)
        end   = (eh, em)
        cur   = (h, m)
        if start <= end:
            if start <= cur <= end:
                return True
        else:
            # over midnight
            if cur >= start or cur <= end:
                return True
    return False

# === SL/TP CALC ===
def calculate_sl_tp(symbol, entry, candles, direction):
    digits = symbol_digits.get(symbol, 2)

    offset_map = {
        "EURUSD.A":  (0.0015, 0.0025), "GBPUSD.A":  (0.0020, 0.0040), "AUDUSD.A":  (0.0012, 0.0022),
        "NZDUSD.A":  (0.0012, 0.0022), "USDCAD.A":  (0.0015, 0.0030), "USDCHF.A":  (0.0010, 0.0020),
        "EURGBP.A":  (0.0012, 0.0022), "EURJPY.A":  (0.10, 0.18), "GBPJPY.A":  (0.12, 0.22),
        "AUDJPY.A":  (0.10, 0.20), "CADJPY.A":  (0.10, 0.20), "NZDJPY.A":  (0.10, 0.20), "USDJPY.A":  (0.10, 0.18),
        "US30.A":    (80, 200), "NAS100.A":  (80, 200), "US500.A":   (80, 200),
        "JPN225.A":  (18, 45), "XAUUSD.A":  (1.0, 2.5), "XAGUSD.A":  (0.02, 0.04),
        "BTCUSD.A":  (80, 200), "ETHUSD.A":  (80, 200),
    }

    min_gap_map = {
        "EURUSD.A":  0.0008, "GBPUSD.A":  0.0010, "AUDUSD.A":  0.0008, "NZDUSD.A":  0.0008,
        "USDCAD.A":  0.0010, "USDCHF.A":  0.0008, "EURGBP.A":  0.0008, "EURJPY.A":  0.05, "GBPJPY.A":  0.05,
        "AUDJPY.A":  0.05, "CADJPY.A":  0.05, "NZDJPY.A":  0.05, "USDJPY.A":  0.05, "US30.A":  120, "NAS100.A":  120,
        "US500.A":  16, "JPN225.A":  24, "XAUUSD.A":  0.5, "XAGUSD.A":  0.5,
        "BTCUSD.A":  120, "ETHUSD.A":  120,
    }

    sl_range = offset_map.get(symbol, (0.002, 0.003))
    sl_offset = random.uniform(*sl_range)
    min_gap = min_gap_map.get(symbol, 0.0002)

    zones = find_zone_levels(candles, entry, direction)

    if direction == "Buy":
        swl_candidates = [z for z in [c['low'] for c in candles[-7:-2]] + zones if z < entry]
        sl = min(swl_candidates) - sl_offset if swl_candidates else entry - sl_offset * 3
    else:
        swh_candidates = [z for z in [c['high'] for c in candles[-7:-2]] + zones if z > entry]
        sl = max(swh_candidates) + sl_offset if swh_candidates else entry + sl_offset * 3

    # dedupe/space zones by min_gap
    filtered_zones = []
    for z in sorted(zones, reverse=(direction == "Sell")):
        if not filtered_zones or abs(z - filtered_zones[-1]) >= min_gap:
            filtered_zones.append(z)
    zones = filtered_zones

    tp1, tp2, tp3 = None, None, None
    zone_buffer = min_gap * 0.30

    if direction == "Buy":
        zone_list = [z for z in zones if z > entry]
        if len(zone_list) >= 1: tp1 = zone_list[0] - zone_buffer
        if len(zone_list) >= 2: tp2 = zone_list[1] - zone_buffer
        if len(zone_list) >= 3: tp3 = zone_list[2] - zone_buffer
    else:
        zone_list = [z for z in zones if z < entry]
        if len(zone_list) >= 1: tp1 = zone_list[0] + zone_buffer
        if len(zone_list) >= 2: tp2 = zone_list[1] + zone_buffer
        if len(zone_list) >= 3: tp3 = zone_list[2] + zone_buffer

    rr = abs(entry - sl)
    factors = (1.5, 2.5, 4)
    for idx, tp in enumerate([tp1, tp2, tp3], start=1):
        if tp is None or abs(tp - entry) < min_gap:
            factor = factors[idx - 1]
            if direction == "Buy":
                if idx == 1: tp1 = entry + rr * factor
                elif idx == 2: tp2 = entry + rr * factor
                else: tp3 = entry + rr * factor
            else:
                if idx == 1: tp1 = entry - rr * factor
                elif idx == 2: tp2 = entry - rr * factor
                else: tp3 = entry - rr * factor

    sl, tp1, tp2, tp3 = round(sl, digits), round(tp1, digits), round(tp2, digits), round(tp3, digits)

    # validation
    side_ok = (direction == "Buy" and sl < entry and all(tp > entry for tp in [tp1, tp2, tp3])) or \
              (direction == "Sell" and sl > entry and all(tp < entry for tp in [tp1, tp2, tp3]))
    gap_ok  = all(abs(entry - v) > min_gap for v in [sl, tp1, tp2, tp3])
    value_ok= all(v is not None and v != 0 for v in [sl, tp1, tp2, tp3])

    if not (side_ok and gap_ok and value_ok):
        # --- ATR fallback ---
        if FALLBACK_USE_ATR:
            atr = get_atr(symbol, mt5.TIMEFRAME_M15)
            if atr is None:
                raise Exception(f"SL/TP validation failed and ATR missing -> entry={entry}, sl={sl}, tp1={tp1}, tp2={tp2}, tp3={tp3}")
            mult = ATR_MULT.get(symbol, 1.0)
            sl_dist = max(atr*mult, 6 * (10 ** (-digits)))
            # add spread buffer
            tick = get_tick(symbol)
            spr_pts = (float(tick.ask) - float(tick.bid)) / (10 ** (-digits)) if tick else 0.0
            sl_buffer = spr_pts * (10 ** (-digits))
            if direction == "Buy":
                sl  = round(entry - sl_dist - sl_buffer, digits)
                tp1 = round(entry + sl_dist * 1.0, digits)
                tp2 = round(entry + sl_dist * 1.6, digits)
                tp3 = round(entry + sl_dist * 2.4, digits)
            else:
                sl  = round(entry + sl_dist + sl_buffer, digits)
                tp1 = round(entry - sl_dist * 1.0, digits)
                tp2 = round(entry - sl_dist * 1.6, digits)
                tp3 = round(entry - sl_dist * 2.4, digits)
        else:
            raise Exception(f"SL/TP validation failed -> entry={entry}, sl={sl}, tp1={tp1}, tp2={tp2}, tp3={tp3}, min_gap={min_gap}")

    if direction == "Buy":
        tp1, tp2, tp3 = sorted([tp1, tp2, tp3])
    else:
        tp1, tp2, tp3 = sorted([tp1, tp2, tp3], reverse=True)

    return sl, [tp1, tp2, tp3]

# ATR helper
def get_atr(symbol, timeframe, period=14):
    if not mt5.initialize():
        log("❌ MT5 Init Fail in get_atr", "warning")
        return None
    rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, period+1)
    mt5.shutdown()
    if rates is None or len(rates) < period+1:
        return None
    tr = []
    for i in range(1, len(rates)):
        h,l,c1 = rates[i]['high'], rates[i]['low'], rates[i-1]['close']
        tr.append(max(h-l, abs(h-c1), abs(l-c1)))
    return sum(tr)/len(tr)

# === SIGNAL DUPLICATE CHECK ===
def check_symbol_for_new_signal(symbol):
    records = get_all_sheet_records_with_retry()
    now = datetime.now()
    for r in records[::-1]:
        if r.get('Symbol', '') == symbol:
            try:
                last_dt = datetime.strptime(r['Date'], "%Y-%m-%d %H:%M:%S")
                if (now - last_dt).total_seconds() < 1800:  # 30 นาที
                    return False
            except:
                pass
            break
    return True

# === M15 close waiter ===
def wait_for_m15_close():
    while True:
        now = datetime.now()
        if now.minute % 15 == 0 and now.second < 10:
            break
        time.sleep(5)
    log("ถึงเวลา M15 close")

# === DAILY/WEEKLY SUMMARY ===
def summarize_results_daily():
    records = get_all_sheet_records_with_retry()
    today = datetime.now().strftime("%Y-%m-%d")
    orders = [r for r in records if r['Date'].startswith(today)]
    win    = sum(1 for o in orders if str(o['Result']).startswith('TP'))
    loss   = sum(1 for o in orders if o['Result'] == 'SL')
    expire = sum(1 for o in orders if o['Result'] == 'Expired')
    msg = f"""📊 *สรุปผลประจำวัน {today}*
----------------------------

ออเดอร์ทั้งหมด: *{len(orders)}*
✅ TP: *{win}* ครั้ง
❌ SL: *{loss}* ครั้ง
⌛ Expired: *{expire}* ครั้ง
----------------------------

*ข้อมูลโดย Begintopro*"""
    send_telegram_message(msg)
    log_daily_summary_to_sheet(today, len(orders), win, loss, expire)

def summarize_results_weekly():
    records = get_all_sheet_records_with_retry()
    now = datetime.now()
    week_start = (now - timedelta(days=now.weekday())).strftime("%Y-%m-%d")
    week_end   = (now + timedelta(days=6-now.weekday())).strftime("%Y-%m-%d")
    orders = [r for r in records if week_start <= r['Date'][:10] <= week_end]
    win    = sum(1 for o in orders if str(o['Result']).startswith('TP'))
    loss   = sum(1 for o in orders if o['Result'] == 'SL')
    expire = sum(1 for o in orders if o['Result'] == 'Expired')
    msg = f"""📈 *สรุปผลสัปดาห์ {week_start} - {week_end}*
----------------------------

ออเดอร์ทั้งหมด: *{len(orders)}*
✅ TP: *{win}* ครั้ง
❌ SL: *{loss}* ครั้ง
⌛ Expired: *{expire}* ครั้ง
----------------------------

*ข้อมูลโดย Begintopro*"""
    send_telegram_message(msg)

# === LOCK HELPERS ===
def has_running_order_for_symbol(symbol: str) -> bool:
    records = get_all_sheet_records_with_retry()
    for r in records:
        if r.get('Symbol', '') == symbol and not is_closed_result(r.get('Result', '')):
            return True
    return False

def has_any_running_order() -> bool:
    records = get_all_sheet_records_with_retry()
    return any(not is_closed_result(r.get('Result', '')) for r in records)

# === ORDER STATUS CHECKER (thread) ===
def check_order_status(order, digits):
    symbol = order.get('Symbol', '')
    entry  = get_float_safe(order, 'Entry')
    sl     = get_float_safe(order, 'SL')
    tp1    = get_float_safe(order, 'TP1')
    tp2    = get_float_safe(order, 'TP2')
    tp3    = get_float_safe(order, 'TP3')
    direction = order.get('Direction', '')
    tick   = get_tick(symbol)
    if not tick or any(x is None for x in [entry, sl, tp1, tp2, tp3]):
        return "Running"
    price = float(tick.bid) if direction == "Buy" else float(tick.ask)
    price = round(price, digits)
    if direction == "Buy":
        if sl and price <= sl: return "SL"
        if tp3 and price >= tp3: return "TP3"
        if tp2 and price >= tp2: return "TP2"
        if tp1 and price >= tp1: return "TP1"
    elif direction == "Sell":
        if sl and price >= sl: return "SL"
        if tp3 and price <= tp3: return "TP3"
        if tp2 and price <= tp2: return "TP2"
        if tp1 and price <= tp1: return "TP1"
    return "Running"

def tp_sl_checker_loop():
    while True:
        try:
            open_orders = find_open_orders()
            for row_idx, order in open_orders:
                symbol = order.get('Symbol', '')
                digits = symbol_digits.get(symbol, 2)
                result = check_order_status(order, digits)
                if result and result != order.get('Result', ''):
                    update_order_result_in_sheet(row_idx, result)
                    if result != "Running":
                        msg = build_tp_sl_message(order, result)
                        root_id = LAST_SIGNAL_MSG_ID.get(symbol)
                        if root_id:
                            send_telegram_message(msg, reply_to_message_id=root_id)
                        else:
                            send_telegram_message(msg)
                elif order_expired(order) and order.get('Result', '') != "Expired":
                    update_order_result_in_sheet(row_idx, "Expired")
                    msg = build_tp_sl_message(order, "Expired")
                    root_id = LAST_SIGNAL_MSG_ID.get(symbol)
                    if root_id:
                        send_telegram_message(msg, reply_to_message_id=root_id)
                    else:
                        send_telegram_message(msg)

                # Optional: trail to BE (disabled by default)
                if TRAIL_TO_BE_AFTER_TP1 and result == "Running":
                    tick = get_tick(symbol)
                    if tick:
                        direction = order.get('Direction','').upper()
                        entry = float(order.get('Entry',0))
                        tp1   = float(order.get('TP1',0))
                        price = float(tick.bid) if direction=="BUY" else float(tick.ask)
                        hit_tp1 = price >= tp1 if direction=="BUY" else price <= tp1
                        if hit_tp1:
                            if round(float(order.get('SL',0)), digits) != round(entry, digits):
                                update_order_sl_in_sheet(row_idx, format_price(entry, digits))
                                send_telegram_message(f"🔒 Move SL → BE @ {symbol} ({format_price(entry, digits)})")

            time.sleep(1)
        except Exception as e:
            print("❌ TP/SL CHECKER ERROR:", e)
            traceback.print_exc()
            time.sleep(10)

# === SIGNAL GENERATOR ===
def check_symbol(symbol):
    print(f"\n[DEBUG] check_symbol called: {symbol} at {datetime.now()}")

    # Logic 1: BLOCK NEW WHEN RUNNING
    if BLOCK_NEW_WHEN_RUNNING_GLOBAL and has_any_running_order():
        print(f"   - {symbol}: GLOBAL LOCK active (some order running), skip")
        return
    if BLOCK_NEW_WHEN_RUNNING_PER_SYMBOL and has_running_order_for_symbol(symbol):
        print(f"   - {symbol}: PER-SYMBOL LOCK active (order running), skip")
        return

    # Logic 2: MARKET GUARD
    if not is_market_open(symbol):
        print(f"   - {symbol}: market closed/idle (guard) -> skip")
        return

    # Only proceed on real new bar (M15)
    if not has_new_bar(symbol, mt5.TIMEFRAME_M15):
        print(f"   - {symbol}: no new M15 bar -> skip")
        return

    # Logic 3A: SPREAD GUARD
    tick = get_tick(symbol)
    if not tick:
        print(f"   - {symbol}: No price tick")
        return
    if not spread_ok(symbol, tick):
        print(f"   - {symbol}: spread too wide -> skip")
        return

    # Logic 3B: SESSION GUARD (for US indices)
    if symbol in INDEX_SYMBOLS and not in_session_local(symbol, datetime.now()):
        print(f"   - {symbol}: out-of-session -> skip")
        return

    candles = get_candles(symbol, mt5.TIMEFRAME_M15, 100)
    if len(candles) < 60:
        print(f"   - {symbol}: Not enough data")
        return

    closes = [c['close'] for c in candles]
    trend = "none"
    if is_uptrend(closes):
        trend = "up"
    elif is_downtrend(closes):
        trend = "down"
    else:
        print(f"   - {symbol}: No clear trend")
        return

    pattern, direction = None, None
    eng = detect_engulfing(candles)
    pin = is_pinbar(candles)
    if eng == "Bullish Engulfing" and trend == "up":
        direction, pattern = "Buy", "Bullish Engulfing"
    elif eng == "Bearish Engulfing" and trend == "down":
        direction, pattern = "Sell", "Bearish Engulfing"
    elif pin == "Pinbar Bottom" and trend == "up":
        direction, pattern = "Buy", "Pinbar Bottom"
    elif pin == "Pinbar Top" and trend == "down":
        direction, pattern = "Sell", "Pinbar Top"
    elif is_double_top(candles) and trend == "down":
        direction, pattern = "Sell", "Double Top"
    elif is_double_bottom(candles) and trend == "up":
        direction, pattern = "Buy", "Double Bottom"
    elif is_morning_star(candles) and trend == "up":
        direction, pattern = "Buy", "Morning Star"
    elif is_evening_star(candles) and trend == "down":
        direction, pattern = "Sell", "Evening Star"
    elif detect_qm(candles) == "QM Buy" and trend == "up":
        direction, pattern = "Buy", "Quasimodo Buy"
    elif detect_qm(candles) == "QM Sell" and trend == "down":
        direction, pattern = "Sell", "Quasimodo Sell"
    elif detect_imbalance(candles) and trend == "up":
        direction, pattern = "Buy", "Imbalance Up"
    elif detect_imbalance(candles) and trend == "down":
        direction, pattern = "Sell", "Imbalance Down"
    elif detect_demand_zone(candles) and trend == "up":
        direction, pattern = "Buy", "Demand Zone"
    elif detect_supply_zone(candles) and trend == "down":
        direction, pattern = "Sell", "Supply Zone"

    if direction is None:
        print(f"   - {symbol}: No entry setup (pattern/trend not matched)")
        return

    entry = float(tick.ask) if direction == "Buy" else float(tick.bid)

    zones = find_zone_levels(candles, entry, direction)
    if not zones or len(zones) == 0:
        print(f"   - {symbol}: No valid zone, reject order")
        return

    try:
        sl, [tp1, tp2, tp3] = calculate_sl_tp(symbol, entry, candles, direction)
    except Exception as ex:
        print(f"   - {symbol}: SL/TP error: {ex}")
        return

    digits = symbol_digits.get(symbol, 2)
    # extra validation layer
    min_gap_map2 = {
        "EURUSD.A": 0.0012, "GBPUSD.A": 0.0020, "AUDUSD.A": 0.0012, "NZDUSD.A": 0.0012,
        "EURGBP.A": 0.0010, "USDCAD.A": 0.0015, "USDJPY.A": 0.10, "XAUUSD.A": 0.5,
        "NAS100.A": 20, "US30.A": 50, "BTCUSD.A": 50
    }
    min_gap2 = min_gap_map2.get(symbol, 0.0002)
    for v in [sl, tp1, tp2, tp3]:
        if v is None or v == 0 or abs(entry - v) < min_gap2 or v == entry:
            print(f"   - {symbol}: SL/TP invalid! sl={sl}, tp1={tp1}, tp2={tp2}, tp3={tp3}, entry={entry}")
            return

    if not check_symbol_for_new_signal(symbol):
        print(f"   - {symbol}: Duplicate signal in last 30 mins, skip")
        return

    dt_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row = [dt_str, symbol, direction, entry, sl, tp1, tp2, tp3, "Pending", pattern, "", ""]
    print(f"   - {symbol}: [DEBUG] appending row: {row}")
    append_row_with_retry(row)
    print(f"   - {symbol}: [DEBUG] appended row and preparing telegram...")

    new_order = {
        "Date": dt_str, "Symbol": symbol, "Direction": direction, "Entry": entry, "SL": sl,
        "TP1": tp1, "TP2": tp2, "TP3": tp3, "Result": "Pending", "Pattern": pattern, "Note": "",
    }
    msg = build_entry_signal_message(new_order)

    # 1) Capture -> 2) Send photo FIRST -> 3) Send text as a REPLY to photo
    try:
        chart_path = capture_chart(symbol, entry, sl, tp1, tp2, tp3, bars=100)
        root_msg_id = None
        if chart_path:
            cap = f"{symbol} M15 — Entry/SL/TP\n#BTP #Signal"
            root_msg_id = send_telegram_photo(chart_path, caption=cap, parse_mode=None)
            if root_msg_id:
                LAST_SIGNAL_MSG_ID[symbol] = root_msg_id
        if root_msg_id:
            send_telegram_message(msg, reply_to_message_id=root_msg_id)
        else:
            send_telegram_message(msg)
    except Exception as e:
        log(f"Chart capture/send error: {e}", "warning")

# === MAIN ===
if __name__ == "__main__":
    print("🚀 Auto Signal + TP/SL Tracker + Expire (Real-time) พร้อมใช้งาน!")

    # Ensure all symbols are visible in MT5 Market Watch
    mt5_select_symbols(SYMBOLS)
    print("✅ MT5 symbols are selected (Market Watch)")

    # Start TP/SL/Expired checker thread
    threading.Thread(target=tp_sl_checker_loop, daemon=True).start()

    last_report_date = None
    last_week_report = None  # stores monday-of-week string

    
while True:
        try:
            # รอให้แท่ง M15 ปิดจริง ก่อนค่อยประมวลผล (กันสัญญาณหลอก)
            print("⌛ [2] Waiting for M15 candle close before checking signals...")
            wait_for_m15_close()

            # วนตรวจทุกสัญลักษณ์ (ผ่าน Guard ทั้งหมดใน check_symbol)
            for symbol in SYMBOLS:
                check_symbol(symbol)

            # === Schedulers: Daily at 23:00 and Weekly (Mon) at 08:00 ===
            now = datetime.now()
            # Daily 23:00 (fire once per day, allow 0-4 min window)
            today = now.strftime("%Y-%m-%d")
            if (now.hour == 23) and (0 <= now.minute < 5):
                if last_report_date != today:
                    summarize_results_daily()
                    last_report_date = today
                    log(f"[Scheduler] Daily summary sent for {today}", "info")

            # Weekly Monday 08:00 (fire once per Monday, allow 0-4 min window)
            monday_of_week = (now - timedelta(days=now.weekday())).strftime("%Y-%m-%d")
            if (now.weekday() == 0) and (now.hour == 8) and (0 <= now.minute < 5):
                if last_week_report != monday_of_week:
                    summarize_results_weekly()
                    last_week_report = monday_of_week
                    log(f"[Scheduler] Weekly summary sent for week starting {monday_of_week}", "info")

            time.sleep(5)

        except Exception as e:
            print("❌ MAIN LOOP ERROR:", e)
            traceback.print_exc()
            time.sleep(30)
