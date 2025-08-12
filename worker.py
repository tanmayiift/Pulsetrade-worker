import os, time, json, threading, traceback, io
import requests, pandas as pd, websocket, yfinance as yf
from datetime import datetime
from psycopg2.extras import execute_values
import psycopg2

from config import *
from indicators import compute_indicators, generate_signal
from sentiment import aggregate_headlines


conn = None
def db_connect():
    global conn
    if SUPABASE_DB_URL and conn is None:
        conn = psycopg2.connect(SUPABASE_DB_URL)
        conn.autocommit = True

def db_exec(sql, params=None):
    if not SUPABASE_DB_URL:
        return
    db_connect()
    with conn.cursor() as cur:
        cur.execute(sql, params or ())


def now_ms():
    return int(time.time()*1000)

def floor_ms(ts_ms, sec):
    return (ts_ms//(sec*1000))*(sec*1000)


frames: dict[str, pd.DataFrame] = {}

def ensure_frame(key: str):
    if key not in frames:
        frames[key] = pd.DataFrame(columns=["open","high","low","close","volume"])

def add_tick(key: str, price: float, volume: int = 0):
    ensure_frame(key)
    df = frames[key]
    ts_ms = now_ms(); ts = pd.to_datetime(floor_ms(ts_ms, CANDLE_SECONDS), unit="ms", utc=True)
    if ts in df.index:
        row = df.loc[ts]
        row["high"] = max(row["high"], price)
        row["low"] = min(row["low"], price)
        row["close"] = price
        row["volume"] += volume
        df.loc[ts] = row
    else:
        open_ = df["close"].iloc[-1] if not df.empty else price
        df.loc[ts] = {"open": open_, "high": price, "low": price, "close": price, "volume": volume}
    df.sort_index(inplace=True)


def push_signal(key: str, decision: str, entry: float, sl: float|None = None, tp: float|None = None,
                score: float = 0.0, confidence: float = 0.0, qty: int = 1):
    # DB write
    if SUPABASE_DB_URL:
        db_exec(
            """
            INSERT INTO signals(symbol, decision, score, model_confidence, qty, entry_price, stop_loss, take_profit, rationale)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (key, decision, score, confidence, qty, float(entry or 0), float(sl or 0), float(tp or 0), "worker")
        )
    # Notify app via Vercel
    try:
        requests.post(VERCEL_SIGNALS_PUSH_URL, json={"signal": {
            "symbol": key.split("|")[-1].replace(".NS",""),
            "decision": decision,
            "score": score,
            "modelConfidence": confidence,
            "qty": qty,
            "entry_price": float(entry or 0),
            "stop_loss": float(sl or 0),
            "take_profit": float(tp or 0),
            "rationale": f"TA+VADER {decision}"
        }}, timeout=10)
    except Exception:
        pass


def get_sentiment(symbol_plain: str) -> float:
    try:
        r = requests.get(f"https://query2.finance.yahoo.com/v1/finance/search?q={symbol_plain}&quotesCount=0&newsCount=10", timeout=10)
        data = r.json(); headlines = [n.get("title","") for n in (data.get("news") or [])]
        return aggregate_headlines(headlines)
    except Exception:
        return 0.0


def fetch_instrument_master() -> pd.DataFrame:
    headers = {}
    if UPSTOX_WS_TOKEN:
        headers["Authorization"] = f"Bearer {UPSTOX_WS_TOKEN}"
    r = requests.get(UPSTOX_INSTRUMENTS_CSV_URL, headers=headers, timeout=30)
    r.raise_for_status()
    return pd.read_csv(io.StringIO(r.text))

def map_to_instrument_keys(df: pd.DataFrame, symbols: list[str]) -> list[str]:
    if UPSTOX_MAP_BY == "isin":
        sub = df[df["isin"].isin(symbols)]
        return list(dict.fromkeys(sub["instrument_key"].astype(str).tolist()))
    wants = [s.replace(".NS","" ).upper() for s in symbols]
    sub = df[df["tradingsymbol"].astype(str).str.upper().isin(wants)]
    return list(dict.fromkeys(sub["instrument_key"].astype(str).tolist()))


def ws_thread():
    if not UPSTOX_WS_URL or not UPSTOX_WS_TOKEN:
        return
    keys_env = [k.strip() for k in (UPSTOX_INSTRUMENT_KEYS or "").split(",") if k.strip()]
    if not keys_env:
        try:
            df = fetch_instrument_master()
            keys_env = map_to_instrument_keys(df, SYMBOLS)
        except Exception as e:
            print("Instrument master mapping failed:", e); return
    for k in keys_env: ensure_frame(k)

    def on_open(ws):
        sub = {"guid":"pulsetrade-sub","method":"sub","data":{"mode":UPSTOX_WS_MODE,"instrumentKeys":keys_env}}
        ws.send(json.dumps(sub))
        print("WS subscribed:", keys_env)

    def on_message(ws, message):
        try:
            msg = json.loads(message)
            data = msg.get("data") or {}
            ikey = data.get("instrumentKey")
            price = data.get("lastPrice") or data.get("ltp") or data.get("close")
            if ikey and price is not None:
                add_tick(ikey, float(price), 0)
        except Exception:
            traceback.print_exc()

    def on_error(ws, err): print("WS error:", err)
    def on_close(ws, code, msg): print("WS closed", code, msg)

    headers = [f"Authorization: Bearer {UPSTOX_WS_TOKEN}"]
    ws = websocket.WebSocketApp(UPSTOX_WS_URL, on_open=on_open, on_message=on_message,
                                on_error=on_error, on_close=on_close, header=headers)
    ws.run_forever(ping_interval=25, ping_timeout=10)


def yf_poll_thread():
    while True:
        try:
            for s in SYMBOLS:
                data = yf.download(s, period="1d", interval="1m", progress=False)
                if not data.empty:
                    last = data.iloc[-1]
                    add_tick(s, float(last["Close"]), int(last.get("Volume",0)))
        except Exception:
            traceback.print_exc()
        time.sleep(YF_POLL_SECONDS)


def main_loop():
    # init fallback frames
    for s in SYMBOLS: ensure_frame(s)

    if UPSTOX_WS_URL and UPSTOX_WS_TOKEN:
        threading.Thread(target=ws_thread, daemon=True).start()
    else:
        threading.Thread(target=yf_poll_thread, daemon=True).start()

    # optional table create
    try:
        if SUPABASE_DB_URL:
            db_exec("""create table if not exists worker_health(
              id bigserial primary key, worker_id text, ts timestamptz default now(),
              ws_status text, model_local boolean, backend_ok boolean, latency_ms integer)""")
            db_exec("""create table if not exists signals(
              id bigserial primary key, ts timestamptz default now(), symbol text,
              decision text, score double precision, model_confidence double precision,
              qty integer, entry_price double precision, stop_loss double precision,
              take_profit double precision, rationale text)""")
    except Exception:
        pass

    while True:
        try:
            for key, df in list(frames.items()):
                if len(df) < 25: continue
                ind = compute_indicators(df); latest = ind.iloc[-1]
                plain = key.split("|")[-1].replace(".NS","")
                sent = get_sentiment(plain)
                decision = generate_signal(latest, sent)
                entry = float(latest["close"])
                atr = float(latest.get("ATR_14", 0.0))
                sl = float(entry - (1.5*atr) if atr>0 else entry*0.98)
                tp = float(entry + (2.0*atr) if atr>0 else entry*1.02)
                push_signal(key, decision, entry, sl, tp, score=float(sent), confidence=0.0, qty=1)
            time.sleep(INTERVAL_SECONDS)
        except Exception:
            traceback.print_exc(); time.sleep(2)


if __name__ == "__main__":
    print("PulseTrade worker starting...")
    main_loop()


