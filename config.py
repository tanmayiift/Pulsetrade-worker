import os

SYMBOLS = os.getenv("SYMBOLS", "RELIANCE.NS,TCS.NS,INFY.NS").split(",")
INTERVAL_SECONDS = int(os.getenv("INTERVAL_SECONDS", "5"))
CANDLE_SECONDS = int(os.getenv("CANDLE_SECONDS", "60"))

UPSTOX_WS_URL = os.getenv("UPSTOX_WS_URL", "").strip()
UPSTOX_WS_TOKEN = os.getenv("UPSTOX_WS_TOKEN", "").strip()
UPSTOX_WS_MODE = os.getenv("UPSTOX_WS_MODE", "ltp").strip()
UPSTOX_INSTRUMENT_KEYS = os.getenv("UPSTOX_INSTRUMENT_KEYS", "").strip()
UPSTOX_INSTRUMENTS_CSV_URL = os.getenv("UPSTOX_INSTRUMENTS_CSV_URL", "https://api.upstox.com/v2/instruments")
UPSTOX_MAP_BY = os.getenv("UPSTOX_MAP_BY", "tradingsymbol").strip()

YF_POLL_SECONDS = int(os.getenv("YF_POLL_SECONDS", "5"))
VERCEL_SIGNALS_PUSH_URL = os.getenv("VERCEL_SIGNALS_PUSH_URL", "https://pulsetrade.vercel.app/api/signals/push")
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
WORKER_ID = os.getenv("WORKER_ID", "render-worker-1")


