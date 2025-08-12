import pandas as pd
import pandas_ta as ta

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["EMA_9"] = ta.ema(out["close"], length=9)
    out["EMA_21"] = ta.ema(out["close"], length=21)
    out["RSI_14"] = ta.rsi(out["close"], length=14)
    macd = ta.macd(out["close"], fast=12, slow=26, signal=9)
    out["MACD"] = macd["MACD_12_26_9"]
    out["MACD_SIGNAL"] = macd["MACDs_12_26_9"]
    out["MACD_HIST"] = macd["MACDh_12_26_9"]
    bb = ta.bbands(out["close"], length=20, std=2)
    out["BB_MID"] = bb["BBM_20_2.0"]
    out["BB_UPPER"] = bb["BBU_20_2.0"]
    out["BB_LOWER"] = bb["BBL_20_2.0"]
    out["ATR_14"] = ta.atr(out["high"], out["low"], out["close"], length=14)
    out["VWAP"] = ta.vwap(out["high"], out["low"], out["close"], out["volume"])
    return out

def generate_signal(latest: pd.Series, sentiment_score: float) -> str:
    rsi = latest.get("RSI_14", 50.0)
    macd = latest.get("MACD", 0.0)
    macds = latest.get("MACD_SIGNAL", 0.0)
    if rsi < 35 and macd > macds and sentiment_score >= 0:
        return "BUY"
    if rsi > 65 and macd < macds and sentiment_score <= 0:
        return "SELL"
    return "HOLD"


