#!/usr/bin/env python3
"""
Lorentzian Classification Scanner — Fixed
==========================================
Fixes aplicados:
 1. normalize_features + momentum  → expanding() causal (ya era correcto; reforzado).
 2. Cache por símbolo              → sólo recalcula si llega nueva vela.
 3. FLIP re-entry                  → re-abre posición en el mismo bar inmediatamente.
 4. Funciones faltantes            → scan_once, main, calculate_metrics,
                                      summarize_trades, format_last_trades.
 5. Vectorised Lorentzian distance → np.sum(np.log1p(|diff|), axis=1) en batch.
"""

import sys
import time
import signal
import threading
from datetime import datetime
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional

import requests
import numpy as np
import pandas as pd

# ─── Configuración ────────────────────────────────────────────────────────────
INTERVAL               = "15m"
KLINES_LIMIT           = 600
TOP_N                  = 20
MAX_WORKERS            = 5
TP_ATR_MULT            = 1.8
SL_ATR_MULT            = 1.2
NEIGHBORS              = 6
MAX_BARS_BACK          = 200
FEATURE_COUNT          = 5          # usa las 5 features, incluido momentum
RISK_PRINT_LAST_TRADES = 5
SLEEP_BETWEEN_CYCLES   = 15         # segundos
TRADE_NOTIONAL         = 100        # USD simulados por trade
INITIAL_BALANCE        = 100
FEE_RATE               = 0.0006     # 0.06 % por lado (taker)
INTRABAR_EXIT_POLICY   = "OPEN_DISTANCE"  # OPEN_DISTANCE | CONSERVATIVE

BASE = "https://fapi.binance.com"
session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0"})

RUNNING = True

# Cache {symbol: {last_time, result}}
# Evita recalcular KNN completo y evita redescargar 600 velas si no cambia la última vela
_symbol_cache: Dict[str, dict] = {}
_cache_lock = threading.Lock()

# ─── Signal handler ───────────────────────────────────────────────────────────
def _handle_sigint(signum, frame):
    global RUNNING
    RUNNING = False
    print("\nSaliendo limpiamente…")

signal.signal(signal.SIGINT, _handle_sigint)

# ─── Logger a fichero ─────────────────────────────────────────────────────────
class SessionLogger:
    def __init__(self, filename: str):
        self.terminal = sys.stdout
        self.log = open(filename, "a", encoding="utf-8")

    def write(self, msg: str):
        self.terminal.write(msg)
        self.log.write(msg)
        self.log.flush()

    def flush(self):
        self.terminal.flush()
        self.log.flush()

# ─── Dataclass ────────────────────────────────────────────────────────────────
@dataclass
class Trade:
    symbol:        str
    side:          str
    entry_time:    str
    exit_time:     str
    entry_price:   float
    exit_price:    float
    result:        str      # TP | SL | FLIP | OPEN
    pnl_pct:       float
    duration_bars: int

# ─── HTTP ─────────────────────────────────────────────────────────────────────
def http_get(path: str, params: Optional[dict] = None, timeout: int = 15):
    r = session.get(BASE + path, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

def fetch_symbol_universe() -> List[str]:
    try:
        info = http_get("/fapi/v1/exchangeInfo")
        return [
            s["symbol"] for s in info["symbols"]
            if s.get("status") == "TRADING"
            and s.get("contractType") == "PERPETUAL"
            and s.get("quoteAsset") == "USDT"
            and s.get("symbol", "").endswith("USDT")
        ]
    except Exception as e:
        print(f"[ERROR] exchangeInfo: {e}")
        return []

def fetch_top_by_volume(limit: int = TOP_N) -> List[str]:
    try:
        tickers = http_get("/fapi/v1/ticker/24hr")
        allowed = set(fetch_symbol_universe())
        rows = [
            (t["symbol"], float(t.get("quoteVolume", 0)))
            for t in tickers if t["symbol"] in allowed
        ]
        rows.sort(key=lambda x: x[1], reverse=True)
        return [s for s, _ in rows[:limit]]
    except Exception as e:
        print(f"[ERROR] tickers: {e}")
        return []

def fetch_klines(symbol: str, interval: str = INTERVAL,
                 limit: int = KLINES_LIMIT) -> pd.DataFrame:
    try:
        data = http_get(
            "/fapi/v1/klines",
            params={"symbol": symbol, "interval": interval, "limit": limit},
        )
        if not data:
            return pd.DataFrame()
        cols = [
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "trades",
            "taker_buy_base", "taker_buy_quote", "ignore",
        ]
        df = pd.DataFrame(data, columns=cols)
        for c in ["open", "high", "low", "close", "volume", "quote_asset_volume"]:
            df[c] = df[c].astype(float)
        df["open_time"]  = pd.to_datetime(df["open_time"],  unit="ms", utc=True)
        df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
        # Eliminar vela incompleta
        now = pd.Timestamp.now(tz="UTC")
        if len(df) and df.iloc[-1]["close_time"] > now:
            df = df.iloc[:-1].copy()
        return df.reset_index(drop=True)
    except Exception:
        return pd.DataFrame()


def fetch_latest_completed_open_time(symbol: str, interval: str = INTERVAL) -> Optional[str]:
    """
    Consulta mínima para saber si llegó una vela cerrada nueva.
    Evita depender del orden de Binance: toma la mayor open_time entre las
    velas que ya tienen close_time vencido. Los errores HTTP/red se propagan
    para no ocultar desconexiones usando datos obsoletos del caché.
    """
    data = http_get(
        "/fapi/v1/klines",
        params={"symbol": symbol, "interval": interval, "limit": 2},
    )
    now = pd.Timestamp.now(tz="UTC")
    completed = [
        pd.to_datetime(row[0], unit="ms", utc=True)
        for row in data or []
        if pd.to_datetime(row[6], unit="ms", utc=True) <= now
    ]
    if not completed:
        return None
    return str(max(completed))
=======
    Evita descargar KLINES_LIMIT velas cuando el caché sigue vigente.
    """
    try:
        data = http_get(
            "/fapi/v1/klines",
            params={"symbol": symbol, "interval": interval, "limit": 2},
        )
        now = pd.Timestamp.now(tz="UTC")
        for row in reversed(data or []):
            close_time = pd.to_datetime(row[6], unit="ms", utc=True)
            if close_time <= now:
                return str(pd.to_datetime(row[0], unit="ms", utc=True))
        return None
    except Exception:
        return None
 main

# ─── Indicadores ─────────────────────────────────────────────────────────────
def rsi(series: pd.Series, length: int = 14) -> pd.Series:
    d        = series.diff()
    avg_gain = d.clip(lower=0).ewm(alpha=1/length, adjust=False).mean()
    avg_loss = (-d.clip(upper=0)).ewm(alpha=1/length, adjust=False).mean()
    rs       = avg_gain / avg_loss.replace(0, np.nan)
    return (100 - 100 / (1 + rs)).fillna(50.0)

def cci(df: pd.DataFrame, length: int = 20) -> pd.Series:
    tp  = (df["high"] + df["low"] + df["close"]) / 3.0
    sma = tp.rolling(length).mean()
    mad = tp.rolling(length).apply(lambda x: np.mean(np.abs(x - x.mean())), raw=True)
    return ((tp - sma) / (0.015 * mad.replace(0, np.nan))).fillna(0.0)

def adx(df: pd.DataFrame, length: int = 14) -> pd.Series:
    hi, lo, cl = df["high"], df["low"], df["close"]
    plus_dm  = hi.diff().where((hi.diff() > -lo.diff()) & (hi.diff() > 0), 0.0)
    minus_dm = (-lo.diff()).where((-lo.diff() > hi.diff()) & (-lo.diff() > 0), 0.0)
    tr       = pd.concat(
        [(hi - lo), (hi - cl.shift()).abs(), (lo - cl.shift()).abs()], axis=1
    ).max(axis=1)
    a        = 1 / length
    atr_     = tr.ewm(alpha=a, adjust=False).mean()
    pdi      = 100 * plus_dm.ewm(alpha=a, adjust=False).mean()  / atr_.replace(0, np.nan)
    mdi      = 100 * minus_dm.ewm(alpha=a, adjust=False).mean() / atr_.replace(0, np.nan)
    dx       = 100 * (pdi - mdi).abs() / (pdi + mdi).replace(0, np.nan)
    return dx.ewm(alpha=a, adjust=False).mean().fillna(0.0)

def wave_trend(df: pd.DataFrame, n1: int = 10, n2: int = 21) -> pd.Series:
    ap  = (df["high"] + df["low"] + df["close"]) / 3.0
    esa = ap.ewm(span=n1, adjust=False).mean()
    d   = (ap - esa).abs().ewm(span=n1, adjust=False).mean()
    ci  = (ap - esa) / (0.015 * d.replace(0, np.nan))
    return ci.ewm(span=n2, adjust=False).mean().fillna(0.0)

def atr(df: pd.DataFrame, length: int = 14) -> pd.Series:
    tr = pd.concat(
        [(df["high"] - df["low"]),
         (df["high"] - df["close"].shift()).abs(),
         (df["low"]  - df["close"].shift()).abs()], axis=1
    ).max(axis=1)
    return tr.ewm(alpha=1/length, adjust=False).mean().bfill().fillna(0.0)

def ema(series: pd.Series, length: int = 200) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()

def sma(series: pd.Series, length: int = 200) -> pd.Series:
    return series.rolling(length).mean()

def kernel_rational_quadratic(series: pd.Series, h: int = 8, r: float = 8.0) -> pd.Series:
    from numpy.lib.stride_tricks import sliding_window_view
    arr     = series.to_numpy(dtype=float)
    padded  = np.pad(arr, (h - 1, 0), mode="edge")
    wins    = sliding_window_view(padded, h)
    rel     = np.arange(h, dtype=float)
    weights = (1.0 + rel**2 / (2.0 * r * h**2)) ** (-r)
    out     = np.dot(wins, weights[::-1]) / weights.sum()
    return pd.Series(out, index=series.index)

# ─── Features ────────────────────────────────────────────────────────────────
def build_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["rsi"]    = rsi(out["close"], 14)
    out["wt"]     = wave_trend(out, 10, 21)
    out["cci"]    = cci(out, 20)
    out["adx"]    = adx(out, 14)
    out["atr"]    = atr(out, 14)
    out["ema200"] = ema(out["close"], 200)
    out["sma200"] = sma(out["close"], 200)
    out["kernel"] = kernel_rational_quadratic(out["close"], h=8, r=8.0)
    return out

def normalize_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalización CAUSAL: expanding min/max.
    En el bar i sólo se conoce el rango [bar_0 … bar_i] → sin look-ahead.
    """
    out = df.copy()
    for c in ["rsi", "wt", "cci", "adx"]:
        s  = out[c]
        mn = s.expanding(min_periods=2).min()
        mx = s.expanding(min_periods=2).max()
        out[c] = ((s - mn) / (mx - mn).replace(0, np.nan) * 2 - 1).fillna(0.0)
    return out

# ─── KNN Lorentzian (vectorizado) ────────────────────────────────────────────
def compute_prediction(df: pd.DataFrame) -> pd.Series:
    x   = normalize_features(df)

    # Momentum causal: expanding min/max → SIN look-ahead
    mom    = x["close"].pct_change(4).fillna(0.0)
    mn_mom = mom.expanding(min_periods=2).min()
    mx_mom = mom.expanding(min_periods=2).max()
    x["mom"] = ((mom - mn_mom) / (mx_mom - mn_mom).replace(0, np.nan) * 2 - 1).fillna(0.0)

    feat_cols = ["rsi", "wt", "cci", "adx", "mom"][:FEATURE_COUNT]
    feats = x[feat_cols].to_numpy(dtype=float)
    close = x["close"].to_numpy(dtype=float)
    n     = len(x)
    pred  = np.zeros(n, dtype=float)

    if n < 250:
        return pd.Series(pred, index=df.index)

    # Etiquetas causales: el label de bar i usa close[i+4] (conocido en el pasado)
    y_train = np.zeros(n, dtype=np.int8)
    for i in range(n - 4):
        if   close[i + 4] > close[i]: y_train[i] =  1
        elif close[i + 4] < close[i]: y_train[i] = -1

    for cur in range(250, n):
        start      = max(0, cur - MAX_BARS_BACK)
        search_idx = np.arange(start, cur - 4)   # cur-4: el label más reciente es seguro
        if len(search_idx) < NEIGHBORS:
            continue

        # Distancia Lorentziana vectorizada
        diff  = feats[search_idx] - feats[cur]          # (m, FEATURE_COUNT)
        dists = np.sum(np.log1p(np.abs(diff)), axis=1)  # (m,)

        k         = min(len(dists), NEIGHBORS)
        near_pos  = np.argpartition(dists, k - 1)[:k]
        labels    = y_train[search_idx[near_pos]].astype(float)
        dist_near = dists[near_pos]
        weights   = (1.0 + dist_near**2 / 128.0) ** (-8.0)
        sum_w     = weights.sum()
        if sum_w > 0:
            pred[cur] = (labels * weights).sum() / sum_w

    return pd.Series(pred, index=df.index)

# ─── Señales de entrada ───────────────────────────────────────────────────────
def entry_signals(df: pd.DataFrame) -> pd.DataFrame:
    out = build_features(df)
    out["prediction"] = compute_prediction(out)

    out["vol_filter"] = out["atr"]   > out["atr"].rolling(50).median()
    out["adx_filter"] = out["adx"]   > 20
    out["ema_up"]     = out["close"] > out["ema200"]
    out["sma_up"]     = out["close"] > out["sma200"]
    out["kernel_up"]  = out["kernel"].diff() > 0

    out["raw_signal"] = np.where(
        out["prediction"] > 0.3,  1,
        np.where(out["prediction"] < -0.3, -1, 0),
    )

    signal_arr = np.zeros(len(out), dtype=np.int8)
    for i in range(len(out)):
        rs = int(out["raw_signal"].iat[i])
        vf = bool(out["vol_filter"].iat[i])
        af = bool(out["adx_filter"].iat[i])
        eu = bool(out["ema_up"].iat[i])
        su = bool(out["sma_up"].iat[i])
        ku = bool(out["kernel_up"].iat[i])
        if   rs > 0 and vf and af and eu      and su      and ku:
            signal_arr[i] =  1
        elif rs < 0 and vf and af and not eu  and not su  and not ku:
            signal_arr[i] = -1

    out["signal"] = signal_arr
    return out

# ─── Backtest TP/SL ──────────────────────────────────────────────────────────
def backtest_tp_sl(
    signal_df: pd.DataFrame,
    tp_mult:   float = TP_ATR_MULT,
    sl_mult:   float = SL_ATR_MULT,
) -> List[Trade]:
    """
    FIX: FLIP re-entry en el mismo bar.
    Cuando la señal invierte, cerramos y volvemos a abrir al mismo precio
    sin esperar al bar siguiente (que perdía la entrada).
    """
    out    = signal_df.reset_index(drop=True)
    trades: List[Trade] = []
    in_pos  = False
    side = entry_price = tp = sl = entry_idx = None

    def open_pos(i: int, direction: int):
        nonlocal in_pos, side, entry_price, tp, sl, entry_idx
        in_pos      = True
        side        = "LONG" if direction > 0 else "SHORT"
        entry_price = float(out["open"].iat[i])
        atr_v       = float(out["atr"].iat[i - 1])
        mult        = 1 if side == "LONG" else -1
        tp          = entry_price + atr_v * tp_mult * mult
        sl          = entry_price - atr_v * sl_mult * mult
        entry_idx   = i

    def resolve_intrabar_exit(bar_open: float, tp_price: float, sl_price: float):
        """Resuelve velas ambiguas donde TP y SL son tocados en el mismo bar."""
        if INTRABAR_EXIT_POLICY == "OPEN_DISTANCE":
            tp_dist = abs(tp_price - bar_open)
            sl_dist = abs(bar_open - sl_price)
            if tp_dist < sl_dist:
                return "TP", tp_price
            if sl_dist < tp_dist:
                return "SL", sl_price

        # Empate o política CONSERVATIVE: no asumir el camino favorable.
        return "SL", sl_price

    def close_pos(i: int, xprice: float, reason: str):
        nonlocal in_pos, side, entry_price, tp, sl, entry_idx
        pnl = (
            (xprice - entry_price) / entry_price * 100 if side == "LONG"
            else (entry_price - xprice) / entry_price * 100
        )
        pnl -= FEE_RATE * 2 * 100
        trades.append(Trade(
            symbol="", side=side,
            entry_time  = str(out["open_time"].iat[entry_idx]),
            exit_time   = str(out["close_time"].iat[i]),
            entry_price = round(entry_price, 8),
            exit_price  = round(xprice, 8),
            result      = reason,
            pnl_pct     = round(pnl, 4),
            duration_bars = i - entry_idx + 1,
        ))
        in_pos = False
        side = entry_price = tp = sl = entry_idx = None

    for i in range(1, len(out)):
        sig      = int(out["signal"].iat[i - 1])
        prev_sig = int(out["signal"].iat[i - 2]) if i >= 2 else 0

        # ── Entrada ──────────────────────────────────────────────────────────
        if not in_pos and sig != 0 and sig != prev_sig:
            open_pos(i, sig)

        # ── Salida ───────────────────────────────────────────────────────────
        if in_pos:
            high  = float(out["high"].iat[i])
            low   = float(out["low"].iat[i])
            is_flip = sig != 0 and (
                (side == "LONG"  and sig < 0) or
                (side == "SHORT" and sig > 0)
            )

            exit_reason = None
            exit_price  = None

            bar_open = float(out["open"].iat[i])
            if side == "LONG":
                hit_sl = low <= sl
                hit_tp = high >= tp
            else:
                hit_sl = high >= sl
                hit_tp = low <= tp

            if hit_sl and hit_tp:
                exit_reason, exit_price = resolve_intrabar_exit(bar_open, tp, sl)
            elif hit_sl:
                exit_reason, exit_price = "SL", sl
            elif hit_tp:
                exit_reason, exit_price = "TP", tp

            # FLIP sólo si no golpeó TP/SL primero
            if is_flip and exit_reason is None:
                exit_reason = "FLIP"
                exit_price  = float(out["open"].iat[i])

            if exit_reason:
                flip_dir = sig if exit_reason == "FLIP" else 0
                close_pos(i, exit_price, exit_reason)

                # Re-entry inmediata en FLIP (mismo bar, mismo precio)
                if flip_dir != 0:
                    open_pos(i, flip_dir)

    # ── Posición abierta al final ─────────────────────────────────────────────
    if in_pos and entry_idx is not None:
        last_close = float(out["close"].iat[-1])
        pnl = (
            (last_close - entry_price) / entry_price * 100 if side == "LONG"
            else (entry_price - last_close) / entry_price * 100
        )
        pnl -= FEE_RATE * 2 * 100
        trades.append(Trade(
            symbol="", side=side,
            entry_time    = str(out["open_time"].iat[entry_idx]),
            exit_time     = str(out["close_time"].iat[-1]),
            entry_price   = round(entry_price, 8),
            exit_price    = round(last_close, 8),
            result        = "OPEN",
            pnl_pct       = round(pnl, 4),
            duration_bars = len(out) - entry_idx,
        ))

    return trades

# ─── Métricas ────────────────────────────────────────────────────────────────
def calculate_metrics(trades: List[Trade]) -> dict:
    closed = [t for t in trades if t.result != "OPEN"]
    if not closed:
        return dict(win_rate=0.0, avg_pnl=0.0, total_pnl=0.0,
                    max_dd=0.0, profit_factor=0.0, n=0)

    wins   = [t for t in closed if t.pnl_pct > 0]
    losses = [t for t in closed if t.pnl_pct <= 0]
    gp     = sum(t.pnl_pct for t in wins)
    gl     = abs(sum(t.pnl_pct for t in losses))
    pf     = float("inf") if gl == 0 else gp / gl
=======
    pf     = gp / max(gl, 1e-9)


    eq   = float(INITIAL_BALANCE)
    peak = eq
    mdd  = 0.0
    for t in closed:
        eq  *= 1 + t.pnl_pct / 100
        peak = max(peak, eq)
        mdd  = max(mdd, (peak - eq) / peak * 100)

    return dict(
        win_rate      = round(len(wins) / len(closed) * 100, 1),
        avg_pnl       = round(sum(t.pnl_pct for t in closed) / len(closed), 4),
        total_pnl     = round(sum(t.pnl_pct for t in closed), 4),
        max_dd        = round(mdd, 2),
        profit_factor = round(pf, 2) if np.isfinite(pf) else pf,
        n             = len(closed),
    )

def summarize_trades(trades: List[Trade], last_n: int = RISK_PRINT_LAST_TRADES) -> dict:
    metrics  = calculate_metrics(trades)
    closed   = [t for t in trades if t.result != "OPEN"]
    last     = closed[-last_n:]
    # Estimación simple: notional fijo por trade, sin balance compuesto.
    pnl_usd_fixed = sum(t.pnl_pct / 100 * TRADE_NOTIONAL for t in last)
    return dict(metrics=metrics, trades=last, pnl_usd_fixed=round(pnl_usd_fixed, 2))

def format_profit_factor(value: float) -> str:
    return "INF" if np.isinf(value) else f"{value:.2f}"

def format_last_trades(trades: List[Trade]) -> str:
    if not trades:
        return "  Sin trades cerrados"
    return "\n".join(
        f"  {'✅' if t.pnl_pct > 0 else '❌'}  {t.side:<5}  {t.result:<4}  "
        f"{t.pnl_pct:+.2f}%  {t.duration_bars}b"
        for t in trades
    )

# ─── Procesamiento por símbolo (con caché) ───────────────────────────────────
def process_symbol(symbol: str) -> Optional[dict]:
    """
    Cachea el resultado por símbolo.
    En cada ciclo sólo consulta una muestra mínima para verificar si llegó una
    vela cerrada nueva; si no cambió, reutiliza el resultado anterior sin
    descargar 600 velas ni recalcular KNN.
    """
    global _symbol_cache
    try:

        with _cache_lock:
            cache = _symbol_cache.get(symbol)
        if cache:
            last_time = fetch_latest_completed_open_time(symbol)
            if last_time is None:
                return dict(symbol=symbol, error="no closed kline in latest poll; cache not reused")
=======
        cache = _symbol_cache.get(symbol)
        if cache:
            last_time = fetch_latest_completed_open_time(symbol)
            if last_time is None:
                return cache["result"]
 main
            if cache.get("last_time") == last_time:
                return cache["result"]

        df = fetch_klines(symbol)
        if df.empty or len(df) < 250:
            return None

        last_time = str(df["open_time"].iloc[-1])
        signal_df = entry_signals(df)
        trades    = backtest_tp_sl(signal_df)
        for t in trades:
            t.symbol = symbol

        last    = signal_df.iloc[-1]
        summary = summarize_trades(trades, RISK_PRINT_LAST_TRADES)
        sig_str = (
            "LONG"  if int(last["signal"]) > 0 else
            "SHORT" if int(last["signal"]) < 0 else
            "NEUTRAL"
        )
        atr_pct = float(last["atr"] / last["close"] * 100) if float(last["close"]) else 0.0

        result = dict(
            symbol       = symbol,
            signal       = sig_str,
            price        = float(last["close"]),
            atr_pct      = atr_pct,
            metrics      = summary["metrics"],
            pnl_usd_5    = summary["pnl_usd_fixed"],
            last_5       = summary["trades"],
            total_trades = summary["metrics"]["n"],
            last_5_text  = format_last_trades(summary["trades"]),
        )

        with _cache_lock:
            _symbol_cache[symbol] = dict(last_time=last_time, result=result)
=======
        _symbol_cache[symbol] = dict(last_time=last_time, result=result)
 main
        return result
    except Exception as e:
        return dict(symbol=symbol, error=str(e))


# ─── Ciclo de escaneo ─────────────────────────────────────────────────────────
def scan_once(symbols: List[str]) -> List[dict]:
    results = []
    unique_symbols = list(dict.fromkeys(symbols))
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_symbol, sym): sym for sym in unique_symbols}
        for future in as_completed(futures):
            r = future.result()
            if r is not None:
                results.append(r)
    return results

# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    log_file = f"scanner_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    sys.stdout = SessionLogger(log_file)

    print(f"Scanner iniciado  │  log → {log_file}")
    print(f"Intervalo: {INTERVAL}  │  Top {TOP_N}  │  Sleep {SLEEP_BETWEEN_CYCLES}s\n")

    cycle = 0
    while RUNNING:
        cycle += 1
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n{'═'*64}")
        print(f"  CICLO #{cycle}  │  {ts}")
        print(f"{'═'*64}")

        symbols = fetch_top_by_volume(TOP_N)
        if not symbols:
            print("No se pudieron obtener símbolos. Reintentando en 30 s…")
            time.sleep(30)
            continue

        print(f"Escaneando {len(symbols)} símbolos: {', '.join(symbols[:6])}…")
        t0      = time.time()
        results = scan_once(symbols)
        elapsed = time.time() - t0

        signals = [r for r in results if "error" not in r and r["signal"] != "NEUTRAL"]
        errors  = [r for r in results if "error" in r]
        with _cache_lock:
            cached = sum(1 for s in symbols if _symbol_cache.get(s, {}).get("last_time"))

        print(
            f"Listo en {elapsed:.1f}s │ {len(results)} proc │ "
            f"{len(signals)} señales │ {cached} en caché │ {len(errors)} errores\n"
        )

        if signals:
            signals.sort(key=lambda x: x["metrics"].get("win_rate", 0), reverse=True)
            sep = "─" * 66
            print(sep)
            print(f"{'SYM':<12} {'SIG':<7} {'PRECIO':<13} {'ATR%':<7} {'WR%':<7} {'PF':<6} {'#T':<5} {'MDD%'}")
            print(sep)
            for r in signals:
                m = r["metrics"]
                print(
                    f"{r['symbol']:<12} {r['signal']:<7} {r['price']:<13.4f} "
                    f"{r['atr_pct']:<7.2f} {m['win_rate']:<7.1f} "
                    f"{format_profit_factor(m['profit_factor']):<6} {r['total_trades']:<5} {m['max_dd']:.2f}"
                )

            print("\nÚltimos trades (top 5 por WR):")
            for r in signals[:5]:
                print(f"\n  {r['symbol']} [{r['signal']}]  pnl_5_fixed={r['pnl_usd_5']:+.2f} USD")
                print(r["last_5_text"])
        else:
            print("Sin señales activas en este ciclo.")

        if errors:
            errs = errors[:5]
            err_strs = [f"{e['symbol']}: {e['error'][:40]}" for e in errs]
            print(f"\nErrores: {err_strs}")

        if RUNNING:
            print(f"\nPróximo ciclo en {SLEEP_BETWEEN_CYCLES} s…")
            time.sleep(SLEEP_BETWEEN_CYCLES)


if __name__ == "__main__":
    main()
