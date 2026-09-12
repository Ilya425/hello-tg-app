from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
import datetime
from typing import Any

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Hello TG App — MOEX Stage 1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)

MOEX_ISS = "https://iss.moex.com/iss"
BOARD = "TQBR"

TICKERS = [
    "LKOH", "SBER", "GAZP", "YDEX", "T", "TATN", "NVTK",
    "GMKN", "PLZL", "VTBR", "X5", "ROSN", "SBERP", "OZON",
    "SNGS", "SNGSP", "MOEX", "CHMF", "MTSS", "ALRS", "NLMK",
]

session = requests.Session()
session.headers.update({
    "User-Agent": "HelloTGApp-MOEX/1.2",
    "Accept": "application/json",
})

import time
CACHE_TTL = 60.0
market_cache = {"ts": 0.0, "value": None}
spark_cache: dict[str, tuple[float, list[float]]] = {}


def fetch_json(url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    """
    Read the raw response bytes and decode explicitly.
    This avoids the mojibake seen in the browser for Cyrillic MOEX names.
    """
    response = session.get(url, params=params, timeout=15)
    response.raise_for_status()

    raw = response.content
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = raw.decode("cp1251")

    return json.loads(text)


def rows(payload: dict[str, Any], name: str) -> list[dict[str, Any]]:
    block = payload.get(name) or {}
    columns = block.get("columns") or []
    data = block.get("data") or []
    return [dict(zip(columns, item)) for item in data]


def get_market_quotes() -> list[dict[str, Any]]:
    url = f"{MOEX_ISS}/engines/stock/markets/shares/boards/{BOARD}/securities.json"
    payload = fetch_json(url, {
        "iss.meta": "off",
        "lang": "ru",
        "iss.only": "marketdata,securities",
        "marketdata.columns": (
            "SECID,LAST,PREVPRICE,LASTCHANGE,LASTCHANGEPRCNT,UPDATETIME"
        ),
        "securities.columns": "SECID,SHORTNAME",
    })

    market = {r.get("SECID"): r for r in rows(payload, "marketdata")}
    names = {r.get("SECID"): r for r in rows(payload, "securities")}

    result = []
    for ticker in TICKERS:
        m = market.get(ticker, {})
        price = m.get("LAST")
        prev = m.get("PREVPRICE")
        pct = m.get("LASTCHANGEPRCNT")
        absolute = m.get("LASTCHANGE")

        if pct is None and price is not None and prev not in (None, 0):
            pct = (float(price) / float(prev) - 1) * 100

        result.append({
            "ticker": ticker,
            "name": names.get(ticker, {}).get("SHORTNAME"),
            "price": price,
            "change_abs": absolute,
            "change_pct": pct,
            "update_time": m.get("UPDATETIME"),
        })

    return result


def get_imoex() -> dict[str, Any]:
    """
    Current IMOEX value and previous value.
    IMPORTANT: filtering must be applied to the marketdata block
    via marketdata.securities, not by a top-level SECID query parameter.
    """
    url = f"{MOEX_ISS}/engines/stock/markets/index/securities.json"

    payload = fetch_json(url, {
        "iss.meta": "off",
        "iss.only": "marketdata,securities",
        "marketdata.securities": "IMOEX",
        "securities": "IMOEX",
        "marketdata.columns": "SECID,CURRENTVALUE,LASTVALUE,PREVVALUE",
        "securities.columns": "SECID,SHORTNAME",
    })

    market_rows = rows(payload, "marketdata")
    if market_rows:
        item = market_rows[0]
        value = item.get("CURRENTVALUE")
        if value is None:
            value = item.get("LASTVALUE")
        prev = item.get("PREVVALUE")

        change = None
        if value is not None and prev not in (None, 0):
            change = (float(value) / float(prev) - 1) * 100

        return {"value": value, "change": change}

    # Fallback: today's latest historical close if the live block is empty.
    # This is better than displaying a fake 0.
    hist_url = (
        f"{MOEX_ISS}/history/engines/stock/markets/index/"
        f"boards/SNDX/securities/IMOEX.json"
    )
    today = date.today().isoformat()
    yesterday = (date.today() - timedelta(days=7)).isoformat()

    try:
        hist = fetch_json(hist_url, {
            "iss.meta": "off",
            "from": yesterday,
            "till": today,
            "limit": 20,
        })
        hist_rows = rows(hist, "history")
        if hist_rows:
            last = hist_rows[-1]
            close = last.get("CLOSE")
            prev_close = hist_rows[-2].get("CLOSE") if len(hist_rows) > 1 else None
            change = None
            if close is not None and prev_close not in (None, 0):
                change = (float(close) / float(prev_close) - 1) * 100
            return {"value": close, "change": change}
    except Exception:
        pass

    return {"value": None, "change": None}


def _clean_candle(item: dict[str, Any]) -> dict[str, Any] | None:
    """Normalize one MOEX candle row and drop incomplete rows."""
    close = item.get("close")
    if close is None:
        return None

    try:
        open_v = float(item["open"])
        high_v = float(item["high"])
        low_v = float(item["low"])
        close_v = float(close)
        volume_v = float(item.get("volume") or 0)
    except (TypeError, ValueError, KeyError):
        return None

    return {
        "open": open_v,
        "high": high_v,
        "low": low_v,
        "close": close_v,
        "volume": volume_v,
        "begin": item.get("begin"),
        "end": item.get("end"),
    }


def _parse_begin(value: Any) -> datetime.datetime | None:
    if not value:
        return None
    try:
        text = str(value).strip().replace("Z", "+00:00")
        return datetime.datetime.fromisoformat(text)
    except ValueError:
        return None


def _parse_end(value: Any) -> datetime.datetime | None:
    dt = _parse_begin(value)
    return dt


def _aggregate_bucket(bucket: list[dict[str, Any]], begin: datetime.datetime, end: datetime.datetime) -> dict[str, Any]:
    return {
        "open": bucket[0]["open"],
        "high": max(x["high"] for x in bucket),
        "low": min(x["low"] for x in bucket),
        "close": bucket[-1]["close"],
        "volume": sum(x["volume"] for x in bucket),
        "begin": begin.isoformat(sep=" "),
        "end": end.isoformat(sep=" "),
    }


def _aggregate_intraday(candles: list[dict[str, Any]], minutes: int) -> list[dict[str, Any]]:
    """
    Build 4h/6h/12h candles from 60-minute MOEX candles.
    Buckets are aligned to the market-day clock (e.g. 09:00, 13:00, 17:00 for 4h).
    """
    grouped: dict[tuple[datetime.date, int], list[tuple[datetime.datetime, dict[str, Any]]]] = {}

    for candle in candles:
        dt = _parse_begin(candle.get("begin"))
        if dt is None:
            continue

        bucket_hour = (dt.hour // (minutes // 60)) * (minutes // 60)
        key = (dt.date(), bucket_hour)
        grouped.setdefault(key, []).append((dt, candle))

    result: list[dict[str, Any]] = []
    for (day, hour), items in sorted(grouped.items()):
        items.sort(key=lambda x: x[0])
        bucket = [c for _, c in items]
        begin = items[0][0].replace(hour=hour, minute=0, second=0, microsecond=0)
        last_dt = _parse_end(bucket[-1].get("end")) or items[-1][0]
        end = last_dt
        result.append(_aggregate_bucket(bucket, begin, end))

    return result


def _aggregate_trading_days(candles: list[dict[str, Any]], count: int) -> list[dict[str, Any]]:
    """
    Build N-trading-day candles from daily MOEX candles.
    Every output candle contains N consecutive available trading sessions.
    """
    grouped_days: dict[datetime.date, list[dict[str, Any]]] = {}

    for candle in candles:
        dt = _parse_begin(candle.get("begin"))
        if dt is None:
            continue
        grouped_days.setdefault(dt.date(), []).append(candle)

    days = sorted(grouped_days)
    day_buckets: list[list[dict[str, Any]]] = []

    for i in range(0, len(days), count):
        selected = days[i:i + count]
        if not selected:
            continue
        bucket = []
        for day in selected:
            grouped_days[day].sort(key=lambda c: str(c.get("begin")))
            bucket.extend(grouped_days[day])
        if bucket:
            day_buckets.append(bucket)

    result = []
    for bucket in day_buckets:
        begin = _parse_begin(bucket[0].get("begin"))
        end = _parse_end(bucket[-1].get("end")) or _parse_begin(bucket[-1].get("begin"))
        if begin and end:
            result.append(_aggregate_bucket(bucket, begin, end))
    return result


def _aggregate_months(candles: list[dict[str, Any]], months_per_candle: int) -> list[dict[str, Any]]:
    """Build 1/3/6-month calendar candles."""
    grouped: dict[tuple[int, int], list[dict[str, Any]]] = {}

    for candle in candles:
        dt = _parse_begin(candle.get("begin"))
        if dt is None:
            continue
        month_index = (dt.month - 1) // months_per_candle
        key = (dt.year, month_index)
        grouped.setdefault(key, []).append(candle)

    result = []
    for (year, block), bucket in sorted(grouped.items()):
        bucket.sort(key=lambda c: str(c.get("begin")))
        first_dt = _parse_begin(bucket[0].get("begin"))
        end_dt = _parse_end(bucket[-1].get("end")) or _parse_begin(bucket[-1].get("begin"))
        if first_dt and end_dt:
            result.append(_aggregate_bucket(bucket, first_dt, end_dt))
    return result


def _aggregate_years(candles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Build one candle per calendar year."""
    grouped: dict[int, list[dict[str, Any]]] = {}

    for candle in candles:
        dt = _parse_begin(candle.get("begin"))
        if dt is None:
            continue
        grouped.setdefault(dt.year, []).append(candle)

    result = []
    for year, bucket in sorted(grouped.items()):
        bucket.sort(key=lambda c: str(c.get("begin")))
        begin = _parse_begin(bucket[0].get("begin"))
        end = _parse_end(bucket[-1].get("end")) or _parse_begin(bucket[-1].get("begin"))
        if begin and end:
            result.append(_aggregate_bucket(bucket, begin, end))
    return result


def get_candles(
    symbol: str,
    interval: int = 60,
    days: int = 14,
) -> list[dict[str, Any]]:
    """
    Return candles for any UI timeframe.

    MOEX is queried at a native/base interval where practical:
      1m/5m/15m/30m/1h -> directly from ISS
      4h/6h/12h       -> aggregate 1h candles
      1d and above    -> aggregate daily candles
    """
    if interval <= 60:
        base_interval = interval
        source_days = days
    elif interval < 1440:
        base_interval = 60
        # Extra calendar room because weekends/holidays do not produce candles.
        source_days = min(3650, max(days + 14, int(days * 1.25)))
    else:
        base_interval = 24 * 60
        source_days = min(3650, max(days + 30, int(days * 1.20)))

    till = date.today()
    since = till - timedelta(days=source_days)

    url = (
        f"{MOEX_ISS}/engines/stock/markets/shares/"
        f"boards/{BOARD}/securities/{symbol}/candles.json"
    )

    payload = fetch_json(url, {
        "iss.meta": "off",
        "from": since.isoformat(),
        "till": till.isoformat(),
        "interval": base_interval,
        "start": 0,
    })

    base = []
    for item in rows(payload, "candles"):
        candle = _clean_candle(item)
        if candle:
            base.append(candle)

    if interval <= 60:
        return base

    if interval < 1440:
        return _aggregate_intraday(base, interval)

    if interval == 1440:
        return base

    if interval in (3 * 1440, 7 * 1440, 14 * 1440):
        return _aggregate_trading_days(base, interval // 1440)

    if interval == 30 * 1440:
        return _aggregate_months(base, 1)

    if interval == 90 * 1440:
        return _aggregate_months(base, 3)

    if interval == 180 * 1440:
        return _aggregate_months(base, 6)

    if interval == 365 * 1440:
        return _aggregate_years(base)

    raise ValueError(f"Unsupported candle interval: {interval}")


def get_spark(symbol: str) -> list[float]:
    now = time.time()
    cached = spark_cache.get(symbol)
    if cached and now - cached[0] < CACHE_TTL:
        return cached[1]
    candles = get_candles(symbol, interval=24, days=40)
    value = [float(c["close"]) for c in candles[-20:]]
    spark_cache[symbol] = (now, value)
    return value


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/market")
def market() -> dict[str, Any]:
    now = time.time()
    if market_cache["value"] is not None and now - market_cache["ts"] < CACHE_TTL:
        return market_cache["value"]

    quotes = get_market_quotes()

    def one(ticker: str) -> tuple[str, list[float]]:
        try:
            return ticker, get_spark(ticker)
        except Exception:
            return ticker, []

    sparks: dict[str, list[float]] = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(one, ticker) for ticker in TICKERS]
        for future in as_completed(futures):
            ticker, spark = future.result()
            sparks[ticker] = spark

    for quote in quotes:
        quote["spark"] = sparks.get(quote["ticker"], [])

    result = {"index": get_imoex(), "quotes": quotes}
    market_cache["ts"] = now
    market_cache["value"] = result
    return result


@app.get("/api/candles")
def candles(
    symbol: str = Query(..., pattern=r"^[A-Z0-9.]+$"),
    interval: int = Query(60, ge=1, le=525600),
    days: int = Query(14, ge=1, le=3650),
) -> dict[str, Any]:
    if symbol not in TICKERS:
        raise HTTPException(status_code=404, detail="Unknown symbol")

    supported = {
        1, 5, 15, 30, 60,
        240, 360, 720,
        1440, 4320, 10080, 20160,
        43200, 129600, 259200,
        525600,
    }
    if interval not in supported:
        raise HTTPException(status_code=400, detail="Unsupported candle interval")

    return {
        "symbol": symbol,
        "interval": interval,
        "candles": get_candles(symbol, interval, days),
    }
