from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
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


def get_candles(
    symbol: str,
    interval: int = 60,
    days: int = 14,
) -> list[dict[str, Any]]:
    till = date.today()
    since = till - timedelta(days=days)

    url = (
        f"{MOEX_ISS}/engines/stock/markets/shares/"
        f"boards/{BOARD}/securities/{symbol}/candles.json"
    )

    payload = fetch_json(url, {
        "iss.meta": "off",
        "from": since.isoformat(),
        "till": till.isoformat(),
        "interval": interval,
        "start": 0,
    })

    return [
        {
            "open": item.get("open"),
            "high": item.get("high"),
            "low": item.get("low"),
            "close": item.get("close"),
            "volume": item.get("volume"),
            "begin": item.get("begin"),
            "end": item.get("end"),
        }
        for item in rows(payload, "candles")
        if item.get("close") is not None
    ]


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
    interval: int = Query(60, ge=1, le=60),
    days: int = Query(14, ge=1, le=3650),
) -> dict[str, Any]:
    if symbol not in TICKERS:
        raise HTTPException(status_code=404, detail="Unknown symbol")

    return {
        "symbol": symbol,
        "interval": interval,
        "candles": get_candles(symbol, interval, days),
    }
