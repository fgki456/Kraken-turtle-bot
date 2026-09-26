#!/usr/bin/env python3
"""Kraken Futures Turtle V2 paper simulation. No private API or order endpoint."""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from decimal import Decimal
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

BASE = "https://futures.kraken.com"
SYMBOLS = tuple(s.strip().upper() for s in os.getenv("SYMBOLS", "PF_XBTUSD,PF_ETHUSD").split(",") if s.strip())
RISK = Decimal(os.getenv("RISK_PER_UNIT_USD", "1"))
POLL = int(os.getenv("POLL_SECONDS", "60"))
REPORT = int(os.getenv("REPORT_SECONDS", "900"))
DB_PATH = os.getenv("DB_PATH", "turtle_paper.sqlite3")
MAX_NOTIONAL = Decimal(os.getenv("MAX_PAPER_NOTIONAL_USD", "200"))
MAX_UNITS = 4

logging.basicConfig(level=logging.INFO, format="%(asctime)s UTC %(levelname)s %(message)s", force=True)
log = logging.getLogger("turtle-paper")


def get_json(path):
    req = Request(BASE + path, headers={"Accept": "application/json", "User-Agent": "turtle-v2-paper/1.0"})
    try:
        with urlopen(req, timeout=15) as response:
            return json.load(response)
    except (HTTPError, URLError, TimeoutError) as exc:
        raise RuntimeError(f"Kraken public API: {exc}") from exc


def candles(symbol):
    data = get_json(f"/api/charts/v1/trade/{symbol}/1h?count=80")
    rows = data.get("candles", [])
    if not isinstance(rows, list) or not rows:
        raise RuntimeError(f"No candles for {symbol}: {str(data)[:250]}")
    rows.sort(key=lambda b: int(b["time"]))
    now_ms = int(time.time() * 1000)
    if int(rows[-1]["time"]) > now_ms + 60_000 or now_ms - int(rows[-1]["time"]) > 7_200_000:
        raise RuntimeError(f"Stale or future candle for {symbol}; skipping market")
    # A candle with time == this hour's open is still forming.
    done = [b for b in rows if int(b["time"]) + 3_600_000 <= now_ms]
    if len(done) < 22:
        raise RuntimeError(f"Only {len(done)} completed candles for {symbol}; need 22")
    current = rows[-1]
    return done, Decimal(str(current["close"]))


def atr_before_signal(done):
    previous_close = Decimal(str(done[-22]["close"]))
    true_ranges = []
    for bar in done[-21:-1]:
        hi, lo = Decimal(str(bar["high"])), Decimal(str(bar["low"]))
        true_ranges.append(max(hi - lo, abs(hi - previous_close), abs(lo - previous_close)))
        previous_close = Decimal(str(bar["close"]))
    return sum(true_ranges) / 20


def signal(done):
    close = Decimal(str(done[-1]["close"]))
    hi = max(Decimal(str(b["high"])) for b in done[-21:-1])
    lo = min(Decimal(str(b["low"])) for b in done[-21:-1])
    if close > hi:
        return "LONG"
    if close < lo:
        return "SHORT"
    return None


class Journal:
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.execute("CREATE TABLE IF NOT EXISTS meta (symbol TEXT PRIMARY KEY, bar INTEGER NOT NULL)")
        self.db.execute("""CREATE TABLE IF NOT EXISTS campaigns (
            symbol TEXT PRIMARY KEY, direction TEXT NOT NULL, entry TEXT NOT NULL,
            average TEXT NOT NULL, n TEXT NOT NULL, unit_qty TEXT NOT NULL,
            units INTEGER NOT NULL, stop TEXT NOT NULL, fast INTEGER NOT NULL)""")
        self.db.commit()

    def seen(self, symbol):
        row = self.db.execute("SELECT bar FROM meta WHERE symbol=?", (symbol,)).fetchone()
        return row[0] if row else 0

    def mark_seen(self, symbol, bar):
        self.db.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (symbol, bar))
        self.db.commit()

    def load(self, symbol):
        row = self.db.execute("SELECT * FROM campaigns WHERE symbol=?", (symbol,)).fetchone()
        if row is None:
            return None
        return dict(zip(("symbol", "direction", "entry", "average", "n", "unit_qty", "units", "stop", "fast"), row))

    def positions(self):
        return [self.load(row[0]) for row in self.db.execute("SELECT symbol FROM campaigns")]

    def save(self, p):
        self.db.execute("INSERT OR REPLACE INTO campaigns VALUES (?,?,?,?,?,?,?,?,?)", tuple(p[k] for k in
                        ("symbol", "direction", "entry", "average", "n", "unit_qty", "units", "stop", "fast")))
        self.db.commit()

    def close(self, symbol):
        self.db.execute("DELETE FROM campaigns WHERE symbol=?", (symbol,))
        self.db.commit()


def total_notional(journal, last_prices):
    return sum((Decimal(str(p["unit_qty"])) * p["units"] * last_prices.get(p["symbol"], Decimal(str(p["entry"])))
                for p in journal.positions()), Decimal(0))


def manage(journal, p, done, price, last_prices):
    symbol, direction = p["symbol"], p["direction"]
    entry, n, stop = (Decimal(str(p[k])) for k in ("entry", "n", "stop"))
    sign = 1 if direction == "LONG" else -1
    if sign * (price - stop) <= 0:
        reason = "2.5N STOP"
    else:
        if not p["fast"] and sign * (price - entry) >= 4 * n:
            p["fast"] = 1
            journal.save(p)
            log.info("D5 SWITCH %s %s price=%s", symbol, direction, price)
        channel = done[-(5 if p["fast"] else 10):]
        exit_level = (min(Decimal(str(b["low"])) for b in channel) if sign == 1 else
                      max(Decimal(str(b["high"])) for b in channel))
        reason = ("D5" if p["fast"] else "D10") if sign * (price - exit_level) <= 0 else None
    if reason:
        pnl = sign * (price - Decimal(str(p["average"]))) * Decimal(str(p["unit_qty"])) * p["units"]
        log.info("PAPER CLOSE %s %s reason=%s price=%s gross_pnl_usd=%.4f", symbol, direction, reason, price, pnl)
        journal.close(symbol)
        return
    if p["units"] < MAX_UNITS and sign * (price - entry) >= Decimal("0.75") * n * p["units"]:
        unit_notional = Decimal(str(p["unit_qty"])) * price
        if total_notional(journal, last_prices) + unit_notional > MAX_NOTIONAL:
            log.info("PAPER ADD BLOCKED %s cap=%s USD", symbol, MAX_NOTIONAL)
            return
        units = p["units"]
        p["average"] = str((Decimal(str(p["average"])) * units + price) / (units + 1))
        p["units"] = units + 1
        journal.save(p)
        log.info("PAPER ADD %s %s units=%s price=%s", symbol, direction, p["units"], price)


def run_once(journal, last_prices):
    for symbol in SYMBOLS:
        try:
            done, price = candles(symbol)
            last_prices[symbol] = price
            p = journal.load(symbol)
            if p:
                manage(journal, p, done, price, last_prices)
                # Never reverse during the same scan after a close.
                continue
            bar_time = int(done[-1]["time"])
            if journal.seen(symbol) >= bar_time:
                continue
            direction = signal(done)
            journal.mark_seen(symbol, bar_time)
            if direction is None:
                continue
            n = atr_before_signal(done)
            if n <= 0:
                log.warning("SKIP %s ATR20=0", symbol)
                continue
            unit_qty = RISK / (Decimal("2.5") * n)
            if total_notional(journal, last_prices) + price * unit_qty > MAX_NOTIONAL:
                log.info("PAPER OPEN BLOCKED %s cap=%s USD", symbol, MAX_NOTIONAL)
                continue
            sign = 1 if direction == "LONG" else -1
            stop = price - sign * Decimal("2.5") * n
            journal.save(dict(symbol=symbol, direction=direction, entry=str(price), average=str(price),
                              n=str(n), unit_qty=str(unit_qty), units=1, stop=str(stop), fast=0))
            log.info("PAPER OPEN %s %s price=%s N=%s qty=%s stop=%s", symbol, direction, price, n, unit_qty, stop)
        except Exception:
            log.exception("SCAN FAILED %s", symbol)


def main():
    if os.getenv("LIVE_TRADING", "false").lower() == "true":
        raise RuntimeError("This version is PAPER ONLY; live trading is disabled even if LIVE_TRADING=true")
    if not SYMBOLS or RISK <= 0 or POLL < 10 or REPORT < 60 or MAX_NOTIONAL <= 0:
        raise ValueError("Invalid configuration; check symbols, risk, polling and notional limit")
    journal = Journal(DB_PATH)
    prices = {}
    log.info("KRAKEN FUTURES TURTLE V2 PAPER START symbols=%s risk=%s USD max_notional=%s USD", ",".join(SYMBOLS), RISK, MAX_NOTIONAL)
    last_report = 0
    while True:
        run_once(journal, prices)
        if time.monotonic() - last_report >= REPORT:
            log.info("PAPER REPORT positions=%s prices=%s", len(journal.positions()),
                     ", ".join(f"{symbol}:{price}" for symbol, price in prices.items()) or "waiting for quotes")
            last_report = time.monotonic()
        time.sleep(POLL)


if __name__ == "__main__":
    main()
