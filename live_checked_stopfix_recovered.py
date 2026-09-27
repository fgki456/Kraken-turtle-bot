#!/usr/bin/env python3
"""Kraken Futures Turtle execution engine. Read-only preview is the default.

Run as a separate process from main.py. Production orders require an explicit
arm switch, a mounted persistent directory, and a dedicated Futures API key.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import sqlite3
import time
import uuid
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from main import SYMBOLS, RISK, POLL, atr_before_signal, signal

log = logging.getLogger("turtle-live")
logging.basicConfig(level=logging.INFO, format="%(asctime)s UTC %(levelname)s %(message)s")


class Halt(RuntimeError):
    """An uncertain exchange state. Do not place more orders."""


def dec(value):
    return Decimal(str(value))


def clean_number(value):
    return format(value, "f")


class Exchange:
    def __init__(self, key, secret, base):
        if base not in ("https://futures.kraken.com", "https://demo-futures.kraken.com"):
            raise ValueError("Unrecognized Kraken Futures endpoint")
        self.key = key
        self.secret = base64.b64decode(secret, validate=True)
        self.base = base

    def request(self, endpoint, params=None, method="GET"):
        if endpoint not in {"/accounts", "/openpositions", "/openorders", "/instruments",
                            "/sendorder", "/cancelorder", "/orders/status"}:
            raise ValueError("Unapproved API endpoint")
        encoded = urlencode(params or {})
        path = "/derivatives/api/v3" + endpoint
        signed_path = "/api/v3" + endpoint
        is_private = endpoint != "/instruments"
        headers = {"Accept": "application/json", "User-Agent": "turtle-v2-live/0.1"}
        if is_private:
            if not self.key:
                raise Halt("Futures API key missing")
            payload = (encoded + signed_path).encode()
            digest = hashlib.sha256(payload).digest()
            headers["APIKey"] = self.key
            headers["Authent"] = base64.b64encode(hmac.new(self.secret, digest, hashlib.sha512).digest()).decode()
        if method == "POST":
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            request = Request(self.base + path, data=encoded.encode(), headers=headers, method="POST")
        else:
            request = Request(self.base + path + ("?" + encoded if encoded else ""), headers=headers)
        with urlopen(request, timeout=15) as response:
            result = json.load(response)
        if not isinstance(result, dict) or result.get("result") != "success":
            raise Halt("Kraken rejected " + endpoint + ": " + str(result.get("error", "unknown")))
        return result

    def positions(self):
        rows = self.request("/openpositions").get("openPositions")
        if not isinstance(rows, list):
            raise Halt("Unexpected openpositions response")
        return rows

    def orders(self):
        rows = self.request("/openorders").get("openOrders")
        if not isinstance(rows, list):
            raise Halt("Unexpected openorders response")
        return rows

    def order_status(self, order_id):
        # Kraken models resting stop triggers separately from book orders.
        # This POST endpoint only reads status; it never places an order.
        rows = self.request("/orders/status", {"orderIds": order_id}, "POST").get("orders")
        if not isinstance(rows, list):
            raise Halt("Unexpected order status response")
        matches = [row for row in rows if row.get("order", {}).get("orderId") == order_id]
        return matches[0] if len(matches) == 1 else None

    def instruments(self):
        rows = self.request("/instruments").get("instruments")
        if not isinstance(rows, list):
            raise Halt("Unexpected instruments response")
        return {i["symbol"].upper(): i for i in rows}

    def candles(self, symbol):
        request = Request(self.base + f"/api/charts/v1/trade/{symbol}/1h?count=80",
                          headers={"Accept": "application/json", "User-Agent": "turtle-v2-live/0.1"})
        with urlopen(request, timeout=15) as response:
            rows = json.load(response).get("candles", [])
        if not isinstance(rows, list) or not rows:
            raise Halt("Missing candle data: " + symbol)
        rows.sort(key=lambda row: int(row["time"]))
        now_ms = int(time.time() * 1000)
        if int(rows[-1]["time"]) > now_ms + 60_000 or now_ms - int(rows[-1]["time"]) > 7_200_000:
            raise Halt("Stale candle data: " + symbol)
        done = [row for row in rows if int(row["time"]) + 3_600_000 <= now_ms]
        if len(done) < 22:
            raise Halt("Not enough completed candles: " + symbol)
        return done, dec(rows[-1]["close"])

    def send(self, **params):
        result = self.request("/sendorder", params, "POST").get("sendStatus", {})
        if result.get("status") not in ("placed", "filled", "partiallyFilled"):
            raise Halt("Order refused: " + str(result.get("status", "unknown")))
        return result

    def cancel(self, order_id):
        result = self.request("/cancelorder", {"order_id": order_id}, "POST").get("cancelStatus", {})
        if result.get("status") != "cancelled":
            raise Halt("Stop cancellation uncertain")


class State:
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.execute("CREATE TABLE IF NOT EXISTS bars(symbol TEXT PRIMARY KEY, bar INTEGER NOT NULL)")
        self.db.execute("""CREATE TABLE IF NOT EXISTS positions(
            symbol TEXT PRIMARY KEY, side TEXT NOT NULL, entry TEXT NOT NULL,
            n TEXT NOT NULL, size TEXT NOT NULL, stop TEXT NOT NULL,
            stops TEXT NOT NULL, unit_size TEXT NOT NULL,
            units INTEGER NOT NULL, fast INTEGER NOT NULL DEFAULT 0,
            avg_entry TEXT NOT NULL DEFAULT '0',
            protection_active INTEGER NOT NULL DEFAULT 0,
            peak_pnl TEXT NOT NULL DEFAULT '0',
            profit_floor TEXT NOT NULL DEFAULT '0')""")
        columns = {row[1] for row in self.db.execute("PRAGMA table_info(positions)")}
        for name, definition in (("avg_entry", "TEXT NOT NULL DEFAULT '0'"),
                                 ("protection_active", "INTEGER NOT NULL DEFAULT 0"),
                                 ("peak_pnl", "TEXT NOT NULL DEFAULT '0'"),
                                 ("profit_floor", "TEXT NOT NULL DEFAULT '0'")):
            if name not in columns:
                self.db.execute(f"ALTER TABLE positions ADD COLUMN {name} {definition}")
        # Old state rows predate avg_entry; the initial fill price is the best
        # available campaign basis until the exchange's current average is read.
        self.db.execute("UPDATE positions SET avg_entry=entry WHERE avg_entry='0'")
        self.db.execute("CREATE TABLE IF NOT EXISTS pending(id INTEGER PRIMARY KEY CHECK(id=1), action TEXT NOT NULL)")
        self.db.commit()

    def items(self):
        self.db.row_factory = sqlite3.Row
        rows = self.db.execute("SELECT * FROM positions").fetchall()
        return {row["symbol"]: dict(row) for row in rows}

    def bar(self, symbol):
        row = self.db.execute("SELECT bar FROM bars WHERE symbol=?", (symbol,)).fetchone()
        return row[0] if row else None

    def mark(self, symbol, bar):
        self.db.execute("INSERT OR REPLACE INTO bars VALUES (?,?)", (symbol, bar))
        self.db.commit()

    def pending(self):
        return self.db.execute("SELECT action FROM pending WHERE id=1").fetchone()

    def begin(self, action):
        if self.pending():
            raise Halt("Unresolved pending exchange action")
        self.db.execute("INSERT INTO pending VALUES (1,?)", (action,))
        self.db.commit()

    def clear(self):
        self.db.execute("DELETE FROM pending")
        self.db.commit()

    def save(self, p):
        keys = ("symbol", "side", "entry", "n", "size", "stop", "stops", "unit_size",
                "units", "fast", "avg_entry", "protection_active", "peak_pnl", "profit_floor")
        self.db.execute(f"INSERT OR REPLACE INTO positions ({','.join(keys)}) "
                        f"VALUES ({','.join('?' for _ in keys)})", tuple(p[k] for k in keys))
        self.db.commit()

    def delete(self, symbol):
        self.db.execute("DELETE FROM positions WHERE symbol=?", (symbol,))
        self.db.commit()


def size_for(instrument, price, n, risk, available):
    # Only linear perpetuals: size is base asset quantity; precision from exchange.
    if instrument.get("type") != "flexible_futures" or not instrument.get("tradeable"):
        raise ValueError("Only tradeable PF linear perpetuals are supported")
    precision = instrument.get("contractValueTradePrecision")
    if not isinstance(precision, int) or precision < 0 or precision > 12:
        raise ValueError("Unknown size precision")
    if n <= 0 or price <= 0:
        raise ValueError("Invalid market price or ATR")
    step = Decimal(1).scaleb(-precision)
    # Kraken's mkt order allows execution away from the observed price.
    worst_stop_distance = Decimal("2.5") * n + price * Decimal("0.01")
    size = min(risk / worst_stop_distance, available / (price * Decimal("1.01")))
    size = size.quantize(step, rounding=ROUND_DOWN)
    if size <= 0 or size * worst_stop_distance > risk or size * price * Decimal("1.01") > available:
        return Decimal(0)
    return size


def price_for(raw, instrument, side):
    tick = dec(instrument["tickSize"])
    if tick <= 0:
        raise Halt("Invalid price tick")
    # A stop for a long rounds up; for a short rounds down, so it is not looser.
    rounding = ROUND_UP if side == "long" else ROUND_DOWN
    return (raw / tick).to_integral_value(rounding=rounding) * tick


class Engine:
    def __init__(self, exchange, state, armed=False, max_notional=Decimal("200")):
        self.ex = exchange
        self.state = state
        self.armed = armed
        self.cap = max_notional
        self.specs = self.ex.instruments()
        if self.cap <= 0 or RISK <= 0:
            raise ValueError("Risk limits must be positive")

    def snapshot(self):
        positions, orders = self.ex.positions(), self.ex.orders()
        owned = self.state.items()
        if self.state.pending():
            raise Halt("Pending action from prior run; inspect Futures positions and orders")
        if any(symbol not in SYMBOLS for symbol in owned):
            raise Halt("A saved live position was removed from SYMBOLS")
        if any(p.get("symbol", "").upper() not in owned for p in positions):
            raise Halt("Unknown exchange position; no new orders")
        by_symbol = {p["symbol"].upper(): p for p in positions}
        allowed_stops = set()
        for symbol, saved in owned.items():
            p = by_symbol.get(symbol)
            if not p:
                # A stop may have filled; leave stale record untouched until reviewed.
                raise Halt("Saved position absent on exchange: " + symbol)
            if p.get("side") != saved["side"] or dec(p["size"]) != dec(saved["size"]):
                raise Halt("Position size/side mismatch: " + symbol)
            attached = json.loads(saved["stops"])
            if not 1 <= len(attached) == saved["units"] <= 4:
                raise Halt("Invalid number of units/stops: " + symbol)
            if sum((dec(s["size"]) for s in attached), Decimal(0)) != dec(saved["size"]):
                raise Halt("Stops do not cover full position: " + symbol)
            for entry in attached:
                stop_id = entry["id"]
                if stop_id in allowed_stops:
                    raise Halt("Duplicate stop ID: " + symbol)
                allowed_stops.add(stop_id)
                matches = [o for o in orders if o.get("order_id") == stop_id]
                if not self.stop_confirmed(stop_id, symbol, saved["side"], dec(entry["size"]),
                                           dec(entry["stop"]) if "stop" in entry else dec(saved["stop"]),
                                           matches):
                    raise Halt("Invalid protective stop: " + symbol)
        if any(o.get("order_id") not in allowed_stops for o in orders):
            raise Halt("Unknown exchange order; no new orders")
        return by_symbol

    def stop_confirmed(self, stop_id, symbol, side, size, stop_price, matches):
        expected_side = "sell" if side == "long" else "buy"
        if len(matches) == 1:
            row = matches[0]
            if (row.get("orderType") == "stp" and row.get("symbol", "").upper() == symbol
                    and row.get("side") == expected_side and row.get("reduceOnly") is True
                    and dec(row.get("unfilledSize", 0)) >= size
                    and dec(row.get("stopPrice", 0)) == stop_price):
                return True
        status = self.ex.order_status(stop_id)
        if not status or status.get("status") != "ENTERED_BOOK" or status.get("error"):
            return False
        row = status.get("order", {})
        trigger = row.get("priceTriggerOptions") or {}
        return (row.get("type") == "TRIGGER_ORDER"
                and row.get("orderId") == stop_id
                and row.get("symbol", "").upper() == symbol
                and row.get("side", "").lower() == expected_side
                and row.get("reduceOnly") is True
                and dec(row.get("quantity", 0)) - dec(row.get("filled", 0)) >= size
                and dec(trigger.get("triggerPrice", 0)) == stop_price
                and trigger.get("triggerSignal") in ("MARK_PRICE", "MarkPrice"))

    def stop_order(self, symbol, side, size, stop):
        status = self.ex.send(orderType="stp", symbol=symbol,
                              side="sell" if side == "long" else "buy",
                              size=clean_number(size), stopPrice=clean_number(stop),
                              triggerSignal="mark", reduceOnly="true", cliOrdId=str(uuid.uuid4()))
        if not status.get("order_id"):
            raise Halt("Stop ID missing")
        stop_id = status["order_id"]
        # An acknowledged order can take a moment to appear in openorders.
        # Never treat an unverified stop as protection; enter() will close the
        # position if these bounded, read-only checks do not confirm it.
        for attempt in range(3):
            matches = [o for o in self.ex.orders() if o.get("order_id") == stop_id]
            if self.stop_confirmed(stop_id, symbol, side, size, stop, matches):
                return stop_id
            if len(matches) == 1:
                order = matches[0]
                # Log public order fields only, never API credentials or account data.
                log.error("Stop confirmation mismatch %s: type=%s symbol=%s side=%s "
                          "reduceOnly=%s unfilledSize=%s expectedSize=%s",
                          symbol, order.get("orderType"), order.get("symbol"),
                          order.get("side"), order.get("reduceOnly"),
                          order.get("unfilledSize"), size)
                break
            if attempt < 2:
                time.sleep(0.3)
        raise Halt("Protective stop not confirmed on exchange: " + symbol)

    def emergency_close(self, symbol, side, size):
        # If stop creation fails, do not leave a position knowingly unprotected.
        self.ex.send(orderType="mkt", symbol=symbol,
                     side="sell" if side == "long" else "buy", size=clean_number(size),
                     reduceOnly="true", cliOrdId=str(uuid.uuid4()))
        log.critical("EMERGENCY CLOSE SUBMITTED %s; VERIFY ON KRAKEN", symbol)

    def enter(self, symbol, side, price, n, instrument, available):
        size = size_for(instrument, price, n, RISK, available)
        if not size:
            log.info("LIVE SKIP %s: minimum size exceeds risk/notional limit", symbol)
            return
        stop = price_for(price + (-1 if side == "long" else 1) * Decimal("2.5") * n,
                         instrument, side)
        if stop <= 0:
            return
        self.state.begin("enter:" + symbol)
        try:
            self.ex.send(orderType="mkt", symbol=symbol, side="buy" if side == "long" else "sell",
                         size=clean_number(size), cliOrdId=str(uuid.uuid4()))
            actual = next((p for p in self.ex.positions() if p["symbol"].upper() == symbol), None)
            if not actual or actual.get("side") != side or dec(actual["size"]) <= 0:
                raise Halt("Entry fill not confirmed: " + symbol)
            actual_size = dec(actual["size"])
            try:
                stop_id = self.stop_order(symbol, side, actual_size, stop)
            except Exception:
                # A stop may have triggered immediately; check before closing.
                remaining = next((p for p in self.ex.positions() if p["symbol"].upper() == symbol), None)
                if remaining:
                    self.emergency_close(symbol, side, dec(remaining["size"]))
                raise Halt("Protective stop failed; emergency close attempted: " + symbol)
            self.state.save(dict(symbol=symbol, side=side, entry=str(actual["price"]),
                                 n=str(n), size=str(actual_size), stop=str(stop),
                                 stops=json.dumps([{"id": stop_id, "size": str(actual_size)}]),
                                 unit_size=str(actual_size), units=1, fast=0,
                                 avg_entry=str(actual["price"]), protection_active=0,
                                 peak_pnl="0", profit_floor="0"))
            self.state.clear()
            log.info("LIVE OPEN %s %s size=%s stop=%s", symbol, side, actual_size, stop)
        except Exception:
            # Keep pending marker; restarting must not repeat an uncertain entry.
            raise

    def add(self, saved, price, available):
        symbol, side = saved["symbol"], saved["side"]
        stop = dec(saved["stop"])
        # Every new part has at most $1 of loss to the original protective stop.
        risk_distance = abs(price - stop) + price * Decimal("0.01")
        if risk_distance <= 0:
            return Decimal(0)
        instrument = self.specs[symbol]
        precision = instrument.get("contractValueTradePrecision")
        if not isinstance(precision, int) or not 0 <= precision <= 12:
            raise Halt("Unknown size precision: " + symbol)
        step = Decimal(1).scaleb(-precision)
        size = min(dec(saved["unit_size"]), RISK / risk_distance,
                   available / (price * Decimal("1.01"))).quantize(step, rounding=ROUND_DOWN)
        if size <= 0:
            log.info("LIVE ADD BLOCKED %s: risk or notional cap", symbol)
            return Decimal(0)
        old_size = dec(saved["size"])
        self.state.begin("add:" + symbol)
        self.ex.send(orderType="mkt", symbol=symbol, side="buy" if side == "long" else "sell",
                     size=clean_number(size), cliOrdId=str(uuid.uuid4()))
        actual = next((p for p in self.ex.positions() if p["symbol"].upper() == symbol), None)
        if not actual or actual.get("side") != side or dec(actual["size"]) <= old_size:
            raise Halt("Addition fill not confirmed: " + symbol)
        extra = dec(actual["size"]) - old_size
        try:
            stop_id = self.stop_order(symbol, side, extra, stop)
        except Exception:
            # Try to remove only the newly added size; existing stops stay on exchange.
            remaining = next((p for p in self.ex.positions() if p["symbol"].upper() == symbol), None)
            if remaining and dec(remaining["size"]) > old_size:
                self.emergency_close(symbol, side, dec(remaining["size"]) - old_size)
            raise Halt("Add-on unprotected; reduce-only reversal attempted: " + symbol)
        attached = json.loads(saved["stops"])
        attached.append({"id": stop_id, "size": str(extra)})
        saved.update(stops=json.dumps(attached), size=str(old_size + extra), units=saved["units"] + 1,
                     avg_entry=str(actual.get("price", saved["avg_entry"])))
        self.state.save(saved)
        self.state.clear()
        log.info("LIVE ADD %s %s units=%s size=%s", symbol, side, saved["units"], extra)
        return extra

    def close(self, saved, reason):
        symbol, size, side = saved["symbol"], dec(saved["size"]), saved["side"]
        self.state.begin("close:" + symbol)
        self.ex.send(orderType="mkt", symbol=symbol,
                     side="sell" if side == "long" else "buy", size=clean_number(size),
                     reduceOnly="true", cliOrdId=str(uuid.uuid4()))
        if any(p["symbol"].upper() == symbol for p in self.ex.positions()):
            raise Halt("Close incomplete; exchange position still open: " + symbol)
        ids = {stop["id"] for stop in json.loads(saved["stops"])}
        for stop_id in ids:
            self.ex.cancel(stop_id)
        if any(o.get("order_id") in ids for o in self.ex.orders()):
            raise Halt("Protective stop still open after close: " + symbol)
        self.state.delete(symbol)
        self.state.clear()
        log.info("LIVE CLOSE %s reason=%s", symbol, reason)

    def cycle(self):
        positions = self.snapshot()  # Refuse all activity on unknown positions/orders.
        owned = self.state.items()
        total = Decimal(0)
        for symbol, saved in owned.items():
            _, current_price = self.ex.candles(symbol)  # Never underestimate exposure using an old entry.
            total += dec(saved["size"]) * max(current_price, dec(positions[symbol]["price"]))
        for symbol in SYMBOLS:
            try:
                done, price = self.ex.candles(symbol)
            except Exception as exc:
                log.warning("Candle check failed for %s: %s", symbol, type(exc).__name__)
                continue
            bar = int(done[-1]["time"])
            saved = owned.get(symbol)
            if saved:
                if not self.armed:
                    log.warning("PREVIEW: live position exists %s", symbol)
                    continue
                sign = 1 if saved["side"] == "long" else -1
                entry, n = dec(saved["entry"]), dec(saved["n"])
                avg_entry = dec(positions[symbol].get("price", saved["avg_entry"]))
                saved["avg_entry"] = str(avg_entry)
                current_pnl = Decimal(sign) * (price - avg_entry) * dec(saved["size"])
                fast = bool(saved["fast"]) or sign * (price - entry) >= 4 * n
                channel = done[-(5 if fast else 10):]
                exit_price = (min(dec(x["low"]) for x in channel) if sign == 1 else
                              max(dec(x["high"]) for x in channel))
                if sign * (price - exit_price) <= 0:
                    self.close(saved, "D5" if fast else "D10")
                    continue
                if fast != bool(saved["fast"]):
                    saved["fast"] = 1
                    saved["protection_active"] = 1
                    saved["peak_pnl"] = str(max(Decimal(0), current_pnl))
                    saved["profit_floor"] = str(max(Decimal(0), current_pnl) / 2)
                    log.info("PROFIT LOCK ACTIVE %s peak_pnl_usd=%s floor_usd=%s",
                             symbol, saved["peak_pnl"], saved["profit_floor"])
                if saved["protection_active"]:
                    peak = max(dec(saved["peak_pnl"]), current_pnl)
                    floor = max(dec(saved["profit_floor"]), peak / 2)
                    if peak != dec(saved["peak_pnl"]) or floor != dec(saved["profit_floor"]):
                        saved["peak_pnl"], saved["profit_floor"] = str(peak), str(floor)
                        log.info("PROFIT LOCK RAISED %s peak_pnl_usd=%s floor_usd=%s",
                                 symbol, peak, floor)
                    self.state.save(saved)
                    if current_pnl <= floor:
                        self.close(saved, "50% PEAK PROFIT LOCK")
                        continue
                elif fast != bool(saved["fast"]):
                    self.state.save(saved)
                if (saved["units"] < 4 and
                        sign * (price - entry) >= Decimal("0.75") * n * saved["units"]):
                    if not self.specs.get(symbol, {}).get("tradeable"):
                        raise Halt("Active market not tradeable: " + symbol)
                    extra = self.add(saved, price, max(self.cap - total, Decimal(0)))
                    total += extra * price * Decimal("1.01")
                continue
            previous = self.state.bar(symbol)
            # First observation is baseline, never enter on a historical signal.
            if previous is None:
                self.state.mark(symbol, bar)
                continue
            if previous >= bar:
                continue
            self.state.mark(symbol, bar)
            direction = signal(done)
            if not direction:
                continue
            n = atr_before_signal(done)
            if n <= 0:
                continue
            side = "long" if direction == "LONG" else "short"
            spec = self.specs.get(symbol)
            if not spec:
                log.warning("SKIP %s: absent from Futures instruments", symbol)
                continue
            try:
                planned = size_for(spec, price, n, RISK, max(self.cap - total, Decimal(0)))
            except (ValueError, KeyError) as exc:
                log.warning("SKIP %s: %s", symbol, exc)
                continue
            if not planned:
                continue
            if not self.armed:
                log.info("PREVIEW SIGNAL %s %s theoretical_size=%s", symbol, side, planned)
                continue
            self.enter(symbol, side, price, n, spec, self.cap - total)
            total += planned * price


def main():
    mode = os.getenv("BOT_MODE", "preview")
    if mode not in ("preview", "demo", "live"):
        raise ValueError("BOT_MODE must be preview, demo or live")
    armed = mode in ("demo", "live") and os.getenv("TRADING_ENABLED") == "YES"
    if mode in ("demo", "live") and not armed:
        raise Halt("Trading requires TRADING_ENABLED=YES")
    base = os.getenv("KRAKEN_FUTURES_BASE", "https://futures.kraken.com")
    volume_path = os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "/data")
    # Use a fresh state database after confirming Kraken Futures Positions and
    # Open Orders are both empty. Keep the previous database untouched.
    # Always use Railway's actual mounted volume path.
    db_path = str(Path(volume_path) / "turtle_live_recovered.sqlite3")
    if mode == "live":
        if base != "https://futures.kraken.com":
            raise Halt("Live trading requires production Futures endpoint")
        mount = Path(volume_path)
        if not mount.is_absolute() or not mount.is_dir() or not os.path.ismount(str(mount)):
            raise Halt("Railway volume is missing; attach one and mount it at the runtime volume path")
    elif mode == "demo":
        if base != "https://demo-futures.kraken.com":
            raise Halt("Demo trading requires demo Futures endpoint")
        db_path = "turtle_demo.sqlite3"
    else:  # Preview never sends an order.
        db_path = ":memory:"
    key = os.getenv("KRAKEN_FUTURES_API_KEY", "").strip()
    secret = os.getenv("KRAKEN_FUTURES_API_SECRET", "").strip()
    if not key or not secret:
        raise Halt("Futures API key and secret are required")
    exchange = Exchange(key, secret, base)
    state = State(db_path)
    engine = Engine(exchange, state, armed, dec(os.getenv("MAX_LIVE_NOTIONAL_USD", "200")))
    log.info("TURTLE LIVE ENGINE mode=%s symbols=%s risk=%s max_notional=%s",
             mode, ",".join(SYMBOLS), RISK, engine.cap)
    last_status_hour = None
    while True:
        try:
            engine.cycle()
        except Halt:
            log.exception("TRADING HALTED: inspect Futures positions and orders before restart")
            raise
        current_hour = int(time.time() // 3600)
        if current_hour != last_status_hour:
            log.info("KRAKEN AUTH OK; market check complete; mode=%s", mode)
            last_status_hour = current_hour
        time.sleep(max(POLL, 30))


if __name__ == "__main__":
    main()
