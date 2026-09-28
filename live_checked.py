#!/usr/bin/env python3
# PATCH: pending recovery + close-state fix verified 2026-09-28
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


class OrderRejected(Halt):
    """Kraken definitively rejected an order without accepting it."""


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
        rows = self.request("/orders/status", {"orderIds": order_id}, "POST").get("orders")
        if not isinstance(rows, list):
            raise Halt("Unexpected orders/status response")
        return rows

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
        status = result.get("status", "unknown")
        if status not in ("placed", "filled", "partiallyFilled"):
            error = "Order refused: " + str(status)
            # Kraken can report this explicit reduce-only rejection status
            # instead of the generic "rejected" value. Callers then reconcile
            # positions/orders before clearing their pending marker.
            if status in ("rejected", "wouldNotReducePosition"):
                raise OrderRejected(error)
            raise Halt(error)
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
            units INTEGER NOT NULL, fast INTEGER NOT NULL DEFAULT 0)""")
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

    def clear_pending(self, action):
        """Clear only the exact marker whose exchange outcome was reconciled."""
        try:
            self.db.execute("BEGIN IMMEDIATE")
            pending = self.pending()
            if pending is None or pending[0] != action:
                self.db.rollback()
                raise Halt("Pending exchange action changed during reconciliation")
            self.db.execute("DELETE FROM pending WHERE id=1 AND action=?", (action,))
            self.db.commit()
        except sqlite3.Error as exc:
            self.db.rollback()
            raise Halt("Could not clear reconciled pending exchange action") from exc

    def recover_entry(self, action, position, stop_id, entry, n, size, stop):
        """Atomically adopt an exchange-confirmed entry protected by its stop."""
        symbol = position["symbol"].upper()
        side = position["side"]
        try:
            self.db.execute("BEGIN IMMEDIATE")
            pending = self.pending()
            existing = self.db.execute(
                "SELECT 1 FROM positions WHERE symbol=?", (symbol,)).fetchone()
            if pending is None or pending[0] != action or existing:
                self.db.rollback()
                raise Halt("Entry recovery state changed: " + symbol)
            self.db.execute(
                "INSERT INTO positions VALUES (?,?,?,?,?,?,?,?,?,?)",
                (symbol, side, str(entry), str(n), str(size), str(stop),
                 json.dumps([{"id": stop_id, "size": str(size)}]),
                 str(size), 1, 0))
            self.db.execute("DELETE FROM pending WHERE id=1 AND action=?", (action,))
            self.db.commit()
        except sqlite3.Error as exc:
            self.db.rollback()
            raise Halt("Could not save recovered exchange position: " + symbol) from exc

    def complete_close(self, symbol, action, side, size):
        """Atomically forget a confirmed-closed position and its exact pending action."""
        try:
            self.db.execute("BEGIN IMMEDIATE")
            pending = self.pending()
            row = self.db.execute("SELECT side,size FROM positions WHERE symbol=?", (symbol,)).fetchone()
            # items() sets sqlite3.Row as the connection row factory, so
            # pending may be a Row rather than a tuple. Compare its value,
            # not its container type.
            if (pending is None or pending[0] != action or not row
                    or row[0] != side or dec(row[1]) != size):
                self.db.rollback()
                raise Halt("Database state changed during confirmed close: " + symbol)
            self.db.execute("DELETE FROM positions WHERE symbol=?", (symbol,))
            self.db.execute("DELETE FROM pending WHERE id=1 AND action=?", (action,))
            self.db.commit()
        except sqlite3.Error as exc:
            self.db.rollback()
            raise Halt("Could not commit confirmed close: " + symbol) from exc

    def save(self, p):
        self.db.execute("INSERT OR REPLACE INTO positions VALUES (?,?,?,?,?,?,?,?,?,?)",
                        tuple(p[k] for k in ("symbol", "side", "entry", "n", "size", "stop",
                                              "stops", "unit_size", "units", "fast")))
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
        pending = self.state.pending()
        if pending:
            action = pending[0]
            # An entry marker is recoverable only after the exchange itself
            # confirms twice that exact symbol is flat and has no open orders,
            # and local state has no saved position for it. This repairs older
            # runs that closed the entry but crashed before clearing the marker.
            # Other action types and inconsistent local state remain halted.
            if action.startswith("enter:"):
                symbol = action[len("enter:"):].upper()
                if symbol in SYMBOLS and symbol not in owned:
                    time.sleep(0.5)
                    positions, orders = self.ex.positions(), self.ex.orders()
                    matching_positions = [p for p in positions
                                          if p.get("symbol", "").upper() == symbol]
                    symbol_orders = [o for o in orders
                                     if o.get("symbol", "").upper() == symbol]
                    if not matching_positions and not symbol_orders:
                        # A failed/reconciled entry left no exchange exposure.
                        self.state.clear_pending(action)
                        log.warning("Recovered prior entry marker %s: two exchange checks confirm "
                                    "flat/no open orders", symbol)
                        pending = None
                    elif len(matching_positions) == 1 and len(symbol_orders) == 1:
                        # Recover only when the exchange confirms one position
                        # and exactly one full-size, reduce-only protective stop.
                        pos, stop_order = matching_positions[0], symbol_orders[0]
                        side = str(pos.get("side", "")).lower()
                        size = dec(pos.get("size", 0))
                        entry_raw = pos.get("price", pos.get("entryPrice"))
                        stop_raw = stop_order.get("stopPrice", stop_order.get("triggerPrice"))
                        stop_id = stop_order.get("order_id")
                        correct_side = "sell" if side == "long" else "buy" if side == "short" else ""
                        valid = (side in ("long", "short") and size > 0 and entry_raw is not None
                                 and stop_raw is not None and stop_id
                                 and stop_order.get("orderType") == "stp"
                                 and stop_order.get("reduceOnly") is True
                                 and str(stop_order.get("side", "")).lower() == correct_side
                                 and dec(stop_order.get("unfilledSize", 0)) >= size)
                        if valid:
                            entry, stop = dec(entry_raw), dec(stop_raw)
                            n = abs(stop - entry) / Decimal("2.5")
                            valid = n > 0 and ((side == "long" and stop < entry)
                                               or (side == "short" and stop > entry))
                        if valid:
                            self.state.recover_entry(action, pos, stop_id,
                                                     entry, n, size, stop)
                            owned = self.state.items()
                            log.warning("Recovered live entry %s %s size=%s using verified "
                                        "exchange stop %s at %s; review before relying on strategy state",
                                        symbol, side, size, stop_id, stop)
                            pending = None
            if pending:
                raise Halt(f"Pending action from prior run: {pending[0]!r}; exchange state is unresolved")
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
                if len(matches) != 1:
                    raise Halt("Missing protective stop: " + symbol)
                stop = matches[0]
                if (stop.get("orderType") != "stp" or stop.get("reduceOnly") is not True
                        or stop.get("symbol", "").upper() != symbol
                        or stop.get("side") != ("sell" if saved["side"] == "long" else "buy")
                        or dec(stop.get("unfilledSize", 0)) < dec(entry["size"])):
                    raise Halt("Invalid protective stop: " + symbol)
        if any(o.get("order_id") not in allowed_stops for o in orders):
            raise Halt("Unknown exchange order; no new orders")
        return by_symbol

    def stop_order(self, symbol, side, size, stop):
        status = self.ex.send(orderType="stp", symbol=symbol,
                              side="sell" if side == "long" else "buy",
                              size=clean_number(size), stopPrice=clean_number(stop),
                              triggerSignal="mark", reduceOnly="true", cliOrdId=str(uuid.uuid4()))
        if not status.get("order_id"):
            raise Halt("Stop ID missing")
        stop_id = status["order_id"]
        self.last_stop_order = (symbol, stop_id)
        # Query the exact order. It can trigger before it appears in openorders.
        for attempt in range(5):
            rows = self.ex.order_status(stop_id)
            matches = [row for row in rows
                       if (row.get("order") or {}).get("orderId") == stop_id]
            if len(matches) == 1:
                result = matches[0]
                order = result.get("order") or {}
                # Kraken Futures has returned both ENTERED_BOOK and
                # TRIGGER_PLACED for a resting trigger order. The latter is
                # the observed live response for valid stop orders; treating
                # it as failure would unnecessarily market-close every entry.
                # Some responses use null for filled while nothing has filled.
                remaining = (dec(order.get("quantity", 0))
                             - dec(order.get("filled") or 0))
                order_state = result.get("status")
                protective = (
                    order.get("type") == "TRIGGER_ORDER"
                    and order.get("symbol", "").upper() == symbol
                    and str(order.get("side", "")).lower() ==
                        ("sell" if side == "long" else "buy")
                    and order.get("reduceOnly") is True
                )
                if (protective and order_state in ("ENTERED_BOOK", "TRIGGER_PLACED")
                        and remaining >= size):
                    return stop_id
                log.warning("Stop status not protective %s: status=%s type=%s symbol=%s "
                            "side=%s reduceOnly=%s quantity=%s filled=%s expected=%s",
                            symbol, order_state, order.get("type"), order.get("symbol"),
                            order.get("side"), order.get("reduceOnly"),
                            order.get("quantity"), order.get("filled"), size)
                if order_state not in ("ENTERED_BOOK", "TRIGGERED", "EXECUTED", "FILLED"):
                    break
            elif attempt == 4:
                log.warning("Stop ID not visible in status lookup %s", symbol)
            if attempt < 4:
                time.sleep(0.35)
        raise Halt("Protective stop not confirmed on exchange: " + symbol)

    def emergency_close(self, symbol, side, size):
        # Stop creation may race a stop fill. Re-read immediately before sending
        # so a stale size does not produce a rejected reduce-only order.
        actual = next((p for p in self.ex.positions()
                       if p.get("symbol", "").upper() == symbol), None)
        if actual is None:
            log.critical("EMERGENCY CLOSE NOT NEEDED %s: exchange already flat", symbol)
            return
        if actual.get("side") != side:
            raise Halt("Emergency close side differs from exchange: " + symbol)
        actual_size = dec(actual.get("size", 0))
        if actual_size <= 0:
            raise Halt("Invalid exchange size during emergency close: " + symbol)
        try:
            self.ex.send(orderType="mkt", symbol=symbol,
                         side="sell" if side == "long" else "buy",
                         size=clean_number(actual_size), reduceOnly="true",
                         cliOrdId=str(uuid.uuid4()))
        except OrderRejected:
            # Kraken rejects reduce-only when the position was closed in the
            # brief interval between the position read and this request.
            for attempt in range(2):
                if any(p.get("symbol", "").upper() == symbol for p in self.ex.positions()):
                    raise
                if attempt == 0:
                    time.sleep(0.5)
            log.critical("EMERGENCY CLOSE %s: rejected, then exchange confirmed flat", symbol)
            return
        if any(p.get("symbol", "").upper() == symbol for p in self.ex.positions()):
            raise Halt("Emergency close incomplete; exchange position still open: " + symbol)
        log.critical("EMERGENCY CLOSE VERIFIED %s", symbol)

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
            try:
                self.ex.send(orderType="mkt", symbol=symbol,
                             side="buy" if side == "long" else "sell",
                             size=clean_number(size), reduceOnly="false",
                             cliOrdId=str(uuid.uuid4()))
            except OrderRejected as exc:
                # A definitive Kraken rejection cannot have opened this entry.
                # Confirm the symbol is flat and has no open orders before
                # removing the marker, otherwise preserve it and halt.
                for attempt in range(2):
                    if any(p.get("symbol", "").upper() == symbol
                           for p in self.ex.positions()):
                        raise Halt("Rejected entry but exchange position exists: " + symbol) from exc
                    if any(o.get("symbol", "").upper() == symbol
                           for o in self.ex.orders()):
                        raise Halt("Rejected entry but exchange order exists: " + symbol) from exc
                    if attempt == 0:
                        time.sleep(1)
                self.state.clear_pending("enter:" + symbol)
                log.error("LIVE ENTRY REJECTED %s; exchange confirmed flat/no orders; "
                          "pending marker cleared: %s", symbol, exc)
                return
            actual = next((p for p in self.ex.positions() if p["symbol"].upper() == symbol), None)
            if not actual or actual.get("side") != side or dec(actual["size"]) <= 0:
                raise Halt("Entry fill not confirmed: " + symbol)
            actual_size = dec(actual["size"])
            self.last_stop_order = None
            try:
                stop_id = self.stop_order(symbol, side, actual_size, stop)
            except Exception as exc:
                # A stop may have triggered immediately; check before closing.
                remaining = next((p for p in self.ex.positions()
                                  if p.get("symbol", "").upper() == symbol), None)
                if remaining:
                    self.emergency_close(symbol, side, dec(remaining["size"]))
                # A trigger may have filled before it appeared in openorders.
                # If it is still resting, cancel only the ID returned for this
                # entry. Keep the marker unless flatness and no remaining
                # symbol orders are both confirmed.
                stop_ref = getattr(self, "last_stop_order", None)
                if stop_ref and stop_ref[0] == symbol:
                    stop_id = stop_ref[1]
                    if any(o.get("order_id") == stop_id for o in self.ex.orders()):
                        self.ex.cancel(stop_id)
                for attempt in range(2):
                    if any(p.get("symbol", "").upper() == symbol for p in self.ex.positions()):
                        raise Halt("Protective stop failed; position remains: " + symbol) from exc
                    if attempt == 0:
                        time.sleep(0.5)
                if any(o.get("symbol", "").upper() == symbol for o in self.ex.orders()):
                    raise Halt("Protective stop failed; symbol still has open orders: " + symbol) from exc
                action = "enter:" + symbol
                try:
                    self.state.clear_pending(action)
                except Halt as reconcile_exc:
                    raise Halt("Protective stop failed; pending entry changed: " + symbol) from exc
                log.error("LIVE ENTRY CLOSED %s: stop was not confirmed (%s); exchange is flat, "
                          "symbol has no open orders, pending marker cleared; this bar stays consumed",
                          symbol, type(exc).__name__)
                return
            self.state.save(dict(symbol=symbol, side=side, entry=str(actual["price"]),
                                 n=str(n), size=str(actual_size), stop=str(stop),
                                 stops=json.dumps([{"id": stop_id, "size": str(actual_size)}]),
                                 unit_size=str(actual_size), units=1, fast=0))
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
        action = "add:" + symbol
        self.state.begin(action)
        try:
            self.ex.send(orderType="mkt", symbol=symbol, side="buy" if side == "long" else "sell",
                         size=clean_number(size), reduceOnly="false", cliOrdId=str(uuid.uuid4()))
        except OrderRejected as exc:
            # Clear an add marker only after two reads confirm the original
            # position and every pre-existing protective stop are unchanged.
            stop_entries = json.loads(saved["stops"])
            for attempt in range(2):
                positions, orders = self.ex.positions(), self.ex.orders()
                actual = next((p for p in positions
                               if p.get("symbol", "").upper() == symbol), None)
                if (actual is None or actual.get("side") != side
                        or dec(actual.get("size", 0)) != old_size):
                    raise Halt("Rejected add but position changed; pending marker preserved: "
                               + symbol) from exc
                symbol_orders = [o for o in orders
                                 if o.get("symbol", "").upper() == symbol]
                expected_ids = {entry["id"] for entry in stop_entries}
                if {o.get("order_id") for o in symbol_orders} != expected_ids:
                    raise Halt("Rejected add but symbol orders changed; pending marker preserved: "
                               + symbol) from exc
                for entry in stop_entries:
                    matches = [o for o in symbol_orders if o.get("order_id") == entry["id"]]
                    if (len(matches) != 1 or matches[0].get("orderType") != "stp"
                            or matches[0].get("reduceOnly") is not True
                            or matches[0].get("side") != ("sell" if side == "long" else "buy")
                            or dec(matches[0].get("unfilledSize", 0)) < dec(entry["size"])):
                        raise Halt("Rejected add but protective stop changed; pending marker preserved: "
                                   + symbol) from exc
                if attempt == 0:
                    time.sleep(0.5)
            self.state.clear_pending(action)
            log.error("LIVE ADD REJECTED %s; original position and stops verified twice; "
                      "pending marker cleared: %s", symbol, exc)
            return Decimal(0)
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
        saved.update(stops=json.dumps(attached), size=str(old_size + extra), units=saved["units"] + 1)
        self.state.save(saved)
        self.state.clear()
        log.info("LIVE ADD %s %s units=%s size=%s", symbol, side, saved["units"], extra)
        return extra

    def close(self, saved, reason):
        symbol, size, side = saved["symbol"], dec(saved["size"]), saved["side"]
        action = "close:" + symbol
        self.state.begin(action)

        def exchange_position():
            return next((p for p in self.ex.positions()
                         if p.get("symbol", "").upper() == symbol), None)

        def finish_if_flat():
            # Require two flat reads; a single read can race an exchange update.
            for attempt in range(2):
                position = exchange_position()
                if position:
                    return False
                if attempt == 0:
                    time.sleep(0.5)
            stop_ids = {entry["id"] for entry in json.loads(saved["stops"])}
            open_orders = self.ex.orders()
            for stop_id in stop_ids:
                if any(order.get("order_id") == stop_id for order in open_orders):
                    self.ex.cancel(stop_id)
            if exchange_position():
                raise Halt("Position reappeared during close reconciliation: " + symbol)
            if any(order.get("order_id") in stop_ids for order in self.ex.orders()):
                raise Halt("Protective stop still open after close: " + symbol)
            self.state.complete_close(symbol, action, side, size)
            return True

        # The strategy scan can take time; an exchange stop may close the
        # position after snapshot() and before this close request.
        actual = exchange_position()
        if actual is None:
            if finish_if_flat():
                log.info("LIVE CLOSE %s already flat reason=%s", symbol, reason)
                return
            raise Halt("Position state changed before close: " + symbol)
        if actual.get("side") != side or dec(actual.get("size", 0)) != size:
            raise Halt("Position side/size changed before close: " + symbol)

        try:
            self.ex.send(orderType="mkt", symbol=symbol,
                         side="sell" if side == "long" else "buy", size=clean_number(size),
                         reduceOnly="true", cliOrdId=str(uuid.uuid4()))
        except OrderRejected:
            # A rejected reduce-only close is recoverable only when the position
            # is now confirmed flat and all attached protective stops are gone.
            if finish_if_flat():
                log.info("LIVE CLOSE %s already flat after rejected reduce-only order reason=%s",
                         symbol, reason)
                return
            raise

        if exchange_position():
            raise Halt("Close incomplete; exchange position still open: " + symbol)
        if not finish_if_flat():
            raise Halt("Position state changed after close: " + symbol)
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
                # The channel exit is based on completed hourly bars, so it
                # must be evaluated once per newly completed bar, not against
                # the still-forming candle's price on every poll. Mark the bar
                # while the position is open too: if it closes, this same bar
                # cannot immediately trigger a new entry below.
                previous = self.state.bar(symbol)
                new_closed_bar = previous is None or previous < bar
                if new_closed_bar:
                    self.state.mark(symbol, bar)
                sign = 1 if saved["side"] == "long" else -1
                entry, n = dec(saved["entry"]), dec(saved["n"])
                fast = bool(saved["fast"]) or sign * (price - entry) >= 4 * n
                if new_closed_bar:
                    # Compare the just-closed bar with the preceding 5/10
                    # completed bars. The live price must not cause repeated
                    # D5/D10 closes during the same hour.
                    channel = done[-(6 if fast else 11):-1]
                    if len(channel) < (5 if fast else 10):
                        raise Halt("Not enough completed candles for channel exit: " + symbol)
                    exit_price = (min(dec(x["low"]) for x in channel) if sign == 1 else
                                  max(dec(x["high"]) for x in channel))
                    closed_price = dec(done[-1]["close"])
                    if sign * (closed_price - exit_price) <= 0:
                        self.close(saved, "D5" if fast else "D10")
                        continue
                if fast != bool(saved["fast"]):
                    saved["fast"] = 1
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
    db_path = os.getenv("LIVE_DB_PATH", "/data/turtle_live.sqlite3")
    if mode == "live":
        if base != "https://futures.kraken.com":
            raise Halt("Live trading requires production Futures endpoint")
        if not Path(db_path).is_absolute() or not Path(db_path).parent.is_dir():
            raise Halt("LIVE_DB_PATH must point to an existing persistent directory")
        if not os.path.ismount(str(Path(db_path).parent)):
            raise Halt("Attach a persistent Railway volume at the LIVE_DB_PATH directory")
    elif mode == "demo":
        if base != "https://demo-futures.kraken.com":
            raise Halt("Demo trading requires demo Futures endpoint")
        if db_path == "/data/turtle_live.sqlite3":
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
