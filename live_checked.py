#!/usr/bin/env python3
"""Kraken Futures BTC/ETH daily SMA(50) bot with risk-based position sizing.

New positions are sized for a $10 stop-risk budget, capped at 5% of FLEX
equity, and never exceed 3x equity notional. A reduce-only
exchange stop-market order is placed 2.5 x ATR(20) from the reference price.
At +30% unrealized return on the tracked initial margin, half the position is
closed reduce-only and the remainder receives a break-even stop.

Python 3.10+, standard library only. Read-only: python live_checked.py --check
Live trading: python live_checked.py --live
"""
from __future__ import annotations

import argparse
import base64
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
import fcntl
import hashlib
import hmac
import json
import logging
import math
import os
from pathlib import Path
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from typing import Any

SYMBOLS = tuple(dict.fromkeys(
    part.strip().upper()
    for part in os.getenv("SYMBOLS", "PF_XBTUSD,PF_ETHUSD").split(",")
    if part.strip()
))
LOT_SIZES = {"PF_XBTUSD": 0.0001, "PF_ETHUSD": 0.001}
# Kraken EEA linear perpetual contract specifications.
PRICE_TICKS = {"PF_XBTUSD": Decimal("1"), "PF_ETHUSD": Decimal("0.1")}
LEVERAGE = 3.0
RISK_PER_TRADE_USD = 10.0
MAX_RISK_PER_EQUITY_FRACTION = 0.05
# Reserve part of the risk budget for entry/stop price movement and fees.
RISK_EXECUTION_BUFFER = 0.10
PARTIAL_TP_MARGIN_RETURN = 0.30
PARTIAL_TP_CLOSE_FRACTION = 0.50
SMA_DAYS = 50
ATR_DAYS = 20
STOP_ATR_MULTIPLIER = 2.5
POLL_SECONDS = 60
CONFIRM_TIMEOUT = int(os.getenv("ORDER_CONFIRM_TIMEOUT", "60"))
ADOPT_EXISTING_POSITION = os.getenv("ADOPT_EXISTING_POSITION", "false").lower() in {"1", "true", "yes"}
DB_PATH = Path(os.getenv("DB_PATH", "btc_sma50_live.sqlite3"))
BASE_URL = os.getenv("KRAKEN_FUTURES_BASE_URL", "https://futures.kraken.com").rstrip("/")
CHARTS_URL = "https://futures.kraken.com/api/charts/v1"
API_KEY = os.getenv("KRAKEN_FUTURES_API_KEY", "")
API_SECRET = os.getenv("KRAKEN_FUTURES_API_SECRET", "")
# Kraken's request URL includes /derivatives, but the v3 signature path starts /api/v3.
API_URL_ROOT = "/derivatives/api/v3"
API_SIGN_ROOT = "/api/v3"

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(),
                    format="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%SZ")
log = logging.getLogger("btc-sma50-live")


class KrakenError(RuntimeError):
    pass


class ProtectiveStopFailure(KrakenError):
    """Entry was confirmed closed after the exchange rejected its stop."""


def align_stop_to_tick(symbol: str, price: float, position_side: int) -> Decimal:
    """Round a protective stop to a valid tick, away from the market."""
    tick = PRICE_TICKS.get(symbol)
    if tick is None:
        raise KrakenError(f"No verified price tick configured for {symbol}; refusing to place a stop")
    rounding = ROUND_FLOOR if position_side > 0 else ROUND_CEILING
    ticks = (Decimal(str(price)) / tick).to_integral_value(rounding=rounding)
    aligned = ticks * tick
    if aligned <= 0:
        raise KrakenError("Calculated protective stop is invalid after tick alignment")
    return aligned


def public_json(url: str) -> dict[str, Any]:
    req = urllib.request.Request(url, headers={"User-Agent": "btc-sma50-live/1.0", "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            payload = json.load(response)
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise KrakenError(f"Public Kraken request failed: {exc}") from exc
    if not isinstance(payload, dict):
        raise KrakenError("Kraken returned an unexpected public response")
    return payload


def get_daily(symbol: str) -> tuple[datetime, float, float, float]:
    end = int(time.time())
    query = urllib.parse.urlencode({"from": end - 200 * 86400, "to": end, "count": 250})
    payload = public_json(f"{CHARTS_URL}/trade/{urllib.parse.quote(symbol)}/1d?{query}")
    now = datetime.now(timezone.utc)
    completed: dict[datetime, tuple[float, float, float]] = {}
    for row in payload.get("candles", []):
        try:
            ts = datetime.fromtimestamp(float(row["time"]) / 1000, tz=timezone.utc)
            high, low, close = float(row["high"]), float(row["low"]), float(row["close"])
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
        if min(high, low, close) > 0 and high >= low and ts + timedelta(days=1) <= now:
            completed[ts] = (high, low, close)
    candles = sorted(completed.items())
    needed = max(SMA_DAYS, ATR_DAYS + 1)
    if len(candles) < needed:
        raise KrakenError(f"Need at least {needed} completed daily candles; got {len(candles)}")
    recent_sma = candles[-SMA_DAYS:]
    candle_ts, close = recent_sma[-1][0], recent_sma[-1][1][2]
    sma = sum(c[2] for _, c in recent_sma) / SMA_DAYS
    recent = candles[-(ATR_DAYS + 1):]
    trs = []
    for i in range(1, len(recent)):
        high, low, _ = recent[i][1]
        prev_close = recent[i - 1][1][2]
        trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    atr = sum(trs) / ATR_DAYS
    if atr <= 0:
        raise KrakenError("Invalid ATR(20); refusing to calculate a stop")
    return candle_ts, close, sma, atr


class KrakenFutures:
    def __init__(self, key: str, secret: str):
        if not key or not secret:
            raise KrakenError("Set KRAKEN_FUTURES_API_KEY and KRAKEN_FUTURES_API_SECRET")
        try:
            self.secret = base64.b64decode(secret, validate=True)
        except Exception as exc:
            raise KrakenError("KRAKEN_FUTURES_API_SECRET must be Kraken's base64-encoded API secret") from exc
        self.key = key

    def _signed(self, method: str, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        # V3 requires the exact URL-encoded postData (spaces as %20), then
        # SHA256(postData + nonce + /api/v3/endpoint), raw digest into HMAC-SHA512.
        params = params or {}
        encoded = urllib.parse.urlencode(params, quote_via=urllib.parse.quote, safe="")
        nonce = str(int(time.time() * 1000))
        sign_path = f"{API_SIGN_ROOT}/{endpoint.lstrip('/')}"
        url_path = f"{API_URL_ROOT}/{endpoint.lstrip('/')}"
        digest = hashlib.sha256((encoded + nonce + sign_path).encode("utf-8")).digest()
        authent = base64.b64encode(hmac.new(self.secret, digest, hashlib.sha512).digest()).decode("ascii")
        headers = {"APIKey": self.key, "Authent": authent, "Nonce": nonce,
                   "Accept": "application/json", "User-Agent": "btc-sma50-live/1.0"}
        if method == "GET":
            url = f"{BASE_URL}{url_path}" + (f"?{encoded}" if encoded else "")
            data = None
        elif method == "POST":
            url = f"{BASE_URL}{url_path}"
            data = encoded.encode("utf-8")
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        else:
            raise ValueError(f"Unsupported HTTP method {method}")
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                payload = json.load(response)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")[:500]
            raise KrakenError(f"Kraken HTTP {exc.code}: {body}") from exc
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise KrakenError(f"Kraken request failed; reconcile exchange state before retry: {exc}") from exc
        if not isinstance(payload, dict):
            raise KrakenError("Kraken returned an unexpected private response")
        if payload.get("result") == "error" or payload.get("error"):
            raise KrakenError(f"Kraken rejected request: {payload.get('error', payload)}")
        return payload

    def open_positions_all(self) -> list[dict[str, Any]]:
        positions = self._signed("GET", "openpositions").get("openPositions")
        if not isinstance(positions, list):
            raise KrakenError("Unexpected openpositions response; refusing to trade")
        return [p for p in positions if abs(float(p.get("size", 0))) > 0]

    def open_positions(self, symbol: str) -> list[dict[str, Any]]:
        return [p for p in self.open_positions_all()
                if str(p.get("symbol", "")).upper() == symbol]

    def open_orders(self, symbol: str) -> list[dict[str, Any]]:
        orders = self._signed("GET", "openorders").get("openOrders")
        if not isinstance(orders, list):
            raise KrakenError("Unexpected openorders response; refusing to clear a pending intent")
        return [o for o in orders if str(o.get("symbol", o.get("instrument", ""))).upper() == symbol]

    def account_equity(self) -> float:
        accounts = self._signed("GET", "accounts").get("accounts")
        if not isinstance(accounts, dict):
            raise KrakenError("Unexpected accounts response; refusing to guess equity")
        flex = accounts.get("flex")
        if isinstance(flex, dict):
            for name in ("marginEquity", "portfolioValue", "balanceValue"):
                value = flex.get(name)
                if value is not None and float(value) > 0:
                    return float(value)
        raise KrakenError("No positive USD FLEX margin equity returned")

    def account_initial_margin(self) -> float:
        accounts = self._signed("GET", "accounts").get("accounts")
        flex = accounts.get("flex") if isinstance(accounts, dict) else None
        if not isinstance(flex, dict):
            raise KrakenError("No FLEX account data returned; cannot measure position margin")
        for name in ("initialMarginWithoutOrders", "initial_margin_without_orders",
                     "initialMargin", "initial_margin"):
            value = flex.get(name)
            if value is not None and float(value) >= 0:
                return float(value)
        raise KrakenError("FLEX initial margin is missing; cannot measure position margin")

    def open_position(self, symbol: str) -> dict[str, Any] | None:
        positions = self._signed("GET", "openpositions").get("openPositions")
        if not isinstance(positions, list):
            raise KrakenError("Unexpected openpositions response; refusing partial close")
        matches = [p for p in positions if str(p.get("symbol", "")).upper() == symbol
                   and abs(float(p.get("size", 0))) > 0]
        if len(matches) > 1:
            raise KrakenError(f"Found multiple open {symbol} positions; refusing partial close")
        return matches[0] if matches else None

    def ticker_price(self, symbol: str) -> float:
        for ticker in public_json(f"{BASE_URL}{API_URL_ROOT}/tickers").get("tickers", []):
            if str(ticker.get("symbol", "")).upper() == symbol:
                price = float(ticker.get("last", 0))
                if price > 0:
                    return price
        raise KrakenError(f"No current ticker price available for {symbol}")

    def send_market(self, symbol: str, side: str, size: float, reduce_only: bool, cli_id: str) -> dict[str, Any]:
        params: dict[str, Any] = {"orderType": "mkt", "symbol": symbol, "side": side,
                                  "size": f"{size:.8f}", "cliOrdId": cli_id}
        if reduce_only:
            params["reduceOnly"] = "true"
        payload = self._signed("POST", "sendorder", params)
        status = payload.get("sendStatus")
        if not isinstance(status, dict) or str(status.get("status", "")).lower() not in {"placed", "filled", "partiallyfilled", "partially_filled"}:
            raise KrakenError(f"Order was not confirmed accepted (status={status})")
        return payload

    def send_stop(self, symbol: str, side: str, size: float, stop_price: float, cli_id: str) -> str:
        aligned_price = align_stop_to_tick(symbol, stop_price, 1 if side == "sell" else -1)
        payload = self._signed("POST", "sendorder", {"orderType": "stp", "symbol": symbol, "side": side,
            "size": f"{size:.8f}", "stopPrice": format(aligned_price, "f"), "triggerSignal": "mark",
            "reduceOnly": "true", "cliOrdId": cli_id})
        status = payload.get("sendStatus")
        accepted = {"placed", "filled", "partiallyfilled", "partially_filled"}
        if not isinstance(status, dict) or str(status.get("status", "")).lower() not in accepted:
            raise KrakenError(f"Protective stop was not confirmed accepted: {status}")
        order_id = status.get("order_id")
        if not order_id:
            for event in status.get("orderEvents", []):
                order = event.get("order", {}) if isinstance(event, dict) else {}
                order_id = order.get("orderId") or order.get("order_id")
                if order_id:
                    break
        if not order_id:
            raise KrakenError("Protective stop response had no order ID; inspect Kraken")
        return str(order_id)

    def cancel_order(self, order_id: str) -> None:
        status = self._signed("POST", "cancelorder", {"order_id": order_id}).get("cancelStatus", {})
        if str(status.get("status", "")).lower() not in {"cancelled", "alreadycancelled", "notfound"}:
            raise KrakenError(f"Could not confirm protective stop cancellation: {status}")


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as db:
        for table in ("checkpoints", "pending"):
            exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
            if exists and "symbol" not in {r[1] for r in db.execute(f"PRAGMA table_info({table})")}:
                db.execute(f"ALTER TABLE {table} RENAME TO {table}_single_market_legacy")
        db.execute("CREATE TABLE IF NOT EXISTS checkpoints (symbol TEXT PRIMARY KEY, candle TEXT NOT NULL, signal INTEGER NOT NULL, updated_at TEXT NOT NULL)")
        db.execute("CREATE TABLE IF NOT EXISTS pending (symbol TEXT PRIMARY KEY, candle TEXT NOT NULL, signal INTEGER NOT NULL, created_at TEXT NOT NULL)")
        db.execute("CREATE TABLE IF NOT EXISTS protective_stops (symbol TEXT PRIMARY KEY, order_id TEXT NOT NULL, stop_price REAL NOT NULL)")
        db.execute("CREATE TABLE IF NOT EXISTS blocked_signals (symbol TEXT NOT NULL, candle TEXT NOT NULL, reason TEXT NOT NULL, PRIMARY KEY(symbol,candle))")
        db.execute("CREATE TABLE IF NOT EXISTS partial_take_profits (symbol TEXT PRIMARY KEY, side INTEGER NOT NULL, margin_usd REAL NOT NULL, triggered INTEGER NOT NULL DEFAULT 0)")


def last_checkpoint(symbol: str) -> str:
    with sqlite3.connect(DB_PATH) as db:
        row = db.execute("SELECT candle FROM checkpoints WHERE symbol=?", (symbol,)).fetchone()
    return row[0] if row else ""


def mark_processed(symbol: str, candle: str, signal: int) -> None:
    with sqlite3.connect(DB_PATH) as db:
        db.execute("INSERT OR REPLACE INTO checkpoints VALUES(?,?,?,?)", (symbol, candle, signal, datetime.now(timezone.utc).isoformat()))


def pending_intent(symbol: str) -> tuple[str, int] | None:
    with sqlite3.connect(DB_PATH) as db:
        row = db.execute("SELECT candle,signal FROM pending WHERE symbol=?", (symbol,)).fetchone()
    return (row[0], row[1]) if row else None


def set_pending(symbol: str, candle: str, signal: int) -> None:
    with sqlite3.connect(DB_PATH) as db:
        db.execute("INSERT OR REPLACE INTO pending VALUES(?,?,?,?)", (symbol, candle, signal, datetime.now(timezone.utc).isoformat()))


def clear_pending(symbol: str) -> None:
    with sqlite3.connect(DB_PATH) as db:
        db.execute("DELETE FROM pending WHERE symbol=?", (symbol,))


def save_partial_tp(symbol: str, side: int, margin_usd: float, triggered: bool = False) -> None:
    with sqlite3.connect(DB_PATH) as db:
        db.execute("INSERT OR REPLACE INTO partial_take_profits VALUES(?,?,?,?)",
                   (symbol, side, margin_usd, int(triggered)))


def get_partial_tp(symbol: str) -> tuple[int, float, bool] | None:
    with sqlite3.connect(DB_PATH) as db:
        row = db.execute("SELECT side,margin_usd,triggered FROM partial_take_profits WHERE symbol=?",
                          (symbol,)).fetchone()
    return (int(row[0]), float(row[1]), bool(row[2])) if row else None


def clear_partial_tp(symbol: str) -> None:
    with sqlite3.connect(DB_PATH) as db:
        db.execute("DELETE FROM partial_take_profits WHERE symbol=?", (symbol,))


def block_signal(symbol: str, candle: str, reason: str) -> None:
    with sqlite3.connect(DB_PATH) as db:
        db.execute("INSERT OR REPLACE INTO blocked_signals VALUES(?,?,?)", (symbol, candle, reason[:500]))


def signal_is_blocked(symbol: str, candle: str) -> str:
    with sqlite3.connect(DB_PATH) as db:
        row = db.execute("SELECT reason FROM blocked_signals WHERE symbol=? AND candle=?", (symbol, candle)).fetchone()
    return row[0] if row else ""


def get_stop(symbol: str) -> tuple[str, float] | None:
    with sqlite3.connect(DB_PATH) as db:
        row = db.execute("SELECT order_id,stop_price FROM protective_stops WHERE symbol=?", (symbol,)).fetchone()
    return (row[0], float(row[1])) if row else None


def forget_stop(symbol: str) -> None:
    with sqlite3.connect(DB_PATH) as db:
        db.execute("DELETE FROM protective_stops WHERE symbol=?", (symbol,))


def save_stop(symbol: str, order_id: str, price: float) -> None:
    with sqlite3.connect(DB_PATH) as db:
        db.execute("INSERT OR REPLACE INTO protective_stops VALUES(?,?,?)", (symbol, order_id, price))


def ensure_stop(client: KrakenFutures, symbol: str, side: int, qty: float, atr: float,
                planned_stop: float | None = None) -> tuple[str, float]:
    if stop := get_stop(symbol):
        return stop
    if planned_stop is None:
        reference = client.ticker_price(symbol)
        raw_price = reference - STOP_ATR_MULTIPLIER * atr if side > 0 else reference + STOP_ATR_MULTIPLIER * atr
        price = align_stop_to_tick(symbol, raw_price, side)
    else:
        price = Decimal(str(planned_stop))
    order_id = client.send_stop(symbol, "sell" if side > 0 else "buy", qty, price, f"sma50s{uuid.uuid4().hex[:20]}")
    save_stop(symbol, order_id, float(price))
    return order_id, float(price)


def cancel_stop(client: KrakenFutures, symbol: str) -> None:
    stop = get_stop(symbol)
    if stop:
        client.cancel_order(stop[0])
        forget_stop(symbol)


def position_state(client: KrakenFutures, symbol: str) -> tuple[int, float]:
    positions = client.open_positions(symbol)
    if len(positions) > 1:
        raise KrakenError(f"Found multiple open {symbol} positions; refusing to assume state")
    if not positions:
        return 0, 0.0
    p = positions[0]
    side = str(p.get("side", "")).lower()
    if side not in {"long", "short"}:
        raise KrakenError(f"Unrecognized position side {side!r}")
    return (1 if side == "long" else -1), abs(float(p["size"]))


def confirm_position_size(client: KrakenFutures, symbol: str, expected_side: int,
                          expected_qty: float, timeout: int) -> tuple[int, float]:
    deadline = time.monotonic() + timeout
    last = (0, 0.0)
    while time.monotonic() <= deadline:
        last = position_state(client, symbol)
        if last[0] == expected_side and math.isclose(last[1], expected_qty,
                                                     rel_tol=1e-5, abs_tol=LOT_SIZES[symbol] / 2):
            return last
        if last[0] not in (0, expected_side):
            raise KrakenError(f"Exchange position changed side during partial close: {last}")
        time.sleep(2)
    raise KrakenError(f"Partial close not confirmed in {timeout}s; inspect exchange (state={last})")


def floor_lot(qty: float, lot: float) -> float:
    return math.floor((qty + 1e-12) / lot) * lot


def size_for_risk(equity: float, price: float, atr: float, side: int, symbol: str) -> tuple[float, float, float, float]:
    """Return quantity, aligned stop, modeled stop loss, and risk budget."""
    lot = LOT_SIZES.get(symbol)
    if lot is None or equity <= 0 or price <= 0 or atr <= 0 or side not in (-1, 1):
        raise KrakenError("Invalid equity, price, ATR, side, or unconfigured lot size; refusing to trade")
    raw_stop = price - STOP_ATR_MULTIPLIER * atr if side > 0 else price + STOP_ATR_MULTIPLIER * atr
    stop_price = float(align_stop_to_tick(symbol, raw_stop, side))
    distance = abs(price - stop_price)
    if distance <= 0:
        raise KrakenError("Stop distance is zero; refusing to trade")
    risk_budget = min(RISK_PER_TRADE_USD, equity * MAX_RISK_PER_EQUITY_FRACTION)
    padded_distance = distance * (1.0 + RISK_EXECUTION_BUFFER)
    risk_qty = risk_budget / padded_distance
    leverage_qty = equity * LEVERAGE / price
    qty = floor_lot(min(risk_qty, leverage_qty), lot)
    if qty < lot:
        raise KrakenError(
            f"Risk-sized quantity is below lot size {lot:g}; no order sent "
            f"(risk budget=${risk_budget:.2f}, stop distance=${distance:.2f})"
        )
    modeled_loss = qty * distance
    if modeled_loss > risk_budget + 1e-9:
        raise KrakenError("Rounded quantity exceeds the configured stop-risk budget; refusing to trade")
    return qty, stop_price, modeled_loss, risk_budget


def confirm_side(client: KrakenFutures, symbol: str, expected: int, timeout: int) -> tuple[int, float]:
    deadline = time.monotonic() + timeout
    last = (0, 0.0)
    while time.monotonic() <= deadline:
        last = position_state(client, symbol)
        if last[0] == expected:
            return last
        if last[0] not in (0, expected):
            raise KrakenError(f"Exchange position is {last[0]}, expected {expected}; inspect")
        time.sleep(2)
    raise KrakenError(f"Position change not confirmed in {timeout}s; no retry sent (state={last})")


def execute_signal(client: KrakenFutures, symbol: str, target: int, atr: float) -> dict[str, Any]:
    current, current_qty = position_state(client, symbol)
    if current == target:
        stop_id, stop_price = ensure_stop(client, symbol, current, current_qty, atr)
        return {"action": "hold", "side": current, "qty": current_qty, "stop": stop_price, "stop_id": stop_id}
    if current:
        client.send_market(symbol, "sell" if current > 0 else "buy", current_qty, True, f"sma50c{uuid.uuid4().hex[:20]}")
        confirm_side(client, symbol, 0, CONFIRM_TIMEOUT)
    clear_partial_tp(symbol)
    cancel_stop(client, symbol)
    price = client.ticker_price(symbol)
    equity = client.account_equity()
    qty, planned_stop, modeled_loss, risk_budget = size_for_risk(equity, price, atr, target, symbol)
    log.info("%s risk sizing: equity=$%.2f budget=$%.2f qty=%.8f planned_stop=%.2f modeled_stop_loss=$%.2f notional=$%.2f",
             symbol, equity, risk_budget, qty, planned_stop, modeled_loss, qty * price)
    margin_before = client.account_initial_margin()
    client.send_market(symbol, "buy" if target > 0 else "sell", qty, False, f"sma50e{uuid.uuid4().hex[:20]}")
    side, actual_qty = confirm_side(client, symbol, target, CONFIRM_TIMEOUT)
    try:
        stop_id, stop_price = ensure_stop(client, symbol, side, actual_qty, atr, planned_stop)
    except Exception as exc:
        log.critical("Protective stop failed; attempting reduce-only emergency close: %s", exc)
        try:
            client.send_market(symbol, "sell" if side > 0 else "buy", actual_qty, True, f"sma50x{uuid.uuid4().hex[:20]}")
            confirm_side(client, symbol, 0, CONFIRM_TIMEOUT)
        except Exception as close_exc:
            raise KrakenError(f"UNPROTECTED POSITION MAY REMAIN OPEN; inspect Kraken immediately: {close_exc}") from exc
        raise ProtectiveStopFailure(f"Entry emergency-closed because protective stop was not confirmed: {exc}") from exc
    try:
        margin_after = client.account_initial_margin()
        position_margin = max(0.0, margin_after - margin_before)
    except Exception as exc:
        position_margin = 0.0
        log.warning("%s partial take-profit disabled: could not measure position margin: %s", symbol, exc)
    if position_margin > 0:
        save_partial_tp(symbol, side, position_margin)
        log.info("%s partial take-profit armed: close %.0f%% at +%.0f%% of tracked position margin ($%.2f)",
                 symbol, PARTIAL_TP_CLOSE_FRACTION * 100, PARTIAL_TP_MARGIN_RETURN * 100,
                 position_margin)
    else:
        log.warning("%s partial take-profit disabled: Kraken margin increase could not be measured", symbol)
    return {"action": "reversed" if current else "opened", "side": side, "qty": actual_qty,
            "stop": stop_price, "stop_id": stop_id, "risk_budget": risk_budget,
            "modeled_loss": modeled_loss}


def manage_partial_take_profit(client: KrakenFutures, symbol: str, atr: float) -> None:
    state = get_partial_tp(symbol)
    if not state and last_checkpoint(symbol):
        positions = client.open_positions_all()
        if len(positions) == 1 and str(positions[0].get("symbol", "")).upper() == symbol:
            # Recover an existing single-position campaign after upgrading the
            # bot. With other positions present, account margin is not per-market.
            try:
                recovered_margin = client.account_initial_margin()
            except Exception as exc:
                log.warning("%s partial take-profit not armed for existing position: margin unavailable: %s",
                            symbol, exc)
                return
            recovered_side = 1 if str(positions[0].get("side", "")).lower() == "long" else -1
            if recovered_margin > 0:
                save_partial_tp(symbol, recovered_side, recovered_margin)
                state = (recovered_side, recovered_margin, False)
                log.info("%s partial take-profit armed for existing position using its account margin ($%.2f)",
                         symbol, recovered_margin)
    if not state:
        return
    recorded_side, margin_usd, already_triggered = state
    if already_triggered:
        return
    current_side, current_qty = position_state(client, symbol)
    if current_side == 0:
        clear_partial_tp(symbol)
        return
    if current_side != recorded_side:
        clear_partial_tp(symbol)
        return
    position = client.open_position(symbol)
    if not position:
        return
    try:
        entry_price = float(position["price"])
        unrealized = float(position["unrealizedPnl"])
        pnl_currency = str(position.get("pnlCurrency", "")).upper()
    except (KeyError, TypeError, ValueError) as exc:
        raise KrakenError("Kraken position response lacks entry price or unrealized PnL; partial close skipped") from exc
    if pnl_currency in {"BTC", "XBT"} or (not pnl_currency and symbol == "PF_XBTUSD"):
        unrealized *= client.ticker_price(symbol)
    elif pnl_currency not in {"", "USD", "USDT"}:
        raise KrakenError(f"Unsupported PnL currency {pnl_currency}; partial close skipped")
    target_profit = margin_usd * PARTIAL_TP_MARGIN_RETURN
    if unrealized < target_profit:
        return
    close_qty = floor_lot(current_qty * PARTIAL_TP_CLOSE_FRACTION, LOT_SIZES[symbol])
    remaining_qty = floor_lot(current_qty - close_qty, LOT_SIZES[symbol])
    if close_qty < LOT_SIZES[symbol] or remaining_qty < LOT_SIZES[symbol]:
        log.warning("%s partial take-profit threshold reached, but size %.8f cannot be split into two valid lots",
                    symbol, current_qty)
        save_partial_tp(symbol, recorded_side, margin_usd, True)
        return
    log.info("%s partial take-profit triggered: unrealized PnL=$%.2f >= $%.2f; closing %.8f of %.8f",
             symbol, unrealized, target_profit, close_qty, current_qty)
    client.send_market(symbol, "sell" if current_side > 0 else "buy", close_qty, True,
                       f"sma50p{uuid.uuid4().hex[:20]}")
    confirm_position_size(client, symbol, current_side, remaining_qty, CONFIRM_TIMEOUT)
    old_stop = get_stop(symbol)
    try:
        stop_id = client.send_stop(symbol, "sell" if current_side > 0 else "buy", remaining_qty,
                                   entry_price, f"sma50b{uuid.uuid4().hex[:20]}")
    except Exception as exc:
        log.critical("%s break-even stop failed after partial close; attempting to close remainder: %s", symbol, exc)
        try:
            client.send_market(symbol, "sell" if current_side > 0 else "buy", remaining_qty, True,
                               f"sma50x{uuid.uuid4().hex[:20]}")
            confirm_side(client, symbol, 0, CONFIRM_TIMEOUT)
            cancel_stop(client, symbol)
            clear_partial_tp(symbol)
        except Exception as close_exc:
            raise KrakenError(f"UNPROTECTED REMAINDER MAY BE OPEN; inspect Kraken immediately: {close_exc}") from exc
        raise KrakenError("Remainder emergency-closed because break-even stop was not confirmed") from exc
    save_stop(symbol, stop_id, entry_price)
    if old_stop and old_stop[0] != stop_id:
        try:
            client.cancel_order(old_stop[0])
        except Exception as exc:
            log.error("%s new break-even stop is active, but old stop cancellation was not confirmed: %s",
                      symbol, exc)
    save_partial_tp(symbol, current_side, margin_usd, True)
    log.info("%s partial profit realized; remainder %.8f protected at break-even %.2f",
             symbol, remaining_qty, entry_price)


@contextmanager
def single_instance():
    lock_path = DB_PATH.with_suffix(DB_PATH.suffix + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "w") as f:
        try:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("Another instance is already using this database") from exc
        yield


def run_once(client: KrakenFutures, symbol: str) -> None:
    candle_ts, close, sma, atr = get_daily(symbol)
    candle = candle_ts.isoformat()
    target = 1 if close > sma else -1
    actual_side, actual_qty = position_state(client, symbol)
    pending = pending_intent(symbol)
    if pending:
        pending_candle, pending_signal = pending
        if actual_side == pending_signal:
            mark_processed(symbol, pending_candle, pending_signal)
            clear_pending(symbol)
        elif actual_side == 0:
            # A flat account is not enough to recover safely: first ensure no
            # resting order for this market can still open/change exposure.
            resting = client.open_orders(symbol)
            if resting:
                raise KrakenError(f"Pending intent {pending_candle} remains and {len(resting)} {symbol} order(s) are open; inspect/cancel them manually")
            clear_pending(symbol)
            block_signal(symbol, pending_candle, "Recovered flat exchange state after an interrupted order; skipped this candle to avoid duplicate entry")
            log.warning("%s recovered flat pending intent for candle=%s; no open orders; candle blocked", symbol, pending_candle)
        else:
            raise KrakenError(f"Unresolved order intent for {pending_candle}; exchange side={actual_side}; no new order sent")
    blocked_reason = signal_is_blocked(symbol, candle)
    if blocked_reason:
        log.warning("%s candle=%s remains blocked after prior order/stop failure: %s", symbol, candle, blocked_reason)
        return
    processed = last_checkpoint(symbol)
    if actual_side and not processed and not ADOPT_EXISTING_POSITION:
        raise KrakenError("Found an existing position without a bot checkpoint; close manually or set ADOPT_EXISTING_POSITION=true")
    if processed == candle:
        if actual_side != target:
            raise KrakenError("Checkpointed candle disagrees with exchange position; inspect manually")
        order_id, stop_price = ensure_stop(client, symbol, actual_side, actual_qty, atr)
        manage_partial_take_profit(client, symbol, atr)
        actual_side, actual_qty = position_state(client, symbol)
        if actual_side:
            current_stop = get_stop(symbol)
            if current_stop:
                order_id, stop_price = current_stop
        log.info("%s candle=%s signal=%s qty=%.8f protective_stop=%.2f stop_id=%s checkpoint=duplicate",
                 symbol, candle, "LONG" if target > 0 else "SHORT", actual_qty, stop_price, order_id)
        return
    set_pending(symbol, candle, target)
    try:
        result = execute_signal(client, symbol, target, atr)
    except ProtectiveStopFailure as exc:
        # execute_signal raises this only after confirming the emergency close.
        clear_pending(symbol)
        block_signal(symbol, candle, str(exc))
        raise
    final_side, final_qty = position_state(client, symbol)
    if final_side != target:
        raise KrakenError("Final exchange position does not match signal; checkpoint not advanced")
    mark_processed(symbol, candle, target)
    clear_pending(symbol)
    log.info("%s candle=%s close=%.6g sma=%.6g signal=%s action=%s exchange=%s qty=%.8f stop=%.2f order=%s",
             symbol, candle, close, sma, "LONG" if target > 0 else "SHORT", result["action"],
             "LONG" if final_side > 0 else "SHORT", final_qty, result["stop"], result["stop_id"])


def preflight(client: KrakenFutures, symbol: str) -> None:
    candle, close, sma, atr = get_daily(symbol)
    target = 1 if close > sma else -1
    equity = client.account_equity()
    price = client.ticker_price(symbol)
    qty, planned_stop, modeled_loss, risk_budget = size_for_risk(equity, price, atr, target, symbol)
    side, size = position_state(client, symbol)
    print(f"Read-only preflight: {symbol} | candle={candle.isoformat()} | close={close:.2f} | SMA{SMA_DAYS}={sma:.2f}")
    print(f"Signal={'LONG' if target > 0 else 'SHORT'} | FLEX equity=${equity:.2f} | risk budget=${risk_budget:.2f} | planned size={qty:.8f} | notional=${qty * price:.2f}")
    print(f"Planned stop={planned_stop:.2f} | modeled gross loss at stop=${modeled_loss:.2f} (10% execution/fee buffer used for sizing)")
    print(f"Partial take-profit: close {PARTIAL_TP_CLOSE_FRACTION:.0%} at +{PARTIAL_TP_MARGIN_RETURN:.0%} of tracked initial margin; protect remainder at entry")
    print(f"Current position={('LONG' if side > 0 else 'SHORT') if side else 'FLAT'} | size={size:.8f} | orders sent=0")
    print(f"Stop distance={STOP_ATR_MULTIPLIER:.1f} x ATR(20) = {STOP_ATR_MULTIPLIER * atr:.2f} USD | notional ceiling={LEVERAGE:.1f}x equity")


def main() -> None:
    parser = argparse.ArgumentParser(description="BTC/ETH daily SMA50 / 3x Kraken Futures bot")
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--check", action="store_true", help="read-only connectivity and sizing check")
    modes.add_argument("--live", action="store_true", help="enable live trading")
    args = parser.parse_args()
    unsupported_symbols = [symbol for symbol in SYMBOLS if symbol not in LOT_SIZES or symbol not in PRICE_TICKS]
    if unsupported_symbols:
        raise SystemExit(f"Unsupported SYMBOLS value(s): {', '.join(unsupported_symbols)}")
    if LEVERAGE <= 0 or RISK_PER_TRADE_USD <= 0 or not 0 < MAX_RISK_PER_EQUITY_FRACTION <= 0.05 or RISK_EXECUTION_BUFFER < 0 or SMA_DAYS < 2 or POLL_SECONDS < 10 or not SYMBOLS:
        raise SystemExit("Leverage, SMA_DAYS, POLL_SECONDS and SYMBOLS must be valid")
    client = KrakenFutures(API_KEY, API_SECRET)
    init_db()
    if not args.live:
        for symbol in SYMBOLS:
            preflight(client, symbol)
        return
    # Authenticate with a private read before announcing live mode or entering the loop.
    # If the signature/key is wrong, startup stops here and sends no orders.
    equity = client.account_equity()
    log.info("Kraken Futures private authentication passed (FLEX equity available: %.2f USD)", equity)
    log.info("LIVE trading enabled: Kraken Futures %d symbol(s)=%s | SMA=%d daily | stop-risk per trade=$%.2f (max %.1f%% equity; up to $%.2f across all symbols) | notional ceiling=%.2fx equity per symbol | reduce-only stop=%.1fx ATR(%d) | partial close=%.0f%% at +%.0f%% margin return",
             len(SYMBOLS), ",".join(SYMBOLS), SMA_DAYS, RISK_PER_TRADE_USD, MAX_RISK_PER_EQUITY_FRACTION * 100,
             len(SYMBOLS) * RISK_PER_TRADE_USD, LEVERAGE, STOP_ATR_MULTIPLIER, ATR_DAYS, PARTIAL_TP_CLOSE_FRACTION * 100,
             PARTIAL_TP_MARGIN_RETURN * 100)
    with single_instance():
        while True:
            for symbol in SYMBOLS:
                try:
                    run_once(client, symbol)
                except Exception as exc:
                    log.error("%s %s: %s", symbol, type(exc).__name__, exc)
            time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
