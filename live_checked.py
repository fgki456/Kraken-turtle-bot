#!/usr/bin/env python3
"""Live Kraken Futures bot for the BTC daily SMA(50), 3x compounding strategy.

Strategy: hold BTC long when the last completed daily close is above SMA(50),
otherwise hold short. By default only PF_XBTUSD is enabled.

The strategy keeps one BTC futures position: long above SMA(50), short below.
Target notional is 3x the Futures account's current margin equity. It has no
price stop, matching the historical backtest logic; that backtest's reported
maximum drawdown was about 51.5%.

The bot sends real orders when valid Kraken Futures API credentials are set.
It never stores credentials, and it fails closed when credentials are absent.
Use an API key limited to Futures trading and account reads, with withdrawals
disabled. This script does not promise or target a specific annual return.

Environment:
  KRAKEN_FUTURES_API_KEY       required
  KRAKEN_FUTURES_API_SECRET    required (base64 secret from Kraken)
  Strategy is fixed at LEVERAGE=3, SMA_DAYS=50, POLL_SECONDS=60
  DB_PATH                      durable checkpoint database
  KRAKEN_FUTURES_BASE_URL      default https://futures.kraken.com
  ORDER_CONFIRM_TIMEOUT        seconds to wait for position confirmation
  ADOPT_EXISTING_POSITION      set true only if the bot may manage an existing
                               positions in the configured symbols (default false)

Requires Python 3.10+; no third-party Python packages are needed.
Run:     python live_checked.py
Read-only check: python live_checked.py --check
"""
from __future__ import annotations

import base64
import argparse
from datetime import datetime, timedelta, timezone
import fcntl
import hashlib
import hmac
import json
import logging
import math
import os
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any

DEFAULT_SYMBOLS = ("PF_XBTUSD",)
SYMBOLS = DEFAULT_SYMBOLS
# Kraken's current linear-contract minimum lots; quantities are in base units.
LOT_SIZES = {"PF_XBTUSD": 0.0001, "PF_ETHUSD": 0.001, "PF_SOLUSD": 0.01,
             "PF_XRPUSD": 1.0, "PF_DOGEUSD": 1.0, "PF_LINKUSD": 0.1,
             "PF_ADAUSD": 1.0, "PF_AVAXUSD": 0.01, "PF_1INCHUSD": 1.0,
             "PF_DOTUSD": 0.1}
LEVERAGE = 3.0
SMA_DAYS = 50
POLL_SECONDS = 60
CONFIRM_TIMEOUT = int(os.getenv("ORDER_CONFIRM_TIMEOUT", "60"))
ADOPT_EXISTING_POSITION = os.getenv("ADOPT_EXISTING_POSITION", "false").lower() in {"1", "true", "yes"}
DB_PATH = Path(os.getenv("DB_PATH", "btc_sma50_live.sqlite3"))
BASE_URL = os.getenv("KRAKEN_FUTURES_BASE_URL", "https://futures.kraken.com").rstrip("/")
CHARTS_URL = "https://futures.kraken.com/api/charts/v1"
API_KEY = os.getenv("KRAKEN_FUTURES_API_KEY", "")
API_SECRET = os.getenv("KRAKEN_FUTURES_API_SECRET", "")
API_ROOT = "/derivatives/api/v3"

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(),
                    format="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%SZ")
log = logging.getLogger("btc-sma50-live")


class KrakenError(RuntimeError):
    pass


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


def get_daily(symbol: str) -> tuple[datetime, float, float]:
    end = int(time.time())
    start = end - 200 * 86400
    query = urllib.parse.urlencode({"from": start, "to": end, "count": 250})
    payload = public_json(f"{CHARTS_URL}/trade/{urllib.parse.quote(symbol)}/1d?{query}")
    rows = payload.get("candles", [])
    if not rows:
        raise KrakenError("Kraken returned no daily candles")
    now = datetime.now(timezone.utc)
    completed: dict[datetime, float] = {}
    for row in rows:
        try:
            candle_time = datetime.fromtimestamp(float(row["time"]) / 1000, tz=timezone.utc)
            close = float(row["close"])
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
        # Ignore the still-forming UTC daily candle and invalid closes.
        if close > 0 and candle_time + timedelta(days=1) <= now:
            completed[candle_time] = close
    candles = sorted(completed.items())
    if len(candles) < SMA_DAYS:
        raise KrakenError(f"Need {SMA_DAYS} completed daily candles; got {len(candles)}")
    recent = candles[-SMA_DAYS:]
    latest_time, latest_close = recent[-1]
    sma = sum(close for _, close in recent) / SMA_DAYS
    return latest_time, latest_close, sma


class KrakenFutures:
    def __init__(self, key: str, secret: str):
        if not key or not secret:
            raise KrakenError("Set KRAKEN_FUTURES_API_KEY and KRAKEN_FUTURES_API_SECRET in the environment")
        try:
            self.secret = base64.b64decode(secret, validate=True)
        except Exception as exc:
            raise KrakenError("KRAKEN_FUTURES_API_SECRET must be Kraken's base64-encoded API secret") from exc
        self.key = key

    def _signed(self, method: str, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        params = params or {}
        encoded = urllib.parse.urlencode(params)
        nonce = str(int(time.time() * 1000))
        endpoint_path = f"{API_ROOT}/{endpoint.lstrip('/')}"
        # Kraken Futures v3 signature: HMAC-SHA512(base64(secret),
        # base64(SHA256(urlencoded_post_data + nonce + endpoint_path))).
        digest = hashlib.sha256((encoded + nonce + endpoint_path).encode("utf-8")).digest()
        authent = base64.b64encode(hmac.new(self.secret, base64.b64encode(digest), hashlib.sha512).digest()).decode()
        headers = {"APIKey": self.key, "Authent": authent, "Nonce": nonce,
                   "Accept": "application/json", "User-Agent": "btc-sma50-live/1.0"}
        if method == "GET":
            url = f"{BASE_URL}{endpoint_path}" + (f"?{encoded}" if encoded else "")
            data = None
        else:
            url = f"{BASE_URL}{endpoint_path}"
            data = encoded.encode("utf-8")
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                payload = json.load(response)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")[:500]
            raise KrakenError(f"Kraken HTTP {exc.code}: {body}") from exc
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            # On a timed-out order POST the exchange may have accepted it. Caller
            # must reconcile exchange state and must never blindly resubmit.
            raise KrakenError(f"Kraken request failed; reconcile exchange state before any retry: {exc}") from exc
        if not isinstance(payload, dict):
            raise KrakenError("Kraken returned an unexpected private response")
        if payload.get("result") == "error" or payload.get("error"):
            raise KrakenError(f"Kraken rejected request: {payload.get('error', payload)}")
        return payload

    def open_positions(self, symbol: str) -> list[dict[str, Any]]:
        payload = self._signed("GET", "openpositions")
        positions = payload.get("openPositions")
        if not isinstance(positions, list):
            raise KrakenError("Unexpected openpositions response; refusing to trade")
        return [p for p in positions if str(p.get("symbol", "")).upper() == symbol and abs(float(p.get("size", 0))) > 0]

    def account_equity(self) -> float:
        """Return Kraken Futures margin equity in USD; fail closed if ambiguous."""
        payload = self._signed("GET", "accounts")
        accounts = payload.get("accounts")
        if not isinstance(accounts, dict):
            raise KrakenError("Unexpected accounts response; refusing to guess account equity")
        flex = accounts.get("flex")
        if isinstance(flex, dict):
            for key in ("marginEquity", "portfolioValue", "balanceValue"):
                value = flex.get(key)
                if value is not None and float(value) > 0:
                    return float(value)
        raise KrakenError("No positive USD FLEX margin equity returned; refusing to guess account equity")

    def ticker_price(self, symbol: str) -> float:
        payload = public_json(f"{BASE_URL}{API_ROOT}/tickers")
        tickers = payload.get("tickers", [])
        for ticker in tickers:
            if str(ticker.get("symbol", "")).upper() == symbol:
                price = float(ticker.get("last", 0))
                if price > 0:
                    return price
        raise KrakenError(f"No current ticker price available for {symbol}")

    def send_market(self, symbol: str, side: str, size: float, reduce_only: bool, cli_id: str) -> dict[str, Any]:
        params: dict[str, Any] = {
            "orderType": "mkt", "symbol": symbol, "side": side,
            "size": f"{size:.8f}", "cliOrdId": cli_id,
        }
        if reduce_only:
            params["reduceOnly"] = "true"
        payload = self._signed("POST", "sendorder", params)
        status = payload.get("sendStatus")
        if not isinstance(status, dict):
            raise KrakenError("Order response lacked sendStatus; reconcile exchange before continuing")
        state = str(status.get("status", "")).lower()
        if state not in {"placed", "filled", "partiallyfilled", "partially_filled"}:
            raise KrakenError(f"Order was not confirmed accepted (status={state!r}); inspect exchange state")
        return payload

    def cancel_order(self, order_id: str) -> None:
        payload = self._signed("POST", "cancelorder", {"order_id": order_id})
        status = payload.get("cancelStatus", {})
        if str(status.get("status", "")).lower() not in {"cancelled", "alreadycancelled", "notfound"}:
            raise KrakenError(f"Could not confirm protective stop cancellation: {status}")


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as db:
        # Migrate the earlier one-market prototype tables without carrying its
        # checkpoint into a multi-market run.
        for table in ("checkpoints", "pending"):
            exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
            if exists:
                columns = {r[1] for r in db.execute(f"PRAGMA table_info({table})")}
                if "symbol" not in columns:
                    db.execute(f"ALTER TABLE {table} RENAME TO {table}_single_market_legacy")
        db.execute("CREATE TABLE IF NOT EXISTS checkpoints (symbol TEXT PRIMARY KEY, candle TEXT NOT NULL, signal INTEGER NOT NULL, updated_at TEXT NOT NULL)")
        db.execute("CREATE TABLE IF NOT EXISTS pending (symbol TEXT PRIMARY KEY, candle TEXT NOT NULL, signal INTEGER NOT NULL, created_at TEXT NOT NULL)")
        db.execute("CREATE TABLE IF NOT EXISTS protective_stops (symbol TEXT PRIMARY KEY, order_id TEXT NOT NULL, stop_price REAL NOT NULL)")
        db.commit()


def last_checkpoint(symbol: str) -> str:
    with sqlite3.connect(DB_PATH) as db:
        row = db.execute("SELECT candle FROM checkpoints WHERE symbol=?", (symbol,)).fetchone()
    return row[0] if row else ""


def mark_processed(symbol: str, candle: str, signal: int) -> None:
    with sqlite3.connect(DB_PATH) as db:
        db.execute("INSERT OR REPLACE INTO checkpoints VALUES(?,?,?,?)", (symbol, candle, signal, datetime.now(timezone.utc).isoformat()))
        db.commit()


def pending_intent(symbol: str) -> tuple[str, int] | None:
    with sqlite3.connect(DB_PATH) as db:
        row = db.execute("SELECT candle,signal FROM pending WHERE symbol=?", (symbol,)).fetchone()
    return (row[0], row[1]) if row else None


def set_pending(symbol: str, candle: str, signal: int) -> None:
    with sqlite3.connect(DB_PATH) as db:
        db.execute("INSERT OR REPLACE INTO pending VALUES(?,?,?,?)", (symbol, candle, signal, datetime.now(timezone.utc).isoformat()))
        db.commit()


def clear_pending(symbol: str) -> None:
    with sqlite3.connect(DB_PATH) as db:
        db.execute("DELETE FROM pending WHERE symbol=?", (symbol,))
        db.commit()


def get_stop(symbol: str) -> tuple[str, float] | None:
    with sqlite3.connect(DB_PATH) as db:
        row = db.execute("SELECT order_id,stop_price FROM protective_stops WHERE symbol=?", (symbol,)).fetchone()
    return (row[0], float(row[1])) if row else None


def forget_stop(symbol: str) -> None:
    with sqlite3.connect(DB_PATH) as db:
        db.execute("DELETE FROM protective_stops WHERE symbol=?", (symbol,))
        db.commit()


def position_state(client: KrakenFutures, symbol: str) -> tuple[int, float]:
    positions = client.open_positions(symbol)
    if len(positions) > 1:
        raise KrakenError(f"Found {len(positions)} open {symbol} positions; refusing to make assumptions")
    if not positions:
        return 0, 0.0
    position = positions[0]
    side = str(position.get("side", "")).lower()
    if side not in {"long", "short"}:
        raise KrakenError(f"Unrecognized position side {side!r}; refusing to trade")
    return (1 if side == "long" else -1), abs(float(position["size"]))


def floor_lot(qty: float, lot_size: float) -> float:
    return math.floor((qty + 1e-12) / lot_size) * lot_size


def confirm_side(client: KrakenFutures, symbol: str, expected_side: int, timeout: int) -> tuple[int, float]:
    deadline = time.monotonic() + timeout
    last = (0, 0.0)
    while time.monotonic() <= deadline:
        last = position_state(client, symbol)
        if last[0] == expected_side:
            return last
        if last[0] not in (0, expected_side):
            raise KrakenError(f"Exchange position is {last[0]}, expected {expected_side}; stop and inspect")
        time.sleep(2)
    raise KrakenError(f"Position change not confirmed within {timeout}s (exchange state={last}); no order retry was sent")


def execute_signal(client: KrakenFutures, symbol: str, target: int) -> dict[str, Any]:
    current, current_qty = position_state(client, symbol)
    if current == target:
        return {"action": "hold", "side": current, "qty": current_qty}

    # Close any opposite exposure with reduceOnly first, then wait until Kraken
    # reports flat. This avoids accidentally increasing/reversing on close.
    if current:
        close_side = "sell" if current > 0 else "buy"
        close_id = f"sma50c{uuid.uuid4().hex[:20]}"
        client.send_market(symbol, close_side, current_qty, True, close_id)
        deadline = time.monotonic() + CONFIRM_TIMEOUT
        while time.monotonic() <= deadline:
            side, qty = position_state(client, symbol)
            if side == 0:
                break
            if side != current:
                raise KrakenError("Position changed unexpectedly during reduce-only close; stopping")
            # A partial close may be in flight; do not submit another close blindly.
            time.sleep(2)
        else:
            raise KrakenError("Close was not confirmed flat; no entry order sent")

    price = client.ticker_price(symbol)
    lot_size = LOT_SIZES.get(symbol)
    if lot_size is None:
        raise KrakenError(f"No verified LOT_SIZE configured for {symbol}; refusing to trade")
    equity = client.account_equity()
    notional_qty = (equity * LEVERAGE) / price
    quantity = floor_lot(notional_qty, lot_size)
    if quantity < lot_size:
        raise KrakenError(f"Computed order quantity {quantity:g} is below lot size {lot_size:g} for {symbol}")
    entry_side = "buy" if target > 0 else "sell"
    entry_id = f"sma50e{uuid.uuid4().hex[:20]}"
    # Never blindly resend: an ambiguous response may still have resulted in a fill.
    client.send_market(symbol, entry_side, quantity, False, entry_id)
    actual_side, actual_qty = confirm_side(client, symbol, target, CONFIRM_TIMEOUT)
    return {"action": "reversed" if current else "opened", "side": actual_side,
            "qty": actual_qty, "target_qty": quantity, "price": price,
            "equity": equity, "target_notional": equity * LEVERAGE}


@contextmanager
def single_instance():
    lock_path = DB_PATH.with_suffix(DB_PATH.suffix + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "w") as lock_file:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("Another instance is already using this database") from exc
        yield


def run_once(client: KrakenFutures, symbol: str) -> None:
    candle_ts, close, sma = get_daily(symbol)
    candle = candle_ts.isoformat()
    target = 1 if close > sma else -1

    # Always reconcile the live account, even on an already-processed candle.
    # The exchange is authoritative; SQLite only prevents repeated daily actions.
    actual_side, actual_qty = position_state(client, symbol)
    pending = pending_intent(symbol)
    if pending:
        pending_candle, pending_signal = pending
        if actual_side == pending_signal:
            # A prior process may have died after the fill but before checkpoint.
            # Exchange-confirmed target position makes it safe to finalize locally.
            mark_processed(symbol, pending_candle, pending_signal)
            clear_pending(symbol)
            log.info("%s recovered completed order intent: candle=%s side=%s qty=%.8f",
                     symbol, pending_candle, "LONG" if actual_side > 0 else "SHORT", actual_qty)
        else:
            raise KrakenError(f"Unresolved order intent for {pending_candle}; exchange side={actual_side}. "
                              "No new order sent. Inspect Kraken and clear the pending row only after reconciliation.")
    processed = last_checkpoint(symbol)
    if actual_side and not processed and not pending and not ADOPT_EXISTING_POSITION:
        raise KrakenError(f"Found an existing {symbol} position with no bot checkpoint. "
                          "Close it manually or set ADOPT_EXISTING_POSITION=true to let the bot manage it.")
    # Cancel a recorded stop from the previous ATR-based version only after
    # confirming this bot is authorized to manage the position.
    old_stop = get_stop(symbol)
    if old_stop:
        client.cancel_order(old_stop[0])
        forget_stop(symbol)
    if processed == candle:
        if actual_side != target:
            raise KrakenError(f"Candle {candle} is checkpointed but exchange position {actual_side} differs from signal {target}; inspect manually")
        log.info("%s candle=%s close=%.6g sma=%.6g signal=%s exchange=%s qty=%.8f checkpoint=duplicate",
                 symbol, candle, close, sma, "LONG" if target > 0 else "SHORT",
                 {1: "LONG", -1: "SHORT", 0: "FLAT"}[actual_side], actual_qty)
        return

    set_pending(symbol, candle, target)
    result = execute_signal(client, symbol, target)
    # Persist only after the account is confirmed in the target state. If the
    # process crashes after a fill, restart reconciliation sees the exchange
    # position and will not duplicate the entry.
    final_side, final_qty = position_state(client, symbol)
    if final_side != target:
        raise KrakenError(f"Final exchange position {final_side} does not match signal {target}; checkpoint not advanced")
    mark_processed(symbol, candle, target)
    clear_pending(symbol)
    log.info("%s candle=%s close=%.6g sma=%.6g signal=%s action=%s exchange=%s qty=%.8f",
             symbol, candle, close, sma, "LONG" if target > 0 else "SHORT", result["action"],
             "LONG" if final_side > 0 else "SHORT", final_qty)


def preflight(client: KrakenFutures, symbol: str) -> None:
    """Read-only check: credentials, market data, account equity and target size."""
    candle, close, sma = get_daily(symbol)
    target = 1 if close > sma else -1
    equity = client.account_equity()
    price = client.ticker_price(symbol)
    quantity = floor_lot(equity * LEVERAGE / price, LOT_SIZES[symbol])
    side, size = position_state(client, symbol)
    print(f"Read-only preflight: {symbol} | candle={candle.isoformat()} | close={close:.2f} | SMA{SMA_DAYS}={sma:.2f}")
    print(f"Signal={'LONG' if target > 0 else 'SHORT'} | Futures FLEX equity=${equity:.2f} | target notional=${equity * LEVERAGE:.2f} | target size={quantity:.8f}")
    print(f"Current position={('LONG' if side > 0 else 'SHORT') if side else 'FLAT'} | size={size:.8f} | live orders sent=0")
    if side and not last_checkpoint(symbol):
        print("NOTICE: position has no checkpoint in this database; live mode will refuse to adopt it by default.")
    if side and side == target:
        print("NOTICE: an existing same-direction position is not resized by this bot until the next signal reversal.")


def main() -> None:
    parser = argparse.ArgumentParser(description="BTC daily SMA50 / 3x Kraken Futures bot")
    parser.add_argument("--check", action="store_true", help="read-only connectivity and sizing check; sends no orders")
    args = parser.parse_args()
    if (LEVERAGE <= 0 or SMA_DAYS < 2 or POLL_SECONDS < 10 or not SYMBOLS):
        raise SystemExit("Leverage, SMA_DAYS, and POLL_SECONDS must be valid")
    unknown = set(SYMBOLS) - set(LOT_SIZES)
    if unknown:
        raise SystemExit(f"No verified lot sizes configured for: {', '.join(sorted(unknown))}")
    # Constructing the client fails closed before polling if secrets are missing.
    client = KrakenFutures(API_KEY, API_SECRET)
    init_db()
    if args.check:
        preflight(client, "PF_XBTUSD")
        return
    log.info("LIVE trading enabled: Kraken Futures %d symbol(s)=%s | SMA=%d daily | target notional=%.2fx account equity | no stop",
             len(SYMBOLS), ",".join(SYMBOLS), SMA_DAYS, LEVERAGE)
    with single_instance():
        while True:
            for symbol in SYMBOLS:
                try:
                    run_once(client, symbol)
                except Exception as exc:
                    # Do not log request headers, credentials, or full response objects.
                    log.error("%s %s: %s", symbol, type(exc).__name__, exc)
            time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
