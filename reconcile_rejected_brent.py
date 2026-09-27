#!/usr/bin/env python3
"""Safely inspect/reconcile a rejected Brent close on Kraken Futures.

Default mode is read-only. --clear first backs up the mounted SQLite database,
then removes only the exact Brent close marker and saved Brent row after two
flat exchange snapshots. It never submits or cancels exchange orders.
"""
import argparse
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

from live_checked import Exchange, Halt


EXPECTED_PENDING = "close:PF_BRENTOILUSD"
EXPECTED_SYMBOL = "PF_BRENTOILUSD"


def read_state(db):
    try:
        pending = db.execute("SELECT action FROM pending WHERE id=1").fetchone()
        positions = db.execute("SELECT symbol FROM positions ORDER BY symbol").fetchall()
    except sqlite3.Error as exc:
        raise Halt("Could not read the existing bot database") from exc
    return pending[0] if pending else None, [row[0] for row in positions]


def reconcile(exchange, db, *, clear=False, path=None):
    pending, saved = read_state(db)
    print(f"Saved state: pending={pending!r}, positions={saved!r}", flush=True)
    if pending != EXPECTED_PENDING or saved != [EXPECTED_SYMBOL]:
        raise Halt("State is not exactly the rejected Brent close; nothing changed")

    for number in (1, 2):
        positions, orders = exchange.positions(), exchange.orders()
        print(f"Exchange check {number}: positions={len(positions)}, open_orders={len(orders)}",
              flush=True)
        if positions or orders:
            raise Halt("Exchange is not flat; database unchanged")
        if number == 1:
            time.sleep(2)

    if not clear:
        return "Read-only checks passed. Brent row and pending marker remain; trading stays halted."
    if path is None:
        raise Halt("Cannot back up database without its persistent path")

    backup_path = path.with_name(
        f"{path.stem}.before_brent_reconcile_"
        f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}_{os.getpid()}.sqlite3"
    )
    fd = os.open(backup_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.close(fd)
    try:
        with sqlite3.connect(backup_path) as backup:
            db.backup(backup)
        with sqlite3.connect(f"file:{backup_path}?mode=ro", uri=True) as check:
            if read_state(check) != (pending, saved):
                raise Halt("Backup state differs; database unchanged")

        db.execute("BEGIN IMMEDIATE")
        if read_state(db) != (pending, saved):
            db.rollback()
            raise Halt("Saved state changed during checks; database unchanged")
        db.execute("DELETE FROM positions WHERE symbol=?", (EXPECTED_SYMBOL,))
        db.execute("DELETE FROM pending WHERE id=1 AND action=?", (EXPECTED_PENDING,))
        db.commit()
    except BaseException:
        db.rollback()
        raise
    return f"Backup saved at {backup_path}; stale Brent close state cleared. Trading stays halted."


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clear", action="store_true",
                        help="back up DB and clear only after two flat exchange checks")
    args = parser.parse_args()

    path_value = os.getenv("LIVE_DB_PATH", "/data/turtle_live.sqlite3").strip()
    path = Path(path_value)
    if not path.is_absolute() or not path.parent.is_dir():
        raise Halt("LIVE_DB_PATH must point to a file in an existing directory")
    if not os.path.ismount(path.parent):
        raise Halt("Railway volume is not mounted at the LIVE_DB_PATH directory")
    if not path.is_file():
        raise Halt("Existing bot database missing; no file created")

    key = os.getenv("KRAKEN_FUTURES_API_KEY", "").strip()
    secret = os.getenv("KRAKEN_FUTURES_API_SECRET", "").strip()
    if not key or not secret:
        raise Halt("Futures API credentials missing")

    exchange = Exchange(key, secret, "https://futures.kraken.com")
    mode = "rw" if args.clear else "ro"
    with sqlite3.connect(f"file:{path}?mode={mode}", uri=True) as db:
        print(reconcile(exchange, db, clear=args.clear, path=path), flush=True)


if __name__ == "__main__":
    main()
