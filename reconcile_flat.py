#!/usr/bin/env python3
"""One-shot, read-only check of a halted Kraken Futures bot.

Default mode reads the persistent database and checks Futures positions and
orders twice. It never submits or cancels orders. The optional --clear mode
first saves an SQLite backup on the same volume, then removes exactly the
pending XBT entry marker only if both exchange snapshots are flat and the
database contains no saved positions. Run --clear only after reviewing the
default mode's output and the exchange account by hand.
"""
import argparse
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

from live_checked import Exchange, Halt


EXPECTED_PENDING = "enter:PF_XBTUSD"


def read_state(db):
    try:
        pending = db.execute("SELECT action FROM pending WHERE id=1").fetchone()
        saved = db.execute("SELECT symbol FROM positions").fetchall()
    except sqlite3.Error as exc:
        raise Halt("Could not read existing bot state") from exc
    return pending[0] if pending else None, [row[0] for row in saved]


def reconcile(exchange, db, *, clear=False, path=None):
    pending, saved = read_state(db)
    print(f"Saved state: pending={pending!r}, positions={saved!r}", flush=True)
    if pending != EXPECTED_PENDING or saved:
        raise Halt("Unexpected saved state; nothing changed")
    for number in (1, 2):
        positions, orders = exchange.positions(), exchange.orders()
        print(f"Exchange check {number}: positions={len(positions)}, open_orders={len(orders)}", flush=True)
        if positions or orders:
            raise Halt("Exchange has positions or open orders; nothing changed")
        if number == 1:
            time.sleep(2)
    if not clear:
        return "Read-only check passed. Pending marker remains; trading stays halted."
    if path is None:
        raise Halt("Cannot back up database without its persistent path")
    # SQLite's backup API includes committed WAL changes.
    backup_path = path.with_name(
        f"{path.stem}.before_reconcile_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}_{os.getpid()}.sqlite3"
    )
    fd = os.open(backup_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.close(fd)
    try:
        with sqlite3.connect(backup_path) as backup:
            db.backup(backup)
        with sqlite3.connect(f"file:{backup_path}?mode=ro", uri=True) as check:
            if read_state(check) != (pending, saved):
                raise Halt("Backup state differs; pending marker preserved")
        db.execute("BEGIN IMMEDIATE")
        current, positions = read_state(db)
        if current != pending or positions:
            db.rollback()
            raise Halt("Saved state changed during checks; pending marker preserved")
        db.execute("DELETE FROM pending WHERE id=1 AND action=?", (EXPECTED_PENDING,))
        db.commit()
    except BaseException:
        db.rollback()
        raise
    return f"Backup saved at {backup_path}; XBT pending marker cleared. Trading still halted."


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clear", action="store_true", help="back up DB and clear only after two flat checks")
    args = parser.parse_args()
    path_value = os.getenv("LIVE_DB_PATH", "/data/turtle_live.sqlite3").strip()
    if not path_value:
        raise Halt("LIVE_DB_PATH is empty")
    path = Path(path_value)
    if not path.is_absolute():
        raise Halt(f"LIVE_DB_PATH must be an absolute path; received {str(path)!r}")
    if not path.parent.is_dir():
        raise Halt(f"LIVE_DB_PATH directory is not visible: {str(path.parent)!r}")
    if not os.path.ismount(path.parent):
        raise Halt("Attach the Railway volume at the LIVE_DB_PATH directory")
    if not path.is_file():
        raise Halt("Existing bot database missing; no file created")
    key = os.getenv("KRAKEN_FUTURES_API_KEY", "").strip()
    secret = os.getenv("KRAKEN_FUTURES_API_SECRET", "").strip()
    if not key or not secret:
        raise Halt("Futures API credentials missing")
    exchange = Exchange(key, secret, "https://futures.kraken.com")
    with sqlite3.connect(f"file:{path}?mode={'rw' if args.clear else 'ro'}", uri=True) as db:
        print(reconcile(exchange, db, clear=args.clear, path=path), flush=True)


if __name__ == "__main__":
    main()
