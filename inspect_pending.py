#!/usr/bin/env python3
"""Read-only inspection of a halted bot and the live Futures account.

No database writes, order submissions, or marker clearing.
"""

import os
import sqlite3
import time
from pathlib import Path

from live_checked import Exchange, Halt


def main():
    path = Path(os.getenv("LIVE_DB_PATH", "/data/turtle_live.sqlite3"))
    if not path.is_absolute() or not path.parent.is_dir() or not os.path.ismount(path.parent):
        raise Halt("Persistent Railway volume not mounted")
    if not path.is_file():
        raise Halt("Existing bot database missing")

    # URI mode=ro guarantees this script cannot change saved trading state.
    with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as db:
        pending = db.execute("SELECT action FROM pending WHERE id=1").fetchone()
        saved = db.execute("SELECT symbol FROM positions").fetchall()
    print("PENDING:", pending[0] if pending else "none", flush=True)
    print("SAVED POSITIONS:", [row[0] for row in saved], flush=True)

    key = os.getenv("KRAKEN_FUTURES_API_KEY", "").strip()
    secret = os.getenv("KRAKEN_FUTURES_API_SECRET", "").strip()
    if not key or not secret:
        raise Halt("Futures API credentials missing")
    exchange = Exchange(key, secret, "https://futures.kraken.com")
    for number in (1, 2):
        positions, orders = exchange.positions(), exchange.orders()
        print(f"CHECK {number}: open positions={len(positions)}, open orders={len(orders)}", flush=True)
        if positions:
            print("POSITION SYMBOLS:", [p.get("symbol") for p in positions], flush=True)
        if orders:
            print("ORDER SYMBOLS:", [o.get("symbol") for o in orders], flush=True)
        if number == 1:
            time.sleep(2)
    print("READ-ONLY INSPECTION COMPLETE. Nothing changed.", flush=True)


if __name__ == "__main__":
    main()
