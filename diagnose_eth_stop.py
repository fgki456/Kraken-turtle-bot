#!/usr/bin/env python3
"""Read-only Kraken Futures order/trigger events near the ETH incident.

Run in the existing Railway service with its existing Futures API credentials.
The script never submits orders and prints no credentials, account IDs or raw JSON.
"""

import base64
import hashlib
import hmac
import json
import os
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


START = int(datetime(2026, 9, 26, 20, 59, tzinfo=timezone.utc).timestamp() * 1000)
END = int(datetime(2026, 9, 26, 21, 3, tzinfo=timezone.utc).timestamp() * 1000)
BASE = "https://futures.kraken.com"
PATH = "/api/history/v3/"


def fetch(kind, key, secret):
    if kind not in ("orders", "triggers"):
        raise ValueError("Unapproved read-only endpoint")
    path = PATH + kind
    query = {"since": START, "before": END, "sort": "asc", "count": 1000,
             "tradeable": "PF_ETHUSD"}
    token = None
    for _ in range(5):
        params = dict(query)
        if token:
            params["continuation_token"] = token
        encoded = urlencode(params)
        # Same Kraken Futures Authent algorithm as the main bot, with the
        # history endpoint's path. GET parameters are signed as transmitted.
        digest = hashlib.sha256((encoded + path).encode()).digest()
        signature = base64.b64encode(hmac.new(secret, digest, hashlib.sha512).digest()).decode()
        request = Request(BASE + path + "?" + encoded,
                          headers={"APIKey": key, "Authent": signature,
                                   "Accept": "application/json"})
        try:
            with urlopen(request, timeout=20) as response:
                result = json.load(response)
        except (HTTPError, URLError) as exc:
            raise RuntimeError(f"History {kind} request failed ({type(exc).__name__}); no changes made") from None
        rows = result.get("elements")
        if not isinstance(rows, list):
            raise RuntimeError(f"History {kind} response unexpected; no changes made")
        yield from rows
        token = result.get("continuationToken")
        if not token:
            return
    raise RuntimeError(f"History {kind} has more pages; no changes made")


def summarize(kind, row):
    event = row.get("event") or {}
    if not isinstance(event, dict) or len(event) != 1:
        return None
    name, detail = next(iter(event.items()))
    detail = detail or {}
    order = detail.get("order") or detail.get("newOrder") or detail.get("oldOrder") or {}
    if order.get("tradeable", "").upper() != "PF_ETHUSD":
        return None
    when = datetime.fromtimestamp(row["timestamp"] / 1000, timezone.utc).strftime("%H:%M:%S.%f")[:12]
    trigger = order.get("triggerOptions") or {}
    # Only non-sensitive fields needed to establish whether a stop was placed,
    # triggered, cancelled or rejected; omit order IDs and account data.
    return (f"{when} UTC {kind} {name} "
            f"type={order.get('orderType')} side={order.get('direction')} "
            f"qty={order.get('quantity')} reduceOnly={order.get('reduceOnly')} "
            f"triggerPrice={trigger.get('triggerPrice')} "
            f"reason={detail.get('reason')}")


def main():
    key = os.environ.get("KRAKEN_FUTURES_API_KEY", "").strip()
    secret = os.environ.get("KRAKEN_FUTURES_API_SECRET", "").strip()
    if not key or not secret:
        raise RuntimeError("Existing Kraken Futures API credentials not found; no changes made")
    secret_bytes = base64.b64decode(secret, validate=True)
    print("Read-only ETH order/trigger history: 2026-09-26 20:59-21:03 UTC")
    for kind in ("orders", "triggers"):
        found = 0
        for row in fetch(kind, key, secret_bytes):
            line = summarize(kind, row)
            if line:
                print(line)
                found += 1
        print(f"{kind}: {found} ETH events")
    print("No trading action or local state change performed")


if __name__ == "__main__":
    main()
