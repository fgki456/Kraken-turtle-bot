#!/usr/bin/env python3
"""Read-only Kraken Futures fills check around the halted Brent entry."""

import base64
import hashlib
import hmac
import json
import os
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen


START = datetime(2026, 9, 26, 23, 30, tzinfo=timezone.utc)
END = datetime(2026, 9, 26, 23, 40, tzinfo=timezone.utc)


def get_fills(key, secret, params=None):
    endpoint = "/fills"
    encoded = urlencode(params or {})
    digest = hashlib.sha256((encoded + "/api/v3" + endpoint).encode()).digest()
    signature = base64.b64encode(hmac.new(secret, digest, hashlib.sha512).digest()).decode()
    url = "https://futures.kraken.com/derivatives/api/v3" + endpoint
    if encoded:
        url += "?" + encoded
    request = Request(url, headers={"Accept": "application/json", "APIKey": key,
                                    "Authent": signature, "User-Agent": "turtle-read-only-check/1"})
    with urlopen(request, timeout=15) as response:
        result = json.load(response)
    if result.get("result") != "success" or not isinstance(result.get("fills"), list):
        raise RuntimeError("Unexpected Kraken fills response: " + str(result.get("error")))
    return result["fills"]


def parse_time(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def main():
    key = os.getenv("KRAKEN_FUTURES_API_KEY", "").strip()
    secret = os.getenv("KRAKEN_FUTURES_API_SECRET", "").strip()
    if not key or not secret:
        raise RuntimeError("Futures API credentials missing")
    decoded = base64.b64decode(secret, validate=True)
    cursor = None
    total = 0
    brent = []
    covered = False
    for _ in range(5):
        page = get_fills(key, decoded, {"lastFillTime": cursor} if cursor else None)
        total += len(page)
        for row in page:
            when = parse_time(row["fillTime"])
            if START <= when < END and row.get("symbol", "").upper() == "PF_BRENTOILUSD":
                brent.append(row)
        if not page:
            covered = True
            break
        oldest = parse_time(page[-1]["fillTime"])
        if oldest < START:
            covered = True
            break
        if len(page) < 100:
            covered = True
            break
        next_cursor = page[-1]["fillTime"]
        if next_cursor == cursor:
            break
        cursor = next_cursor

    print("WINDOW UTC: 2026-09-26 23:30 through 23:40", flush=True)
    print("HISTORY COVERED:", "YES" if covered else "NO - more history needed", flush=True)
    print("BRENT FILLS IN WINDOW:", len(brent), flush=True)
    for row in brent:
        print("FILL:", row.get("fillTime"), row.get("side"),
              "size=" + str(row.get("size")), "price=" + str(row.get("price")), flush=True)
    print("Other contract fills and credentials were not printed. Nothing changed.", flush=True)


if __name__ == "__main__":
    main()
