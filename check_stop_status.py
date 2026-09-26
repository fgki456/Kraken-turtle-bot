#!/usr/bin/env python3
"""Probe Kraken Futures' read-only order status endpoint without trading."""

import os
import uuid
from urllib.error import HTTPError, URLError

from live_checked_stopfix import Exchange


def main():
    key = os.environ.get("KRAKEN_FUTURES_API_KEY", "").strip()
    secret = os.environ.get("KRAKEN_FUTURES_API_SECRET", "").strip()
    base = os.environ.get("KRAKEN_FUTURES_BASE", "https://futures.kraken.com")
    if not key or not secret:
        raise RuntimeError("Missing Futures credentials; no changes made")
    if base != "https://futures.kraken.com":
        raise RuntimeError("This check expects the existing production Futures account")
    exchange = Exchange(key, secret, base)
    # A fresh, random ID cannot belong to any real order.  The same request
    # encoding and authentication as the patched bot are exercised here.
    try:
        result = exchange.request("/orders/status", {"orderIds": str(uuid.uuid4())}, "POST")
    except (HTTPError, URLError, ValueError) as exc:
        print("STOP STATUS CHECK FAILED:", type(exc).__name__)
        print("Trading remains in preview; no order or account state was changed")
        return
    except Exception as exc:
        print("STOP STATUS CHECK FAILED:", type(exc).__name__)
        print("Trading remains in preview; no order or account state was changed")
        return
    orders = result.get("orders")
    if isinstance(orders, list):
        print("STOP STATUS API OK; response format OK")
        print("A real protective stop still needs verification when an order exists")
    else:
        print("STOP STATUS RESPONSE UNEXPECTED; keep trading in preview")
    print("No order submitted and no account state changed")


if __name__ == "__main__":
    main()
