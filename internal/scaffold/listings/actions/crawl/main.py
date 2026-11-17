#!/usr/bin/env python3
"""The crawler of the listings example: for each item, it emits the sample listings of listings.json, keyed by id.

A real crawler would fetch pages here. It speaks the Millstream action protocol directly, with the Python standard
library only: millstream init action <name> --lang raw scaffolds an action of this shape with a README of the
messages.
"""

import json
import os
import sys


def send(message):
    """Write one message: one JSON object on one line of stdout, flushed at once."""
    sys.stdout.write(json.dumps(message, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def main():
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "listings.json"), encoding="utf-8") as f:
        listings = json.load(f)
    ready = False
    while True:
        line = sys.stdin.readline()
        if not line:
            return 0  # the end of stdin means shutdown
        try:
            message = json.loads(line)
        except ValueError as err:
            print("malformed line: %s" % err, file=sys.stderr)
            return 1
        kind = message.get("type") if isinstance(message, dict) else None
        if kind == "hello" and not ready:
            ready = True
            send({"type": "ready"})
        elif kind == "item" and ready:
            delivery = message.get("delivery")
            for listing in listings:  # several outputs: each needs its own key
                send({"type": "emit", "delivery": delivery, "key": listing["id"], "payload": listing})
            send({"type": "done", "delivery": delivery})
        elif kind == "shutdown":
            return 0
        else:
            print("unexpected message: %s" % kind, file=sys.stderr)
            return 1


if __name__ == "__main__":
    sys.exit(main())
