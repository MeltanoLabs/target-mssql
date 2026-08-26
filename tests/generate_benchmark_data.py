"""Generate a synthetic Singer file for benchmarking."""

import json
import sys
from datetime import datetime, timezone

SCHEMA = {
    "type": "SCHEMA",
    "stream": "benchmark",
    "schema": {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": ["null", "string"]},
            "email": {"type": ["null", "string"]},
            "score": {"type": ["null", "number"]},
            "active": {"type": ["null", "boolean"]},
            "created_at": {"type": ["null", "string"], "format": "date-time"},
            "category": {"type": ["null", "string"]},
            "amount": {"type": ["null", "number"]},
        },
    },
    "key_properties": ["id"],
}

CATEGORIES = ["alpha", "beta", "gamma", "delta", "epsilon"]


def generate(n: int) -> None:
    ts = datetime.now(timezone.utc).isoformat()
    print(json.dumps(SCHEMA))
    for i in range(n):
        record = {
            "type": "RECORD",
            "stream": "benchmark",
            "record": {
                "id": i,
                "name": f"User {i}",
                "email": f"user{i}@example.com",
                "score": round(i * 1.1, 2),
                "active": i % 2 == 0,
                "created_at": ts,
                "category": CATEGORIES[i % len(CATEGORIES)],
                "amount": round(i * 9.99, 2),
            },
            "time_extracted": ts,
        }
        print(json.dumps(record))


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 10_000
    generate(n)
