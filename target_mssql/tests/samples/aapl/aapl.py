"""A simple tap with one big record and schema."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

from singer_sdk import SchemaDirectory, Stream, StreamSchema, Tap

if TYPE_CHECKING:
    from singer_sdk.helpers.types import Context

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

PROJECT_DIR = Path(__file__).parent


class AAPL(Stream):
    """An AAPL stream."""

    name = "aapl"
    schema: ClassVar[StreamSchema[str]] = StreamSchema(
        SchemaDirectory(PROJECT_DIR),
        key="fundamentals",
    )

    @override
    def get_records(self, context: Context | None = None):
        """Generate a single record."""
        with open(PROJECT_DIR / "AAPL.json") as f:
            record = json.load(f)

        yield record


class Fundamentals(Tap):
    """Singer tap for fundamentals."""

    name = "fundamentals"

    @override
    def discover_streams(self):
        """Get financial streams."""
        return [AAPL(self)]
