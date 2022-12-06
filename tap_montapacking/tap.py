"""Montapacking tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers
from tap_montapacking.streams import (
    MontapackingStream,
    ProductsStream,
    InboundsStream
)

STREAM_TYPES = [
    ProductsStream,
    InboundsStream
]


class TapMontapacking(Tap):
    """Montapacking tap class."""
    name = "tap-montapacking"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property("username", th.StringType, required=True, secret=True),
        th.Property("password", th.StringType, required=True, secret=True),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapMontapacking.cli()
