"""Montapacking tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_montapacking.streams import (
    InboundsForecastParentStream,
    InboundsForecastStream,
    InboundsStream,
    ProductsStream,
    ProductRuleStream,
    SupplierStream,
    OrdersStream,
    ProductsStockStream,
    ReturnForecastStream,
    InboundForecastEventsStream,
    InboundForecastGroupSinceIdStream,
    ProductEventsStream,
    ProductsDetailsStream
)

STREAM_TYPES = [
    ProductsStream, # Phase 2
    SupplierStream, # Phase 2
    InboundsStream,
    InboundsForecastParentStream,
    InboundsForecastStream,
    OrdersStream,
    ProductsStockStream,
    ReturnForecastStream,
    InboundForecastEventsStream,
    InboundForecastGroupSinceIdStream,
    ProductEventsStream,
    ProductsDetailsStream,
    ProductRuleStream,
]


class TapMontapacking(Tap):
    """Montapacking tap class."""

    name = "tap-montapacking"

    config_jsonschema = th.PropertiesList(
        th.Property("username", th.StringType, required=True, secret=True),
        th.Property("password", th.StringType, required=True, secret=True),
        th.Property("since_id", th.StringType, required=False, secret=True),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapMontapacking.cli()
