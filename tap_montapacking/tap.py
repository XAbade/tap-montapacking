"""Montapacking tap class."""

import logging
from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk import metrics as sdk_metrics

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


class _SyncDurationOnlyFilter(logging.Filter):
    """Only allow sync_duration metrics through; drop http_request_*, record_count, etc."""

    def filter(self, record: logging.LogRecord) -> bool:
        point = getattr(record, "point", None)
        if point is None:
            return True
        if isinstance(point, dict):
            return point.get("metric") == "sync_duration"
        return True


class TapMontapacking(Tap):
    """Montapacking tap class."""

    name = "tap-montapacking"

    def load_state(self, state: dict) -> None:
        self._raw_bookmark_keys = set(state.get("bookmarks", {}).keys())
        super().load_state(state)
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        metrics_logger = logging.getLogger(sdk_metrics.METRICS_LOGGER_NAME)
        metrics_logger.addFilter(_SyncDurationOnlyFilter())

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
