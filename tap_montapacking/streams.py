"""Stream type classes for tap-montapacking."""

from typing import Any, Dict, Iterable, List, Optional, Union

import requests
from pendulum import parse
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_montapacking.client import MontapackingStream

# STREAMS TODO
# PRODUCTS [ ]
# INBOUND FORECASTS / BUY ORDERS [X]
# SUPPLIERS [ ]
# ORDERS [ ] # missing creating new orders to test # No need to worry now, we're stil on phase 1
# INBOUNDS [X] 


class ProductsStream(MontapackingStream):
    """Define custom stream."""

    name = "products"
    path = "/products"
    primary_keys = ["Sku"]
    replication_key = None
    records_jsonpath = "$.Products[*].Product"

    schema = th.PropertiesList(
        th.Property("name", th.StringType),
        th.Property("Description", th.StringType),
        th.Property("Barcodes", th.CustomType({"type": ["array", "string"]})),
        th.Property("WeightGrammes", th.IntegerType),
        th.Property("LengthMm", th.StringType),
        th.Property("WidthMm", th.StringType),
        th.Property("HeightMm", th.StringType),
        th.Property(
            "Stock",
            th.ObjectType(
                th.Property("StockInboundForecasted", th.IntegerType),
                th.Property("StockQuarantaine", th.IntegerType),
                th.Property("StockAll", th.IntegerType),
                th.Property("StockBlocked", th.IntegerType),
                th.Property("StockInTransit", th.IntegerType),
                th.Property("StockReserved", th.IntegerType),
                th.Property("StockAvailable", th.IntegerType),
                th.Property("StockWholeSaler", th.IntegerType),
                th.Property(
                    "PerWarehouse", th.CustomType({"type": ["array", "string"]})
                ),
            ),
        ),
        th.Property("SupplierCode", th.StringType),
        th.Property("PurchasePrice", th.StringType),
        th.Property("SellingPrice", th.StringType),
        th.Property("PurchasePriceHidden", th.StringType),
        th.Property("Food", th.BooleanType),
        th.Property("MinimumExpiryPeriodInbound", th.StringType),
        th.Property("MinimumExpiryPeriodOutbound", th.StringType),
        th.Property("Cool", th.BooleanType),
        th.Property("Note", th.StringType),
        th.Property("HStariefCode", th.StringType),
        th.Property("CountryOfOrigin", th.StringType),
        th.Property("PurchaseStepQty", th.IntegerType),
        th.Property("RegisterSerialNumber", th.BooleanType),
        th.Property("RegisterSerialNumberB2B", th.BooleanType),
    ).to_dict()


class InboundsStream(MontapackingStream):
    """Define custom stream."""

    name = "inbounds"
    path = "/inbounds"
    primary_keys = ["Id"]
    replication_key = "Id"
    records_jsonpath = "$.[*]"

    schema = th.PropertiesList(
        th.Property("Id", th.IntegerType),
        th.Property("Sku", th.StringType),
        th.Property("Quantity", th.IntegerType),
        th.Property("Created", th.DateTimeType),
        th.Property("Type", th.StringType),
        th.Property("Quarantaine", th.BooleanType),
        th.Property("ReturnedOrderWebshopOrderId", th.StringType),
        th.Property("Reference", th.StringType),
        th.Property(
            "InboundForecastReference", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "InboundForecastSupplier",
            th.PropertiesList(
                th.Property("AddressCity", th.StringType),
                th.Property("AddressCountry", th.StringType),
                th.Property("AddressEmail", th.StringType),
                th.Property("AddressHouseNo", th.StringType),
                th.Property("AddressHouseNoAddition", th.StringType),
                th.Property("AddressName", th.StringType),
                th.Property("AddressPhone", th.StringType),
                th.Property("AddressPostalCode", th.StringType),
                th.Property("AddressState", th.StringType),
                th.Property("AddressStreet", th.StringType),
                th.Property("Code", th.StringType),
                th.Property("CreditorNumber", th.StringType),
                th.Property("Default", th.BooleanType),
                th.Property("Title", th.StringType),
            ),
        ),
        th.Property("InboundReference", th.CustomType({"type": ["array", "string"]})),
        th.Property(
            "InboundSupplier",
            th.PropertiesList(
                th.Property("AddressCity", th.StringType),
                th.Property("AddressCountry", th.StringType),
                th.Property("AddressEmail", th.StringType),
                th.Property("AddressHouseNo", th.StringType),
                th.Property("AddressHouseNoAddition", th.StringType),
                th.Property("AddressName", th.StringType),
                th.Property("AddressPhone", th.StringType),
                th.Property("AddressPostalCode", th.StringType),
                th.Property("AddressState", th.StringType),
                th.Property("AddressStreet", th.StringType),
                th.Property("Code", th.StringType),
                th.Property("CreditorNumber", th.StringType),
                th.Property("Default", th.BooleanType),
                th.Property("Title", th.StringType),
            ),
        ),
        th.Property("Batch", th.CustomType({"type": ["array", "string"]})),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}

        rep_key = self.get_starting_replication_key_value(context)
        if rep_key and next_page_token is None:
            params["sinceid"] = rep_key
        elif next_page_token is not None:
            params["sinceid"] = next_page_token
        return params

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""

        # Some streams do not need pagination
        if not self.paginate:
            return None

        records = response.json()

        # Check if there are less than 30 records
        if len(records) < 30:
            # if less than 30 records stop paginating
            return None

        # if more than 30 records get the greatest value and return ir as next_page_token
        return max([record["Id"] for record in records])


class InboundsForecastParentStream(MontapackingStream):
    """Define custom stream."""

    name = "inboundforecast_parent"
    path = "/inboundforecast/group"
    primary_keys = ["PoNumber"]
    replication_key = "Created"
    paginate = True
    records_jsonpath = "$.[*]"

    schema = th.PropertiesList(
        th.Property("PoNumber", th.StringType),
        th.Property("Created", th.DateTimeType),
    ).to_dict()

    def get_starting_time(self, context):
        start_date = self.config.get("start_date")
        if start_date:
            start_date = parse(self.config.get("start_date"))
        rep_key = self.get_starting_timestamp(context)
        return rep_key or start_date

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        params["page_size"] = 30
        if next_page_token:
            params["page"] = next_page_token

        rep_key = self.get_starting_time(context).strftime("%Y-%m-%dT%H:%M:%S")
        if rep_key:
            params["created_since"] = rep_key
        else:
            # Thei api requires a created_since date for this endpoit.
            params["created_since"] = "2000-01-01T00:00:00"
        return params

    def validate_response(self, response: requests.Response) -> None:

        if (
            response.status_code in self.extra_retry_statuses
            or 500 <= response.status_code < 600
        ):
            msg = self.response_error_message(response)
            raise RetriableAPIError(msg, response)
        elif 400 <= response.status_code < 500:
            if (
                response.status_code == 404
                and "No groups found for these filters" in response.text
            ):
                return None
            msg = self.response_error_message(response)
            raise FatalAPIError(msg)

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        if "No groups found for these filters" in record.get("Message", ""):
            return None
        return {"id": record["PoNumber"]}

    def safeget(dct, *keys):
        for key in keys:
            dct = dct.get(key)
            if dct is None:
                return None
        return dct

    def _sync_children(self, child_context: dict) -> None:
        # Don't get previous records
        if child_context is None:
            return None
        for child_stream in self.child_streams:
            if child_stream.selected or child_stream.has_selected_descendents:
                child_stream.sync(context=child_context)

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        # Do not store records from the last page (empty) to avoid replication key issue
        if "No groups found for these filters" in response.text:
            return None
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())


class InboundsForecastStream(MontapackingStream):
    """Define custom stream."""

    name = "inboundforecast"
    path = "/inboundforecast/group/{id}"
    primary_keys = ["PoNumber"]
    paginate = False
    records_jsonpath = "$.[*]"
    rest_method = "GET"
    parent_stream_type = InboundsForecastParentStream

    schema = th.PropertiesList(
        th.Property("PoNumber", th.StringType),
        th.Property("Reference", th.StringType),
        th.Property(
            "InboundForecasts",
            th.ArrayType(
                th.ObjectType(
                    th.Property("DeliveryDate", th.DateTimeType),
                    th.Property("Sku", th.StringType),
                    th.Property("Quantity", th.IntegerType),
                    th.Property("Approved", th.BooleanType),
                    th.Property("QuantityReceived", th.IntegerType),
                    th.Property("Reference", th.StringType),
                    th.Property("Comment", th.StringType),
                )
            ),
        ),
        th.Property("Created", th.DateTimeType),
        th.Property("SupplierCode", th.StringType),
        th.Property("Comment", th.StringType),
    ).to_dict()

    def validate_response(self, response: requests.Response) -> None:

        if (
            response.status_code in self.extra_retry_statuses
            or 500 <= response.status_code < 600
        ):
            msg = self.response_error_message(response)
            raise RetriableAPIError(msg, response)
        elif 400 <= response.status_code < 500:
            if (
                response.status_code == 404
                and "No groups found for these filters" in response.text
            ):
                return None
            msg = self.response_error_message(response)
            raise FatalAPIError(msg)


class SupplierStream(MontapackingStream):

    name = "suppliers"
    path = "/supplier"
    primary_keys = ["Code"]
    replication_key = None
    records_jsonpath = "$.[*]"

    schema = th.PropertiesList(
        th.Property("Code", th.StringType),
        th.Property("Title", th.StringType),
        th.Property("AddressName", th.StringType),
        th.Property("AddressStreet", th.StringType),
        th.Property("AddressHouseNo", th.StringType),
        th.Property("AddressHouseNoAddition", th.StringType),
        th.Property("AddressPostalCode", th.StringType),
        th.Property("AddressCity", th.StringType),
        th.Property("AddressState", th.StringType),
        th.Property("AddressCountry", th.StringType),
        th.Property("AddressPhone", th.StringType),
        th.Property("AddressEmail", th.StringType),
        th.Property("CreditorNumber", th.StringType),
        th.Property("Default", th.BooleanType),
    ).to_dict()

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        return None

    # TODO figure out how pagination for this endpoint works


# class OrdersStream(MontapackingStream):

#     name = "orders"
#     path = "/order"
#     primary_keys = ["Id"]
#     replication_key = None
#     records_jsonpath = None

#     schema = th.PropertiesList(

#     ).to_dict()
