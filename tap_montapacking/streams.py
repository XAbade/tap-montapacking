"""Stream type classes for tap-montapacking."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_montapacking.client import MontapackingStream

class ProductsStream(MontapackingStream):
    """Define custom stream."""
    name = "products"
    path = "/products"
    primary_keys = ["Sku"]
    replication_key = None
    records_jsonpath = "$.Products[*].Product"


    schema = th.PropertiesList(
        th.Property("name", th.StringType),
        th.Property("Description",th.StringType),
        th.Property("Barcodes",th.CustomType({"type": ["array", "string"]})),
        th.Property("WeightGrammes",th.StringType),
        th.Property("LengthMm",th.StringType),
        th.Property("WidthMm",th.StringType),
        th.Property("HeightMm",th.StringType),
        th.Property("Stock",th.ObjectType(
            th.Property("StockInboundForecasted",th.IntegerType),
            th.Property("StockQuarantaine",th.IntegerType),
            th.Property("StockAll",th.IntegerType),
            th.Property("StockBlocked",th.IntegerType),
            th.Property("StockInTransit",th.IntegerType),
            th.Property("StockReserved",th.IntegerType),
            th.Property("StockAvailable",th.IntegerType),
            th.Property("StockWholeSaler",th.IntegerType),
            th.Property("PerWarehouse",th.CustomType({"type": ["array", "string"]})),
        )),
        th.Property("SupplierCode",th.StringType),
        th.Property("PurchasePrice",th.StringType),
        th.Property("SellingPrice",th.StringType),
        th.Property("PurchasePriceHidden",th.StringType),
        th.Property("Food",th.BooleanType),
        th.Property("MinimumExpiryPeriodInbound",th.StringType),
        th.Property("MinimumExpiryPeriodOutbound",th.StringType),
        th.Property("Cool",th.BooleanType),
        th.Property("Note",th.StringType),
        th.Property("HStariefCode",th.StringType),
        th.Property("CountryOfOrigin",th.StringType),
        th.Property("PurchaseStepQty",th.IntegerType),
        th.Property("RegisterSerialNumber",th.BooleanType),
        th.Property("RegisterSerialNumberB2B",th.BooleanType)
    ).to_dict()


class InboundsStream(MontapackingStream):
    """Define custom stream."""
    name = "inbounds"
    path = "/inbounds"
    primary_keys = ["Id"]
    replication_key = "Id"
    records_jsonpath = "$.Inbounds[*]"

    schema = th.PropertiesList(
        th.Property("Id",th.IntegerType),
        th.Property("Sku",th.StringType),
        th.Property("Quantity",th.NumberType),
        th.Property("Created",th.DateTimeType),
        th.Property("Type",th.StringType),
        th.Property("Quarantaine",th.BooleanType),
        th.Property("ReturnedOrderWebshopOrderId",th.StringType),
        th.Property("Reference",th.StringType),
        th.Property("InboundForecastReference",th.CustomType({"type": ["array", "string"]})),
        th.Property("InboundForecastSupplier",th.CustomType({"type": ["array", "string"]})),
        th.Property("InboundReference",th.CustomType({"type": ["array", "string"]})),
        th.Property("InboundSupplier",th.CustomType({"type": ["array", "string"]})),
        th.Property("Batch",th.CustomType({"type": ["array", "string"]})),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        if next_page_token:
            params["page"] = next_page_token

        rep_key = self.get_starting_replication_key_value(context)
        if rep_key:
            params['sinceid'] = rep_key
        return params
