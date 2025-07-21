"""Stream type classes for tap-montapacking."""

from typing import Any, Dict, Iterable, List, Optional, Union

import requests
from pendulum import parse
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.helpers.jsonpath import extract_jsonpath
import datetime
from dateutil.relativedelta import relativedelta
from tap_montapacking.client import MontapackingStream
from urllib.parse import quote
from datetime import datetime, timedelta
from backports.cached_property import cached_property
import pytz

# STREAMS TODO
# PRODUCTS [x]
# INBOUND FORECASTS / BUY ORDERS [X]
# SUPPLIERS [x]
# ORDERS [ ] 
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
        th.Property("Sku", th.StringType),
        th.Property("Description", th.StringType),
        th.Property("Barcodes", th.CustomType({"type": ["array", "string"]})),
        th.Property("WeightGrammes", th.IntegerType),
        th.Property("LengthMm", th.IntegerType),
        th.Property("WidthMm", th.IntegerType),
        th.Property("HeightMm", th.IntegerType),
        th.Property(
            "Stock",
            th.ObjectType(
                th.Property("StockInboundForecasted", th.IntegerType),
                th.Property("StockQuarantaine", th.IntegerType),
                th.Property("StockAll", th.IntegerType),
                th.Property("StockBlocked", th.IntegerType),
                th.Property("StockInTransit", th.IntegerType),
                th.Property("StockReserved", th.IntegerType),
                th.Property("StockInboundHistory", th.IntegerType),
                th.Property("StockAvailable", th.IntegerType),
                th.Property("StockWholeSaler", th.IntegerType),
                th.Property("StockOpen", th.IntegerType),
                th.Property(
                    "PerWarehouse", th.CustomType({"type": ["array", "string"]})
                ),
                th.Property(
                    "PerLocation", th.CustomType({"type": ["array", "string"]})
                ),
                th.Property("MaxPhysicalStock", th.IntegerType),
                th.Property("MinPhysicalStock", th.IntegerType),
            ),
        ),
        th.Property("SupplierCode", th.StringType),
        th.Property("PurchasePrice", th.NumberType),
        th.Property("SellingPrice", th.NumberType),
        th.Property("PurchasePriceHidden", th.BooleanType),
        th.Property("Food", th.BooleanType),
        th.Property("MinimumExpiryPeriodInbound", th.IntegerType),
        th.Property("MinimumExpiryPeriodOutbound", th.IntegerType),
        th.Property("Cool", th.BooleanType),
        th.Property("Note", th.StringType),
        th.Property("HStariefCode", th.StringType),
        th.Property("CountryOfOrigin", th.StringType),
        th.Property("PurchaseStepQty", th.IntegerType),
        th.Property("RegisterSerialNumber", th.BooleanType),
        th.Property("RegisterSerialNumberB2B", th.BooleanType),
        th.Property("IsFragile", th.BooleanType),
        th.Property("IsDangerous", th.BooleanType),
        th.Property("SupplierProductCode", th.StringType),
        th.Property("ProductId", th.IntegerType),
        th.Property("MinimumStock", th.IntegerType),
        th.Property("HSCode", th.StringType),
        th.Property("WareHouseProductSettings", th.StringType),
        th.Property("HTSCode", th.StringType),
        th.Property("CustomField1", th.StringType),
        th.Property("ExcludeFromStockForecast", th.BooleanType),
        th.Property("LeadTime", th.IntegerType)
    ).to_dict()


class ProductsStockStream(ProductsStream):
    """Define custom stream."""

    name = "products_stock"
    primary_keys = ["Sku"]
    replication_key = "LastModified"
    records_jsonpath = "$.Products[*].Product"
    paginate = False

    @cached_property
    def path(self):
        default_date = (datetime.utcnow().astimezone(pytz.timezone('Europe/Amsterdam')) - timedelta(days=7))
        rep_key = self.stream_state.get("replication_key_value")
        if rep_key:
            rep_key = parse(rep_key)
        rep_key = (rep_key or default_date).strftime('%Y-%m-%dT%H:%M:%S')
        return f"/product/updated_since/{rep_key}"
        
    base_properties = [
        th.Property("LastModified", th.DateTimeType)
    ]

    @property
    def schema(self) -> dict:
        schema = super().schema
        for field in self.base_properties:
            schema["properties"].update(field.to_dict())
        return schema
    
    def post_process(self, row, context):
        now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")
        row["LastModified"] = now
        return row

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
        # th.Property("Batch", th.CustomType({"type": ["array", "string"]})),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}

        if next_page_token is not None:
            # For the pagination we need to set the next page token like this.
            params["sinceid"] = next_page_token
        else:
            config_since_id = int(self.config.get("since_id") or "0")
            # For the replication key logic we need the state
            state = self.get_context_state(context) 
            if "replication_key_value" in state:
                state_since_id = int(state["replication_key_value"])
                if state_since_id > config_since_id:
                    params['sinceid'] = state_since_id
                else:
                    params['sinceid'] = config_since_id
            else:
                # if no replication key in state
                # use the since_id from config if not available in state
                params['sinceid'] = config_since_id

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
    primary_keys = ["Reference"]
    replication_key = None
    paginate = True
    records_jsonpath = "$.[*]"
    last_child = None

    schema = th.PropertiesList(
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
                    th.Property("InboundForecastId", th.NumberType),
                )
            ),
        ),
        th.Property("Created", th.DateTimeType),
        th.Property("SupplierCode", th.StringType),
        th.Property("Comment", th.StringType),
        th.Property("DeliveryDate", th.DateTimeType),
        th.Property("ExpectedDeliveryDate", th.DateTimeType),
        th.Property("UniqueId", th.StringType),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        params["page_size"] = 30
        if next_page_token:
            params["page"] = next_page_token

        # Thei api requires a created_since date for this endpoit.
        if self.config.get('start_date') is None:
            start_date = "2000-01-01T00:00:00.000Z"
        else:
            start_date = self.config.get('start_date')
        params["created_since"] = parse(start_date).strftime("%Y-%m-%dT%H:%M:%S")
        return params

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        if "No groups found for these filters" in record.get("Message", ""):
            return None
        if not record.get("Reference"):
            return None
        if self.last_child == record["Reference"]:
            return None
        # if "/" in record["Reference"] or "\\" in record["Reference"]:
        #     return None
        record["Reference"] = record["Reference"].strip().replace("\t", "")
        self.last_child = record["Reference"]
        return {"id": quote(record["Reference"], safe='')}

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
    path = "/inboundforecast/group?Reference={id}"
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
                    th.Property("InboundForecastId", th.NumberType),
                )
            ),
        ),
        th.Property("Created", th.DateTimeType),
        th.Property("SupplierCode", th.StringType),
        th.Property("Comment", th.StringType),
        th.Property("DeliveryDate", th.DateTimeType),
        th.Property("ExpectedDeliveryDate", th.DateTimeType),
        th.Property("UniqueId", th.StringType),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        if next_page_token:
            params["page"] = next_page_token
        return params


class SupplierStream(MontapackingStream):

    name = "suppliers"
    path = "/supplier"
    primary_keys = ["Code"]
    replication_key = None
    records_jsonpath = "$.[*]"
    paginate = False

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


class OrdersStream(MontapackingStream):

    name = "orders"
    path = "/order"
    replication_key = "Received"
    records_jsonpath = "$.[*]"
    created_since = None
    paginate_years = True

    schema = th.PropertiesList(
        th.Property("InternalWebshopOrderId",th.StringType),
        th.Property("WebshopOrderId",th.StringType),
        th.Property("Reference",th.StringType),
        th.Property("Origin",th.StringType),
        th.Property("ConsumerDetails",
            th.ObjectType(
                th.Property("DeliveryAddress",th.ObjectType(
                    th.Property("Company",th.StringType),
                    th.Property("FirstName",th.StringType),
                    th.Property("MiddleName",th.StringType),
                    th.Property("LastName",th.StringType),
                    th.Property("Street",th.StringType),
                    th.Property("HouseNumber",th.StringType),
                    th.Property("HouseNumberAddition",th.StringType),
                    th.Property("PostalCode",th.StringType),
                    th.Property("City",th.StringType),
                    th.Property("State",th.StringType),
                    th.Property("CountryCode",th.StringType),
                    th.Property("PhoneNumber",th.StringType),
                    th.Property("EmailAddress",th.StringType),
                    ),
                ),
                th.Property("InvoiceAddress", th.ObjectType(
                    th.Property("Company",th.StringType),
                    th.Property("FirstName",th.StringType),
                    th.Property("MiddleName",th.StringType),
                    th.Property("LastName",th.StringType),
                    th.Property("Street",th.StringType),
                    th.Property("HouseNumber",th.StringType),
                    th.Property("HouseNumberAddition",th.StringType),
                    th.Property("PostalCode",th.StringType),
                    th.Property("City",th.StringType),
                    th.Property("State",th.StringType),
                    th.Property("CountryCode",th.StringType),
                    th.Property("PhoneNumber",th.StringType),
                    th.Property("EmailAddress",th.StringType),
                    )
                ),
                th.Property("InvoiceDebtorNumber",th.StringType),
                th.Property("B2B",th.BooleanType),
                th.Property("ShippingComment",th.StringType),
                th.Property("CommunicationLanguageCode",th.StringType),
            )),
            th.Property("PlannedShipmentDate",th.DateTimeType),
            th.Property("ShipOnPlannedShipmentDate",th.BooleanType),
            th.Property("Blocked",th.BooleanType),
            th.Property("BlockedMessage",th.CustomType({"type": ["string"]})),
            th.Property("Quarantine",th.BooleanType),
            th.Property("ShipperCode",th.StringType),
            th.Property("MailboxShipperMandatory",th.BooleanType),
            th.Property("ShipperTrackingMandatory",th.BooleanType),
            th.Property("ShipperInsuranceRequired",th.BooleanType),
            th.Property("ShipperInsuranceValue",th.NumberType),
            th.Property("ShipperInsuranceCurrency",th.CustomType({"type": ["string"]})),
            th.Property("DeliveryDateRequested",th.DateTimeType),
            th.Property("Lines",th.ArrayType(
                th.ObjectType(
                    th.Property("Sku",th.StringType),
                    th.Property("OrderedQuantity",th.IntegerType),
                    th.Property("WebshopOrderLineId",th.CustomType({"type": ["string"]})),
                    th.Property("Occured",th.DateTimeType),
                    th.Property("IsFlyer",th.BooleanType),
                    th.Property("Backorder",th.BooleanType),
                    th.Property("HasBeenBackorder",th.BooleanType),
                    th.Property("Description",th.StringType),
                    th.Property("ShippingLabels",th.CustomType({"type": ["string"]})),
                )
            )),
            th.Property("AllowedShippers",th.CustomType({"type": ["array","string"]})),
            th.Property("PackingServices",th.CustomType({"type": ["array","string"]})),
            th.Property("ShipperOptions",th.CustomType({"type": ["array","string"]})),
            th.Property("Received",th.DateTimeType),
            th.Property("Verified",th.DateTimeType),
            th.Property("Backorder",th.BooleanType),
            th.Property("Picking",th.BooleanType),
            th.Property("Picked",th.DateTimeType),
            th.Property("Shipped",th.DateTimeType),
            th.Property("TrackAndTraceLink",th.StringType),
            th.Property("TrackAndTraceCode",th.StringType),
            th.Property("ShipperDescription",th.StringType),
            th.Property("ActionCode",th.CustomType({"type": ["array","string"]})),
            th.Property("Comment",th.CustomType({"type": ["array","string"]})),
            th.Property("HasStockReservation",th.BooleanType),
            th.Property("Deleted",th.BooleanType),
            th.Property("DeliveryStatusDescription",th.StringType),
            th.Property("DeliveryStatusCode",th.StringType),
            th.Property("DropShip",th.BooleanType),
            th.Property("MontaEorderId",th.IntegerType),
            th.Property("Invoice",th.CustomType({"type": ["array", "string", "object"]})),
            th.Property("Family",th.CustomType({"type": ["array","string"]})),
            th.Property("MontaEorderGuid",th.StringType),
            th.Property("IsRunner",th.BooleanType),
            th.Property("EstimatedDeliveryFrom",th.DateTimeType),
            th.Property("EstimatedDeliveryTo",th.DateTimeType),
            th.Property("PickbonIds",th.CustomType({"type": ["array","string"]})),
            th.Property("Prepack",th.BooleanType),
            th.Property("PrepackShip",th.BooleanType),
            th.Property("DeliveryDate",th.DateTimeType),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        if not self.created_since:
            self.created_since = self.get_starting_time(context)
        params['created_since'] = self.created_since   
        if next_page_token and not self.paginate_years:
            params["page"] = next_page_token
        return params   
    
    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        year = None
        current_year = datetime.now().year
        if self.created_since:
            year = self.created_since.year
        if self.paginate_years:
            if year < current_year and not response.json():
                self.created_since = self.created_since + relativedelta(years=1)
                previous_token = previous_token or year
                return previous_token + 1
            else:
                self.paginate_years = False
                return 1
        else:
            if 'No groups found for these filters' in response.text:
                return None
            if response.json():
                return previous_token + 1
            return None
        

class ReturnForecastStream(MontapackingStream):
    name = "return_forecast"
    path = "/returnforecast"
    replication_key = "Created"
    records_jsonpath = "$.[*]"
    created_since = None


    schema = th.PropertiesList(
        th.Property("Id", th.IntegerType),
        th.Property("ReturnForecastId", th.IntegerType),
        th.Property("EorderId", th.IntegerType),
        th.Property("ViaServicePointId", th.IntegerType),
        th.Property("RmaOrderId", th.IntegerType),
        th.Property("Comment", th.StringType),
        th.Property("CommentConsumer", th.StringType),
        th.Property("Code", th.StringType),
        th.Property("RelatieId", th.IntegerType),
        th.Property("CauseDescriptionConsumer", th.StringType),
        th.Property("Created", th.DateTimeType),
        th.Property("ReturnId", th.IntegerType),
        th.Property("IsEnRoute", th.BooleanType),
        th.Property("IsDelivered", th.BooleanType),
        th.Property("IsDeliveredUnknown", th.BooleanType),
        th.Property("CreationSystemId", th.IntegerType),
        th.Property("ShipperId", th.IntegerType),
        th.Property("PickupDate", th.DateTimeType),
        th.Property("AmountOfPackages", th.IntegerType),
        th.Property("Completed", th.BooleanType),
        th.Property("TradeInEorderId", th.IntegerType),
        th.Property("TrackAndTraceCode", th.StringType),
        th.Property("TrackAndTraceLink", th.StringType),
        th.Property("RmaSettingsId", th.IntegerType),
        th.Property(
            "ForecastLineTradeInProduct",
            th.ObjectType(
                th.Property("Id", th.IntegerType),
                th.Property("ProductCode", th.StringType),
                th.Property("ProductDescription", th.StringType),
                th.Property("Quantity", th.IntegerType),
                th.Property("Comment", th.StringType),
            )
        ),
        th.Property(
            "Lines",
            th.ArrayType(
                th.ObjectType(
                    th.Property("Id", th.IntegerType),
                    th.Property("ReturnForecastId", th.IntegerType),
                    th.Property("ProductCode", th.StringType),
                    th.Property("ProductDescription", th.StringType),
                    th.Property("MateriaalId", th.IntegerType),
                    th.Property("ReturnReasonId", th.IntegerType),
                    th.Property("RelatieReturnReasonId", th.IntegerType),
                    th.Property("ParentRelatieReturnReasonId", th.IntegerType),
                    th.Property("ReturnReason", th.StringType),
                    th.Property("ReturnSubReasonId", th.IntegerType),
                    th.Property("ReturnSubReason", th.StringType),
                    th.Property("Comment", th.StringType),
                    th.Property("EorderLineId", th.IntegerType),
                    th.Property("ReturnQuantity", th.IntegerType),
                    th.Property("Reference", th.StringType),
                    th.Property("FollowUpActionId", th.IntegerType),
                )
            )
        )
    ).to_dict()
    
    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of URL parameters for the API request."""
        params: dict = {}

        if not self.created_since:
            self.created_since = self.get_starting_time(context)

        params["startDate"] = self.created_since.strftime("%Y-%m-%d")
        params["endDate"] = (self.created_since + relativedelta(months=1)).strftime("%Y-%m-%d")

        if next_page_token:
            params["page"] = next_page_token

        return params

    def get_starting_time(self, context: Optional[dict]) -> datetime:
        """Determine the start date for fetching data."""
        default_start = datetime.utcnow() - timedelta(days=30)

        state = self.get_context_state(context)
        replication_key_value = state.get(self.replication_key) if state else None

        if replication_key_value:
            replicated_date = datetime.strptime(replication_key_value, "%Y-%m-%dT%H:%M:%S.%fZ")
            return max(replicated_date, default_start)
        else:
            return default_start

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Handle pagination based on API response."""
        data = response.json()
        if not data or not data[0].get('Lines'):
            return None

        # Basic pagination logic
        return (previous_token + 1) if previous_token else 2