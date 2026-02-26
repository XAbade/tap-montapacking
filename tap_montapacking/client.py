"""REST client handling, including MontapackingStream base class."""

from typing import Any, Dict, Iterable, List, Optional, Union,Generator
import backoff
from memoization import cached
from pendulum import parse
from datetime import timedelta
import requests
from singer_sdk.authenticators import BasicAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
import logging
from requests.exceptions import JSONDecodeError as RequestsJSONDecodeError
import time
from datetime import datetime
import urllib3

# Reduce log noise: only log request URL, not response or per-request metrics
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class MontapackingStream(RESTStream):
    """Montapacking stream class."""

    url_base = "https://api-v6.monta.nl"
    paginate = True
    extra_retry_statuses = [429,401]
    timeout = 300  # 5 minutes timeout for API requests

    @property
    def authenticator(self) -> BasicAuthenticator:
        """Return a new authenticator object."""
        return BasicAuthenticator.create_for_stream(
            self,
            username=self.config.get("username"),
            password=self.config.get("password"),
        )

    def _get_state_partition_context(self, context):
        if self.replication_key:
            return super()._get_state_partition_context(context)
        
        return None

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""

        # Some streams do not need pagination
        if not self.paginate:
            return None

        # If the previous token is null, this means we were on the 0th page
        if not previous_token:
            return 1

        # If the previous record had a 404 with 'No groups found for these filters'
        # then we should terminate the pagination
        if 'No groups found for these filters' in response.text:
            return None

        if self.records_jsonpath:
            all_matches = extract_jsonpath(self.records_jsonpath, response.json())
            first_match = next(iter(all_matches), None)

            if first_match is None:
                return None
            else:
                return previous_token + 1

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        if next_page_token:
            params["page"] = next_page_token
        return params

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response and handle Rate Limiting."""
        
        # Handle Rate Limiting (429)
        if response.status_code == 429:
            # Montapacking provides ResetsIn in seconds
            resets_in = int(response.headers.get("RateLimit-ResetsIn", 60))
            remaining = int(response.headers.get("RateLimit-Remaining", 0))
            
            if remaining == 0:
                self.logger.info(f"Rate limit exhausted. Window resets in {resets_in}s.")
                
                # CHOICE: If the wait is short (e.g., < 5 mins), just sleep. 
                # If it's long, fail so the orchestrator can try later.
                if resets_in < 300: 
                    self.logger.info(f"Sleeping for {resets_in} seconds...")
                    time.sleep(resets_in + 1) # Add 1s buffer
                    raise RetriableAPIError("Rate limit reset period over, retrying...")
                else:
                    raise FatalAPIError(
                        f"Rate limit reached. Too long to wait ({resets_in}s). "
                        f"Try again after the window resets."
                    )

            # If remaining > 0 but we still got a 429, it's a transient spike
            raise RetriableAPIError(f"Spike in traffic: {response.text}")

        # Handle standard Retriable Errors (500s, etc)
        if (
            response.status_code in self.extra_retry_statuses
            or 500 <= response.status_code < 600
        ):
            msg = self.response_error_message(response)
            raise RetriableAPIError(msg, response)

        # Handle other 400 Client Errors
        elif 400 <= response.status_code < 500:
            if response.status_code == 404:
                return None
                
            self.logger.error("Error response body: %s", response.text)
            msg = self.response_error_message(response)
            raise FatalAPIError(msg)

    def parse_response(self, response: requests.Response) -> Iterable[dict]:

        try:
            input = response.json()
        except RequestsJSONDecodeError:
            return []

        yield from extract_jsonpath(self.records_jsonpath, input=input)
    
    def backoff_wait_generator(self) -> Generator[float, None, None]:

        return backoff.expo(base=2,factor=3) 
    
    def backoff_max_tries(self) -> int:
        return 7

    @cached
    def get_starting_time(self, context):
        if self.config.get('start_date') is None:
            start_date = "2015-01-01T00:00:00.000Z"
        else:
            start_date = self.config.get('start_date')
        start_date = parse(start_date)
        rep_key = self.get_starting_timestamp(context)
        return rep_key or start_date
    

    def post_process(self, row: dict, context: dict) -> dict :
    
        ## Substract 1 hour from the replication key
        if self.replication_key and self.name not in ["inbounds","inboundforecast_events","product_events"]:
            time_utc = parse(row[self.replication_key]) - timedelta(hours=1)
            row[self.replication_key] = time_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")
        return row
    
    def _request(
        self, prepared_request: requests.PreparedRequest, context: dict 
    ) -> requests.Response:
        
        logging.info(f"Making request to: {prepared_request.url} (timeout: {self.timeout}s)")
        
        try:
            response = self.requests_session.send(prepared_request, timeout=self.timeout, verify=False)
        except requests.exceptions.Timeout:
            logging.error(f"Request timeout after {self.timeout}s for URL: {prepared_request.url}")
            raise RetriableAPIError(f"Request timeout after {self.timeout}s", None)
        except requests.exceptions.ConnectionError as e:
            logging.error(f"Connection error for URL: {prepared_request.url} - {str(e)}")
            raise RetriableAPIError(f"Connection error: {str(e)}", None)
        except requests.exceptions.RequestException as e:
            logging.error(f"Request error for URL: {prepared_request.url} - {str(e)}")
            raise RetriableAPIError(f"Request error: {str(e)}", None)
        
        # Skip per-request metric logs; only sync_duration is logged (via tap metrics filter)
        self.validate_response(response)
        return response

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        use_return_forecast = (
            self.config.get("use_return_forecast")
            if self.config.get("use_return_forecast") != None
            else True
        )
        sync_products = (
            self.config.get("sync_products")
            if self.config.get("sync_products") != None
            else True
        )
        sync_suppliers = (
            self.config.get("sync_suppliers")
            if self.config.get("sync_suppliers") != None
            else True
        )
        sync_sell_orders = (
            self.config.get("sync_sell_orders")
            if self.config.get("sync_sell_orders") != None
            else True
        )
        sync_buy_orders = (
            self.config.get("sync_buy_orders")
            if self.config.get("sync_buy_orders") != None
            else True
        )
        sync_receipts = (
            self.config.get("sync_receipts")
            if self.config.get("sync_receipts") != None
            else True
        )
        sync_productrule = (
            self.config.get("sync_productrule")
            if self.config.get("sync_productrule") != None
            else True
        )

        if (
            (self.name == "products" and not sync_products)
            or (self.name == "suppliers" and not sync_suppliers)
            or (self.name == "orders" and not sync_sell_orders)
            or (self.name == "inboundforecast_parent" and not sync_buy_orders)
            or (self.name == "inbounds" and not sync_receipts)
            or (self.name == "return_forecast" and not use_return_forecast)
            or (self.name == "productrule" and not sync_productrule)
        ):
            pass
        else:
            for record in self.request_records(context):
                transformed_record = self.post_process(record, context)
                if transformed_record is None:
                    continue
                yield transformed_record

    def _write_state_message(self) -> None:
        """Write a STATE message with cleaned bookmarks.

        Removes partition data for streams that do not have a replication key,
        to avoid polluting ``state.json`` with non-incremental child stream
        partitions (e.g. ``inbounds_forecast``, ``inboundforecast_group_sinceid``,
        ``products_details``).
        """
        tap_state = self.tap_state

        if tap_state and tap_state.get("bookmarks"):
            for stream_name, bookmark in list(tap_state["bookmarks"].items()):
                # If the stream exists and has no replication key, drop partitions.
                tap_stream = self._tap.streams.get(stream_name)
                if (
                    tap_stream is not None
                    and not getattr(tap_stream, "replication_key", None)
                    and bookmark.get("partitions")
                ):
                    bookmark["partitions"] = []

        super()._write_state_message()
