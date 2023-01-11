"""REST client handling, including MontapackingStream base class."""

from typing import Any, Dict, Iterable, List, Optional, Union

import requests
from singer_sdk.authenticators import BasicAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError


class MontapackingStream(RESTStream):
    """Montapacking stream class."""

    url_base = "https://api.montapacking.nl/rest/v5"
    paginate = True
    extra_retry_statuses = [429,401]

    @property
    def authenticator(self) -> BasicAuthenticator:
        """Return a new authenticator object."""
        return BasicAuthenticator.create_for_stream(
            self,
            username=self.config.get("username"),
            password=self.config.get("password"),
        )

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

        # If the previous record had a 404 with `{"Message":"No groups found for these filters"}`
        # then we should terminate the pagination
        if '{"Message":"No groups found for these filters"}' in response.text:
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

    # Useful for debugging this API
    # def parse_response(self, response: requests.Response) -> Iterable[dict]:
    #     """Parse the response and return an iterator of result records.

    #     Args:
    #         response: A raw `requests.Response`_ object.

    #     Yields:
    #         One item for every item found in the response.

    #     .. _requests.Response:
    #         https://requests.readthedocs.io/en/latest/api/#requests.Response
    #     """
    #     yield from extract_jsonpath(self.records_jsonpath, input=response.json())
    # USE A BREAKPOINT IN THE yield STATEMENT 

    def validate_response(self, response: requests.Response) -> None:

        if (
            response.status_code in self.extra_retry_statuses
            or 500 <= response.status_code < 600
        ):
            msg = self.response_error_message(response)
            raise RetriableAPIError(msg, response)
        elif 400 <= response.status_code < 500:
            if response.status_code == 404:
                return None
            msg = self.response_error_message(response)
            raise FatalAPIError(msg)