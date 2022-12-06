"""REST client handling, including MontapackingStream base class."""

import requests
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from memoization import cached

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import BasicAuthenticator

class MontapackingStream(RESTStream):
    """Montapacking stream class."""

    url_base = "https://api.montapacking.nl/rest/v5"
    paginate = True

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
        if not self.paginate: 
            return None 

        if not previous_token:
            return 1

        if self.records_jsonpath:
            all_matches = extract_jsonpath(
                self.records_jsonpath, response.json()
            )
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
