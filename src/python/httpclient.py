import logging

import backoff
import requests

from src.python.logger import Logger


class HttpClient:

    default_headers: dict[str, str]
    default_timeout: int
    max_tries: int
    log: logging.Logger

    def __init__(
        self,
        log4py: Logger,
        default_headers: dict[str, str],
        default_timeout: int,
        max_tries: int,
    ) -> None:
        self.log = log4py.getLogger("HttpClient")
        self.default_headers = default_headers
        self.default_timeout = default_timeout
        self.max_tries = max_tries

    def _get_max_tries(self):
        return self.max_tries  # Returns instance-specific max_tries

    def get(self, url: str, headers: dict[str, str] | None = None):
        decorated_get_request = backoff.on_exception(
            wait_gen=backoff.expo,
            exception=Exception,
            max_tries=self._get_max_tries(),
        )(self._get_request)

        return decorated_get_request(url, headers or {})

    def _get_request(self, url: str, headers: dict[str, str]):
        try:
            response = requests.get(
                url,
                headers={**self.default_headers, **headers},
                timeout=self.default_timeout,
            )
            return response
        except Exception as e:
            self.log.error(f"Request failed: {e}")
            raise
