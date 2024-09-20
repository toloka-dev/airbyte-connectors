#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

import datetime
import json
import logging
from abc import ABC
from collections.abc import Iterable, Mapping, MutableMapping
from typing import Any, Optional, Union

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import NoAuth
from airbyte_cdk.sources.streams.http.exceptions import RequestBodyException

LOGGER = logging.getLogger("airbyte")


class VictoriaMetricsStream(HttpStream, ABC):
    url_base = "FAKE_URL"
    primary_key = None

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__()
        self._url_base = config["url"]
        self._metric_name = config["metric_name"]
        self._n_hours_lookups = config["n_hours_lookups"]
        self._step_in_sec = config["step_in_sec"]

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        return "select/0/prometheus/api/v1/export"

    def _create_prepared_request(
        self,
        path: str,
        headers: Optional[Mapping[str, str]] = None,
        params: Optional[Mapping[str, str]] = None,
        json: Optional[Mapping[str, Any]] = None,
        data: Optional[Union[str, Mapping[str, Any]]] = None,
    ) -> requests.PreparedRequest:
        url = self._join_url(self._url_base, path)
        query_params = params or {}
        args = {"method": self.http_method, "url": url, "headers": headers, "params": query_params}
        if self.http_method.upper() in ("GET", "POST", "PUT", "PATCH"):
            if json and data:
                raise RequestBodyException(
                    "At the same time only one of the 'request_body_data' and 'request_body_json' functions can return data"
                )
            elif json:
                args["json"] = json
            elif data:
                args["data"] = data
        prepared_request: requests.PreparedRequest = self._session.prepare_request(requests.Request(**args))

        return prepared_request

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        now_dttm = datetime.datetime.utcnow()
        start_timestamp = int((now_dttm - datetime.timedelta(hours=self._n_hours_lookups)).timestamp() * 1000)
        end_timestamp = int(now_dttm.timestamp() * 1000)
        return {
            "match[]": self._metric_name,
            "start": start_timestamp,
            "end": end_timestamp,
            "step": self._step_in_sec * 1000,
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        Response is expected to be a newline separated JSON records. This method parses the response and returns an iterable of records.
        :return an iterable containing each record in the response
        """
        for raw_record in response.text.splitlines():
            rec = json.loads(raw_record)
            rec["metric"] = json.dumps(rec["metric"])
            yield rec


class SourceVictoriaMetrics(AbstractSource):
    def check_connection(self, logger, config) -> tuple[bool, any]:
        base_url = config["url"]
        try:
            response = requests.get(f"{base_url}/select/0/prometheus/api/v1/status/tsdb")
            response.raise_for_status()
            return True, None
        except Exception as e:
            return False, repr(e)

    def streams(self, config: Mapping[str, Any]) -> list[Stream]:
        auth = NoAuth()

        return [VictoriaMetricsStream(auth=auth, config=config)]
