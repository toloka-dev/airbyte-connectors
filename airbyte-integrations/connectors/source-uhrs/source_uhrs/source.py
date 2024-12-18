from abc import ABC
from datetime import datetime, timedelta
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import msal
import pytz
import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator

PST = pytz.timezone("US/Pacific")


def get_uhrs_oauth_token(
    client_id: str,
    tenant_id: str,
    resource_id: str,
    secret: str,
) -> str:
    authority_url = f"https://login.microsoftonline.com/{tenant_id}"
    resource_url = f"api://{resource_id}"

    app = msal.ConfidentialClientApplication(
        client_id=client_id,
        authority=authority_url,
        client_credential=secret,
    )
    resp = app.acquire_token_for_client(
        scopes=[f"{resource_url}/.default"],
    )

    if "error" in resp:
        raise ValueError(f"Unable to retrieve UHRS Oauth token: {resp}")

    return resp["access_token"]


class UhrsStream(HttpStream, ABC):

    url_base = "https://prod.uhrs.playmsn.com/UHRSService/"

    def __init__(self, start_date, end_date, **kwargs):
        super().__init__(**kwargs)
        self.start_date: str = start_date
        self.end_date: str = end_date
        self.download_datetime: str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "startDate": stream_slice["pst_report_dt"],
            "endDate": stream_slice["pst_report_dt"],
            "hitappIds": "",
            "applicationId": "-1",
            "vendorId": "-1",
            "project": "",
        }

    def request_headers(self, *args, **kwargs) -> Mapping[str, Any]:
        return {
            "Accept": "application/json",
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data = response.json()
        start_report_dt = parse_qs(urlparse(response.url).query)["startDate"][0]
        end_report_dt = parse_qs(urlparse(response.url).query)["endDate"][0]

        # There is no date field in the report, so we need to retrieve the date from the query
        # Also we need to avoid downloading report for more than one date to prevent ambiguity
        assert start_report_dt == end_report_dt, f"Query for more than one date: {start_report_dt}..{end_report_dt}"

        for entry in data:
            entry["pst_report_dt"] = start_report_dt
            entry["utc_download_dttm"] = self.download_datetime
            yield entry

    @property
    def cursor_field(self) -> str:
        return "pst_report_dt"


# Basic incremental stream
class IncrementalUhrsStream(UhrsStream, ABC):
    state_checkpoint_interval = None
    primary_key = None

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        latest_state = latest_record.get(self.cursor_field)
        current_state = current_stream_state.get(self.cursor_field) or latest_state
        try:
            if current_state:
                return {self.cursor_field: max(latest_state or current_state, current_state)}
            return {}
        except TypeError as e:
            raise TypeError(
                f"Expected {self.cursor_field} type '{type(current_state).__name__}' but returned type '{type(latest_state).__name__}'."
            ) from e

    def stream_slices(
        self, *, sync_mode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        stream_state = stream_state or {}
        cursor_value = stream_state.get(self.cursor_field) or self.start_date
        start_date = max(self.start_date, cursor_value)

        return [{self.cursor_field: dt} for dt in self.get_date_chunks(start_date, self.end_date)]

    @staticmethod
    def get_date_chunks(start_dt: str, end_dt: str) -> Iterable[str]:
        start = datetime.fromisoformat(start_dt)
        end = datetime.fromisoformat(end_dt)

        while start <= end:
            yield str(start.date())
            start = start + timedelta(days=1)


class HitAppJudgeReport(IncrementalUhrsStream):
    def path(self, **kwargs) -> str:
        return "WebReporting/HitAppJudgeReport"


class HitAppReport(IncrementalUhrsStream):
    def path(self, **kwargs) -> str:
        return "WebReporting/HitAppReport"


class HitAppDisableEventsReport(IncrementalUhrsStream):
    def path(self, **kwargs) -> str:
        return "WebReporting/HitAppDisableEventsReport"


class SourceUhrs(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """

        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        try:
            oauth_token = self.get_access_token(config)

            url = "https://prod.uhrs.playmsn.com/UHRSService/WebReporting/HitAppDisableEventsReport"
            resp = requests.get(
                url=url,
                headers={
                    "Authorization": f"Bearer {oauth_token}",
                    "Accept": "application/json",
                },
                params={
                    "startDate": datetime.today(),
                    "endDate": datetime.today(),
                    "hitappIds": "",
                    "applicationId": "-1",
                    "vendorId": "-1",
                    "project": "",
                },
            )
            if resp.status_code != 200:
                error_message = resp.text.rstrip("\n")
                if error_message:
                    return False, error_message
                resp.raise_for_status()
        except Exception as e:
            return False, e

        return True, None

    @staticmethod
    def get_access_token(config: Mapping[str, Any]):
        client_id = config["client_id"]
        tenant_id = config["tenant_id"]
        resource_id = config["resource_id"]
        uhrs_secret = config["uhrs_secret"]

        oauth_token = get_uhrs_oauth_token(client_id=client_id, tenant_id=tenant_id, resource_id=resource_id, secret=uhrs_secret)

        return oauth_token

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """

        start_date = config["start_date"]
        end_date = config.get("end_date", str(datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(PST).date()))

        auth = TokenAuthenticator(token=self.get_access_token(config))
        return [
            HitAppJudgeReport(start_date, end_date, authenticator=auth),
            HitAppReport(start_date, end_date, authenticator=auth),
            HitAppDisableEventsReport(start_date, end_date, authenticator=auth),
        ]
