#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import json
import logging
import time
import typing as tp
from collections import defaultdict
from uuid import uuid4

import dbxio
from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import (
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteStateType,
    ConfiguredAirbyteCatalog,
    DestinationSyncMode,
    Status,
    Type,
)

FIELD_AB_ID = "_airbyte_ab_id"
FIELD_DATA = "_airbyte_data"
FIELD_EMITTED_AT = "_airbyte_emitted_at"


def get_client(config: tp.Mapping[str, tp.Any]) -> dbxio.DbxIOClient:
    http_path = config["databricks_http_path"]
    server_hostname = config["databricks_server_hostname"]
    token = config.get("databricks_personal_access_token")
    if token:
        creds = dbxio.BareAuthProvider(
            cluster_type=dbxio.ClusterType.SQL_WAREHOUSE,
            access_token=token,
            server_hostname=server_hostname,
            http_path=http_path,
        )
    else:
        creds = dbxio.DefaultCredentialProvider(
            cluster_type=dbxio.ClusterType.SQL_WAREHOUSE,
            server_hostname=server_hostname,
            http_path=http_path,
        )

    return dbxio.DbxIOClient.from_auth_provider(creds)


def table_path(catalog: str, schema: str, stream: str) -> dbxio.Table:
    return dbxio.Table(
        f"{catalog}.{schema}.{stream}",
        schema=dbxio.TableSchema(
            [
                {"name": FIELD_AB_ID, "type": dbxio.types.StringType()},
                {"name": FIELD_DATA, "type": dbxio.types.StringType()},
                {"name": FIELD_EMITTED_AT, "type": dbxio.types.TimestampType()},
            ]
        ),
    )


class DestinationDatabricks(Destination):
    def write(
        self,
        config: tp.Mapping[str, tp.Any],
        configured_catalog: ConfiguredAirbyteCatalog,
        input_messages: tp.Iterable[AirbyteMessage],
    ) -> tp.Iterable[AirbyteMessage]:
        client = get_client(config=config)
        catalog = config["database"]
        default_schema = config["schema"]
        abs_name = config["abs_name"]
        abs_container_name = config["abs_container_name"]

        stream_tables: tp.Dict[str, dbxio.Table] = {}
        for configured_stream in configured_catalog.streams:
            stream_name = configured_stream.stream.name
            stream_schema = configured_stream.stream.namespace or default_schema
            assert stream_schema
            stream_tables[stream_name] = table_path(catalog, stream_schema, stream_name)
            if configured_stream.destination_sync_mode == DestinationSyncMode.overwrite:
                dbxio.drop_table(stream_tables[stream_name], client, force=True).wait()

        batch_id = str(uuid4())
        buffer: tp.Dict[str, tp.List[dict]] = defaultdict(list)

        def flush_streams(streams: tp.List[str]):
            for stream in streams:
                records = buffer.pop(stream, None)
                if not records:
                    continue
                dbxio.bulk_write_table(
                    table=stream_tables[stream],
                    new_records=records,
                    client=client,
                    abs_name=abs_name,
                    abs_container_name=abs_container_name,
                    append=True,
                )

        for message in input_messages:
            if message.type == Type.STATE:
                state = message.state
                if state.type is None or state.type == AirbyteStateType.LEGACY:
                    streams_to_flush = list(stream_tables.keys())
                elif state.type == AirbyteStateType.STREAM:
                    streams_to_flush = [state.stream.stream_descriptor.name]
                elif state.type == AirbyteStateType.GLOBAL:
                    streams_to_flush = [s.stream_descriptor.name for s in state.global_.stream_states]
                else:
                    raise NotImplementedError(f"Unknown state event: {state.type}")
                flush_streams(streams_to_flush)
                yield message
            elif message.type == Type.RECORD:
                record = message.record
                buffer[record.stream].append(
                    {
                        FIELD_AB_ID: batch_id,
                        FIELD_EMITTED_AT: record.emitted_at,
                        FIELD_DATA: json.dumps(record.data, separators=(",", ":")),
                    }
                )
            else:
                continue

        flush_streams(list(stream_tables.keys()))

    def check(self, logger: logging.Logger, config: tp.Mapping[str, tp.Any]) -> AirbyteConnectionStatus:
        logger.debug("Databricks Destination Config Check")
        try:
            client = get_client(config=config)
            catalog = config["database"]
            default_schema = config["schema"]
            abs_name = config["abs_name"]
            abs_container_name = config["abs_container_name"]

            t = table_path(catalog, default_schema, "_connection_check")
            dbxio.drop_table(t, client, force=True).wait()
            dbxio.bulk_write_table(
                table=t,
                new_records=[
                    {FIELD_DATA: '{"key": "value"}', FIELD_EMITTED_AT: int(time.time()), FIELD_AB_ID: str(uuid4())}
                ],
                client=client,
                abs_name=abs_name,
                abs_container_name=abs_container_name,
                append=True,
            )
            dbxio.drop_table(t, client, force=True).wait()
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {repr(e)}")
