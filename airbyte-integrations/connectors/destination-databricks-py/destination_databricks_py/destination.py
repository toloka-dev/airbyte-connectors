#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import json
import logging
import time
import typing as tp
from contextlib import ExitStack
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

from destination_databricks_py.consts import DEST_SCHEMA, FIELD_AB_ID, FIELD_DATA, FIELD_EMITTED_AT
from destination_databricks_py.local_cached_stream import LocalCachedStream

LOGGER = logging.getLogger("airbyte")


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
        f"{catalog}.{schema}._airbyte_raw_{stream}",
        schema=DEST_SCHEMA,
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
            LOGGER.info("Register %s stream. Table path: %s", stream_name, stream_tables[stream_name].table_identifier)
            if configured_stream.destination_sync_mode == DestinationSyncMode.overwrite:
                dbxio.drop_table(stream_tables[stream_name], client, force=True).wait()

        def reset_streams(streams: tp.List[str]):
            for stream in streams:
                t = stream_tables[stream]
                LOGGER.info("Resetting stream %s. Dropping table %s", t.table_identifier)
                dbxio.drop_table(t, client, force=True).wait()

        reset_streams(
            [
                s.stream.name
                for s in configured_catalog.streams
                if s.destination_sync_mode == DestinationSyncMode.overwrite
            ]
        )

        batch_id = str(uuid4())
        with ExitStack() as stack:
            buffer = {
                s.stream.name: stack.enter_context(LocalCachedStream(s.stream.name, DEST_SCHEMA, LOGGER))
                for s in configured_catalog.streams
            }

            def flush_streams(streams: tp.List[str]):
                for stream in streams:
                    LOGGER.info("Flushing stream %s", stream)
                    cache = buffer[stream]
                    files = cache.get_files()
                    if not files:
                        continue
                    dbxio.bulk_write_local_files(
                        table=stream_tables[stream],
                        path=cache.cache_dir,
                        table_format=dbxio.TableFormat.PARQUET,
                        client=client,
                        abs_name=abs_name,
                        abs_container_name=abs_container_name,
                        append=True,
                        force=True,
                    )
                    cache.reset()

            for message in input_messages:
                if message.type == Type.STATE:
                    state = message.state
                    if state.type is None or state.type == AirbyteStateType.LEGACY:
                        if not state.stream.stream_state:
                            LOGGER.info("Got legacy request to reset all streams")
                            streams_to_reset = list(stream_tables.keys())
                        else:
                            LOGGER.info("Got legacy request to flush all streams")
                            streams_to_flush = list(stream_tables.keys())
                    elif state.type == AirbyteStateType.STREAM:
                        stream_name = state.stream.stream_descriptor.name
                        if not state.stream.stream_state:
                            LOGGER.info("Got request to reset stream %s", stream_name)
                            streams_to_reset = [stream_name]
                        else:
                            LOGGER.info("Got request to flush stream %s", stream_name)
                            streams_to_flush = [stream_name]
                    elif state.type == AirbyteStateType.GLOBAL:
                        streams = state.global_.stream_states
                        streams_to_reset = [s.stream_descriptor.name for s in streams if not s.stream_state]
                        streams_to_flush = [s.stream_descriptor.name for s in streams if s.stream_state]
                        LOGGER.info(
                            "Got global request. Flush streams: %s. Reset streams: %s",
                            streams_to_flush,
                            streams_to_reset,
                        )
                        # BUG: global stream_states keep stream names without prefix
                        assert not streams_to_flush and not streams_to_reset
                        if streams_to_flush:
                            streams_to_flush = list(stream_tables.keys())
                        else:
                            streams_to_reset = list(stream_tables.keys())
                    else:
                        raise NotImplementedError(f"Unknown state event: {state.type}")
                    flush_streams(streams_to_flush)
                    reset_streams(streams_to_reset)
                    yield message
                elif message.type == Type.RECORD:
                    record = message.record
                    buffer[record.stream].add_record(
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
