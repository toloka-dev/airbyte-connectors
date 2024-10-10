from __future__ import annotations

import logging
import typing as tp

import attrs
import dbxio
from airbyte_cdk.models import DestinationSyncMode

from destination_databricks_py.local_cached_stream import LocalCachedStream


@attrs.define
class DbxWriter:
    client: dbxio.DbxIOClient
    stream_name: str
    table: dbxio.Table
    abs_name: str
    abs_container_name: str
    logger: logging.Logger
    cache: LocalCachedStream
    replace_table: bool

    @classmethod
    def create(cls, client, stream_name, table, abs_name, abs_container_name, sync_mode, logger):
        logger.info("Register %s stream. Table path: %s", stream_name, table.table_identifier)
        return cls(
            client, stream_name, table, abs_name, abs_container_name, logger,
            cache=LocalCachedStream(stream_name, table.schema, logger),
            replace_table=sync_mode == DestinationSyncMode.overwrite,
        )

    def __enter__(self) -> LocalCachedStream:
        self.cache.__enter__()
        return self

    def __exit__(self, *args, **kwargs):
        self.cache.__exit__(*args, **kwargs)

    def flush(self):
        self.logger.info("Flushing stream %s", self.stream_name)
        files = self.cache.get_files()
        if not files:
            if self.replace_table:
                dbxio.create_table(self.table, client=self.client, replace=True).wait()
                self.replace_table = False
            return
        dbxio.bulk_write_local_files(
            table=self.table,
            path=self.cache.cache_dir,
            table_format=dbxio.TableFormat.PARQUET,
            client=self.client,
            abs_name=self.abs_name,
            abs_container_name=self.abs_container_name,
            append=not self.replace_table,
            force=True,
        )
        self.replace_table = False
        self.cache.reset()

    def add_record(self, record: tp.Dict[str, tp.Any]) -> None:
        self.cache.add_record(record)

    def reset_stream(self) -> None:
        self.logger.info("Resetting stream %s", self.stream_name)
        self.replace_table = True
