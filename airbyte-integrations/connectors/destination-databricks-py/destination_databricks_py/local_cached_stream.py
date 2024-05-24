from __future__ import annotations

import logging
import os
import typing as tp
from pathlib import Path
from tempfile import TemporaryDirectory

import attrs
import pyarrow.parquet as pq
from dbxio import TableSchema
from dbxio.blobs.parquet import create_pa_table
from pympler import asizeof

ROW_GROUP_SIZE_BYTES = 128 * 2**20


def _save_to_parquet_file(records: tp.Dict[str, tp.List[tp.Any]], schema: TableSchema, path: Path):
    pa_table = create_pa_table(records, schema=schema)
    row_group_size = int(pa_table.num_rows / int(pa_table.nbytes / ROW_GROUP_SIZE_BYTES + 1) + 1)
    pq.write_table(pa_table, path, flavor={"spark"}, row_group_size=row_group_size)


@attrs.define
class LocalCachedStream:
    name: str
    schema: TableSchema
    logger: logging.Logger
    tmp_dir: tp.Optional[str] = None
    _cache_dir: tp.Optional[TemporaryDirectory] = None
    _buffer: tp.Optional[tp.Dict[str, tp.List[tp.Any]]] = None
    _buffer_size: int = 0
    _max_buffer_size: int = 50 * 2**20  # 50 Mb
    _file_index: int = 0

    def __enter__(self) -> LocalCachedStream:
        self.reset()
        return self

    def __exit__(self, *args, **kwargs):
        self._cache_dir.__exit__(*args, **kwargs)

    def reset(self) -> None:
        self._cache_dir = TemporaryDirectory(dir=self.tmp_dir)
        self._buffer = None
        self._buffer_size = 0
        self._file_index = 0
        self.logger.debug("Local stream cache %s: new temp directory %s", self.name, self.cache_dir)

    def add_record(self, record: tp.Dict[str, tp.Any]) -> None:
        self._buffer = self._buffer or {col_name: [] for col_name in self.schema.columns}
        for col_name in self.schema.columns:
            val = record.get(col_name)
            self._buffer_size += asizeof.asizeof(val)
            self._buffer[col_name].append(val)

        if self._buffer_size > self._max_buffer_size:
            self._flush_buffer()

    @property
    def cache_dir(self):
        return Path(self._cache_dir.name)

    def get_files(self) -> List[str]:
        self._flush_buffer()
        return os.listdir(self.cache_dir)

    def _flush_buffer(self) -> None:
        assert self._cache_dir
        if self._buffer:
            file_name = f"{self.name}_{self._file_index}.parquet"
            file_path = Path(self.cache_dir) / file_name
            assert not file_path.exists()
            self.logger.debug("Local stream cache %s: flushing data to file %s", self.name, file_name)
            _save_to_parquet_file(self._buffer, self.schema, file_path)
            self._buffer = None
            self._file_index += 1
