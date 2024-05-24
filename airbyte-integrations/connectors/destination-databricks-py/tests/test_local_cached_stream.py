import logging
import os
from pathlib import Path
from tempfile import mkstemp

import dbxio
import pyarrow.parquet as pq

from destination_databricks_py.local_cached_stream import LocalCachedStream, _save_to_parquet_file  # noqa


def test_save_to_parquet_file():
    schema = dbxio.TableSchema(
        [
            {"name": "str_field", "type": dbxio.types.StringType()},
            {"name": "int_field", "type": dbxio.types.IntType()},
        ]
    )
    data = {
        "str_field": ["a", "b", "c"],
        "int_field": [1, 2, 3],
    }
    _, f_name = mkstemp(suffix=".parquet")
    _save_to_parquet_file(data, schema, f_name)

    assert data == pq.read_table(f_name).to_pydict()


def test_simple():
    schema = dbxio.TableSchema(
        [
            {"name": "str_field", "type": dbxio.types.StringType()},
            {"name": "int_field", "type": dbxio.types.IntType()},
        ]
    )
    data = [
        {"str_field": "a", "int_field": 1},
        {"str_field": "b", "int_field": 2},
        {"str_field": "c", "int_field": 3},
    ]
    with LocalCachedStream("test", schema, logger=logging.getLogger(__name__)) as stream:
        for _ in range(2):
            for rec in data:
                stream.add_record(rec)

            assert len(os.listdir(stream.cache_dir)) == 0
            stream._flush_buffer()
            files = os.listdir(stream.cache_dir)
            assert len(files) == 1
            assert data == pq.read_table(Path(stream.cache_dir) / files[0]).to_pylist()

            stream.reset()
            assert len(os.listdir(stream.cache_dir)) == 0


def test_auto_flush():
    schema = dbxio.TableSchema(
        [
            {"name": "str_field", "type": dbxio.types.StringType()},
            {"name": "int_field", "type": dbxio.types.IntType()},
        ]
    )
    data = [
        {"str_field": "a", "int_field": 1},
        {"str_field": "b", "int_field": 2},
        {"str_field": "c", "int_field": 3},
    ]
    with LocalCachedStream("test", schema, logger=logging.getLogger(__name__)) as stream:
        stream._max_buffer_size = 0

        for _ in range(2):
            for rec in data:
                stream.add_record(rec)

            assert len(os.listdir(stream.cache_dir)) == 3
            stream._flush_buffer()
            files = sorted(os.listdir(stream.cache_dir))
            assert len(files) == 3
            for idx in range(len(data)):
                assert data[idx] == pq.read_table(Path(stream.cache_dir) / files[idx]).to_pylist()[0]

            stream.reset()
            assert len(os.listdir(stream.cache_dir)) == 0
