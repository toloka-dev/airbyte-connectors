import itertools
import logging
import os
from pathlib import Path
from tempfile import mkstemp

import dbxio
import pyarrow.parquet as pq
from pympler import asizeof

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
        stream._max_buffer_size = asizeof.asizeof("a") + asizeof.asizeof(1)

        for _ in range(2):
            for rec in data:
                stream.add_record(rec)

            assert len(os.listdir(stream.cache_dir)) == 1
            stream._flush_buffer()
            files = os.listdir(stream.cache_dir)
            assert len(files) == 2
            result_data = itertools.chain(*[pq.read_table(Path(stream.cache_dir) / f).to_pylist() for f in files])
            assert data == sorted(result_data, key=lambda r: r["str_field"])

            stream.reset()
            assert len(os.listdir(stream.cache_dir)) == 0
