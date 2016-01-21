#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Tests for PostgresMixin

Test Runner: PyTest
"""
import os

import psycopg2
import pytest

from shiftmanager import util


@pytest.mark.postgrestest
def test_get_connection(postgres):
    cur = postgres.pg_connection.cursor()
    cur.execute("SELECT COUNT(*) FROM test_table;")
    count = [row for row in cur][0][0]
    assert 300 == 300


@pytest.mark.postgrestest
def test_copy_table_to_csv(postgres, tmpdir):
    csv_path = os.path.join(str(tmpdir), "test_table.csv")
    row_count = postgres.copy_table_to_csv("test_table", csv_path)
    assert os.path.isfile(csv_path) is True
    assert row_count == 300

    with open(csv_path, "r") as f:
        rows = [row for row in f]
        assert len(rows) == 300


@pytest.mark.postgrestest
def test_csv_chunk_generator(postgres, tmpdir):
    csv_path = os.path.join(str(tmpdir), "test_table.csv")
    row_count = postgres.copy_table_to_csv("test_table", csv_path)

    def test_row_ids(chunks):
        all_row_ids = []
        for chunk in chunks:
            rows = chunk.split('\n')
            all_row_ids.extend([int(row.split(',')[0]) for row in rows[:-1]])

        assert all_row_ids == [x for x in range(0, 300, 1)]

    for num_chunks in range(1, 30, 1):

        csv_gen = postgres.get_csv_chunk_generator(csv_path,
                                                   row_count, num_chunks)
        chunks = [x for x in csv_gen]

        assert len(chunks) == num_chunks
        test_row_ids(chunks)


@pytest.mark.postgrestest
def test_write_chunk_to_s3(postgres, tmpdir):
    csv_path = os.path.join(str(tmpdir), "test_table.csv")
    row_count = postgres.copy_table_to_csv("test_table", csv_path)
    csv_gen = postgres.get_csv_chunk_generator(csv_path, row_count, 30)
    chunks = [x for x in csv_gen]

    bucket = postgres.get_bucket('com.simple.postgres.mock')
    postgres.write_csv_chunk_to_S3(chunks[0], bucket, 'tmp_chunk_1.csv')

    assert 'tmp_chunk_1.csv' == [x for x in bucket.s3keys.keys()][0]

    val = [x for x in bucket.s3keys.values()][0]
    val.set_contents_from_string.assert_called_once_with(chunks[0],
                                                         encrypt_key=True)


@pytest.mark.postgrestest
def test_copy_table_to_redshift(postgres, tmpdir):
    postgres.copy_table_to_redshift("test_table", 'com.simple.postgres.mock',
                                    "/tmp/backfill/", 10)

    bucket = postgres.get_bucket('com.simple.postgres.mock')

    bucket_keys = list(bucket.s3keys.copy().keys())
    bucket_keys.sort()

    manifest = bucket_keys.pop(0)

    assert manifest.endswith('.manifest')

    key_list = [x.split("_")[3] for x in bucket_keys]

    comparison_list = ["".join([str(y), '.csv']) for y in range(0, 10)]

    assert key_list == comparison_list
