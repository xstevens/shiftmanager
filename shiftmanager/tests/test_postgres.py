#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Tests for PostgresMixin

Test Runner: PyTest
"""
import os

import pytest


@pytest.mark.postgrestest
def test_get_connection(postgres):
    cur = postgres.pg_connection.cursor()
    cur.execute("SELECT COUNT(*) FROM test_table;")
    count = [row for row in cur][0][0]
    assert 300 == count


@pytest.mark.postgrestest
def test_pg_copy_table_to_csv(postgres, tmpdir):

    csv_path = os.path.join(str(tmpdir), "test_table.csv")

    def test_csv_copied(row_count):
        assert os.path.isfile(csv_path) is True
        assert row_count == 300

        with open(csv_path, "r") as f:
            rows = [row for row in f]
            assert len(rows) == 300

        os.remove(csv_path)

    table_name_count = postgres.pg_copy_table_to_csv(
        csv_path,
        pg_table_name="test_table")
    test_csv_copied(table_name_count)
    select_count = postgres.pg_copy_table_to_csv(
        csv_path,
        pg_select_statement="select * from test_table")
    test_csv_copied(select_count)


@pytest.mark.postgrestest
@pytest.mark.parametrize("limit", [1, 5, 13, 29, 30, 31, 97, 300])
def test_csv_chunk_generator(postgres, tmpdir, limit):
    csv_path = os.path.join(str(tmpdir), "test_table.csv")
    select_statement = "select * from test_table LIMIT %s" % limit
    row_count = postgres.pg_copy_table_to_csv(
        csv_path, pg_select_statement=select_statement)

    def test_row_ids(chunks):
        all_row_ids = []
        for chunk in chunks:
            rows = chunk.split('\n')
            all_row_ids.extend([int(row.split(',')[0]) for row in rows[:-1]])

        assert all_row_ids == [x for x in range(0, row_count, 1)]

    for num_chunks in range(1, 30, 1):
        csv_gen = postgres.get_csv_chunk_generator(csv_path,
                                                   row_count, num_chunks)
        chunks = [x for x in csv_gen]

        if row_count <= num_chunks:
            assert len(chunks) == 1
        else:
            assert len(chunks) == num_chunks
        test_row_ids(chunks)


@pytest.mark.postgrestest
def test_write_string_to_s3(postgres, tmpdir):
    csv_path = os.path.join(str(tmpdir), "test_table.csv")
    row_count = postgres.pg_copy_table_to_csv(csv_path, "test_table")

    csv_gen = postgres.get_csv_chunk_generator(csv_path, row_count, 30)
    chunks = [x for x in csv_gen]

    bucket = postgres.get_bucket('com.simple.postgres.mock')
    postgres.write_string_to_s3(chunks[0], bucket, 'tmp_chunk_1.csv')

    assert 'tmp_chunk_1.csv' == [x for x in bucket.s3keys.keys()][0]

    val = [x for x in bucket.s3keys.values()][0]
    val.set_contents_from_string.assert_called_once_with(chunks[0],
                                                         encrypt_key=True)


@pytest.mark.postgrestest
def test_copy_table_to_redshift(postgres, tmpdir):

    # Set up mocking behavior
    cur = postgres.connection.cursor()
    cur.return_rows = [(1,)]

    postgres.copy_table_to_redshift("test_table", 'com.simple.postgres.mock',
                                    "/tmp/backfill/", 10, "test_table")

    bucket = postgres.get_bucket('com.simple.postgres.mock')

    bucket_keys = list(bucket.s3keys.copy().keys())
    bucket_keys.sort()

    manifest = bucket_keys.pop(0)

    assert manifest.endswith('.manifest')

    key_list = [x.split("_")[3] for x in bucket_keys]

    comparison_list = ["".join([str(y), '.csv']) for y in range(0, 10)]

    assert key_list == comparison_list

    copy_statement = cur.statements[-1]

    split_statement = [x.strip() for x in copy_statement.split("\n")]

    assert split_statement[0] == "copy test_table"

    s3_start = "from 's3://com.simple.mock/tmp/backfill/"
    s3_end = ".manifest'"
    assert (split_statement[1].startswith(s3_start) and
            split_statement[1].endswith(s3_end))

    creds = "credentials 'aws_access_key_id=None;aws_secret_access_key=None'"
    assert split_statement[2] == creds
    assert split_statement[3] == "manifest"
    assert split_statement[4] == "csv;"


@pytest.mark.postgrestest
def test_aws_role_copy(postgres, tmpdir):
    cur = postgres.connection.cursor()
    cur.return_rows = [(1,)]

    postgres.set_aws_role('000000', 'TestRole')
    postgres.copy_table_to_redshift('test_table', 'com.simple.postgres.mock',
                                    '/tmp/backfill/', 10, 'test_table')

    copy_statement = cur.statements[-1]
    split_statement = [x.strip() for x in copy_statement.split("\n")]

    creds = "credentials 'aws_iam_role=arn:aws:iam::000000:role/TestRole'"
    assert split_statement[2] == creds


@pytest.mark.postgrestest
def test_aws_security_token(postgres, tmpdir):
    cur = postgres.connection.cursor()
    cur.return_rows = [(1,)]

    postgres.set_aws_credentials('access_key', 'secret_key', 'sec_token')
    postgres.copy_table_to_redshift('test_table', 'com.simple.postgres.mock',
                                    '/tmp/backfill/', 10, 'test_table')

    copy_statement = cur.statements[-1]
    split_statement = [x.strip() for x in copy_statement.split("\n")]

    creds = ("credentials 'aws_access_key_id=access_key;"
             "aws_secret_access_key=secret_key;token=sec_token'")
    assert split_statement[2] == creds
