#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Tests for PostgresMixin

Test Runner: PyTest
"""
import os
import random
import uuid

import psycopg2
import pytest

import shiftmanager.mixins.postgres as sp
from shiftmanager import util

@pytest.fixture(scope="session")
def postgres(request):
    """
    Setup Postgres table with a few random columns of data for testing.
    Tear down table at end of text fixture context.
    """

    pg = sp.PostgresMixin()
    conn = pg.get_postgres_connection(database="shiftmanager",
                                      user="shiftmanager")
    cur = conn.cursor()

    # Just in case of an unclean exit
    drop_if_exists_query = "DROP TABLE IF EXISTS test_table;"

    cur.execute(drop_if_exists_query)

    # Temp table for test runs; schema is arbitrary.
    create_query = """CREATE TABLE test_table (
                          row_count integer,
                          uuid char(36),
                          name varchar(255));"""

    cur.execute(create_query)

    # Fill table with a few hundred rows of random data
    names = {"jill", "jane", "joe", "jim", "carol"}
    insert_statement = ["INSERT INTO test_table VALUES"]
    for i in range(0, 300, 1):
        name = random.sample(names, 1)[0]
        row_to_insert = " ({row_count}, '{uuid}', '{name}'),".format(
            row_count=i, uuid=uuid.uuid4(), name=name)
        insert_statement.append(row_to_insert)

    joined_insert = "".join(insert_statement)
    complete_insert = "{};".format(joined_insert[:-1])
    cur.execute(complete_insert)
    conn.commit()

    def teardown_pg():
        drop_query = "DROP TABLE test_table;"
        cur.execute(drop_query)
        conn.commit()
        cur.close()
        conn.close()

    request.addfinalizer(teardown_pg)
    return pg

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
    assert os.path.isfile(csv_path) == True
    assert row_count == 300

    with open(csv_path, "r") as f:
        rows = [row for row in f]
        assert len(rows) == 300

@pytest.mark.postgrestest
def test_csv_chunk_generator(postgres, tmpdir):
    csv_path = os.path.join(str(tmpdir), "test_table.csv")
    row_count = postgres.copy_table_to_csv("test_table", csv_path)
    csv_gen = postgres.get_csv_chunk_generator(csv_path, row_count, 10)
    chunks = [x for x in csv_gen]

    assert len(chunks) == 10
