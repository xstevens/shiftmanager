#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Tests for PostgresMixin

Test Runner: PyTest
"""

import psycopg2
import pytest

import shiftmanager.mixins.postgres as sp

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

    create_query = """CREATE TABLE test_table (
                          uuid char(36),
                          number integer,
                          name varchar(255));"""

    cur.execute(create_query)
    conn.commit()

    def teardown_pg():
        drop_query = "DROP TABLE test_table;"
        cur.execute(drop_query)
        conn.commit()
        cur.close()
        conn.close()

    request.addfinalizer(teardown_pg)

@pytest.mark.postgrestest
def test_get_connection(postgres):
    assert True is True
