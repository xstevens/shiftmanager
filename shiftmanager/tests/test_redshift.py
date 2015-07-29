#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Redshift tests

Test Runner: PyTest
"""

from contextlib import contextmanager
import gzip
import json
import os
import shutil

from mock import MagicMock
import pytest

import shiftmanager.redshift as rs


@pytest.fixture
def mocks():
    """Mock the psycopg2 connection"""
    mock_cur = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    return mock_conn


@pytest.fixture
def shift(monkeypatch, mocks):
    """Patch psycopg2 with connection mocks, return conn"""
    boto_mock = MagicMock()
    monkeypatch.setattr('psycopg2.connect',
                        lambda *args, **kwargs: mocks)
    shift = rs.Shift("", "", "", "", "", "", connect_s3=False)
    return shift


@contextmanager
def temp_test_directory():
    try:
        user_home = os.path.expanduser("~")
        directory = os.path.join(user_home, ".shiftmanager", "test")
        if not os.path.exists(directory):
            os.makedirs(directory)

        yield directory

    finally:
        shutil.rmtree(directory)


def assert_execute(shift, expected):
    """Helper for asserting an executed SQL statement on mock connection"""
    shift.conn.cursor().execute.assert_called_with(expected)


def test_redshift_transaction(shift):

    with shift.redshift_transaction("") as (conn, cur):
        pass

    shift.cur.execute.assert_called_once_with("SET search_path = public")
    shift.conn.commit.assert_called_with()


def test_random_password(shift):
    for password in [shift.random_password() for i in range(0, 6, 1)]:
        assert len(password) < 65
        assert len(password) > 7
        for char in r'''\/'"@ ''':
            assert char not in password


def test_jsonpaths(shift):

    test_dict_1 = {"one": 1, "two": {"three": 3}}
    expected_1 = {"jsonpaths": ["['one']", "['two']['three']"]}
    assert expected_1 == shift.gen_jsonpaths(test_dict_1)

    test_dict_2 = {"one": [0, 1, 2], "a": {"b": [0]}}
    expected_2 = {"jsonpaths": ["['a']['b'][1]", "['one'][1]"]}
    assert expected_2 == shift.gen_jsonpaths(test_dict_2, 1)

def chunk_checker(file_paths):
    """Ensure that we wrote and can read all 16 integers"""
    expected_numbers = range(1, 17, 1)
    result_numbers = []
    for filepath in file_paths:
        with gzip.open(filepath, 'rb') as f:
            res = [json.loads(x)["a"] for x in f.read().split("\n")
                   if x != ""]
            result_numbers.extend(res)

    assert expected_numbers == result_numbers


def test_chunk_json_slices(shift):

    docs = [{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4},
            {"a": 5}, {"a": 6}, {"a": 7}, {"a": 8},
            {"a": 9}, {"a": 10}, {"a": 11}, {"a": 12},
            {"a": 13}, {"a": 14}, {"a": 15}, {"a": 16}]

    with temp_test_directory() as dpath:
        for slices in range(1, 19, 1):
            with shift.chunk_json_slices(docs, slices, dpath) as file_paths:
                assert len(file_paths) == slices
                chunk_checker(file_paths)

            with shift.chunk_json_slices(docs, slices, dpath) as file_paths:
                assert len(file_paths) == slices
                chunk_checker(file_paths)

def test_create_user(shift):

    shift.create_user("swiper", "swiperpass")

    expected = """
        CREATE USER swiper
        PASSWORD 'swiperpass'
        IN GROUP analyticsusers;
        ALTER USER swiper
        SET wlm_query_slot_count TO 4;
        """

    assert_execute(shift, expected)


def test_set_password(shift):

    shift.set_password("swiper", "swiperpass")

    expected = """
        ALTER USER swiper
        PASSWORD 'swiperpass';
        """

    assert_execute(shift, expected)


def test_dedupe(shift):

    shift.dedupe("test")

    expected = """
        -- make all updates to this table block
        LOCK test;

        -- CREATE TABLE LIKE copies the dist key
        CREATE TEMP TABLE test_copied (LIKE test);

        -- move the data
        INSERT INTO test_copied SELECT DISTINCT * FROM test;
        DELETE FROM test;  -- slower than TRUNCATE, but transaction-safe
        INSERT INTO test (SELECT * FROM test_copied);
        DROP TABLE test_copied;
        """

    assert_execute(shift, expected)
