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
import tempfile

from mock import MagicMock, ANY
import pytest

import shiftmanager.redshift as rs


@pytest.fixture
def mock_redshift():
    """Mock the psycopg2 connection"""
    mock_cur = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    return mock_conn


@pytest.fixture
def mock_s3():
    """Mock the S3 Connection, Bucket, and Key"""

    class MockBucket(object):

        s3keys = {}
        name = "com.simple.mock"

        def new_key(self, keypath):
            key_mock = MagicMock()
            self.s3keys[keypath] = key_mock
            return key_mock

        def delete_keys(self, keys):
            self.recently_deleted_keys = keys

        def reset(self):
            self.s3keys = {}
            self.recently_deleted_keys = []

    mock_S3 = MagicMock()
    mock_S3.get_bucket.return_value = MockBucket()
    return mock_S3


@pytest.fixture
def json_data():
    data = [{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4},
            {"a": 5}, {"a": 6}, {"a": 7}, {"a": 8},
            {"a": 9}, {"a": 10}, {"a": 11}, {"a": 12},
            {"a": 13}, {"a": 14}, {"a": 15}, {"a": 16}]
    return data


@pytest.fixture
def shift(monkeypatch, mock_redshift, mock_s3):
    """Patch psycopg2 with connection mocks, return conn"""
    monkeypatch.setattr('psycopg2.connect',
                        lambda *args, **kwargs: mock_redshift)
    monkeypatch.setattr('shiftmanager.s3.S3.get_s3_connection',
                        lambda *args, **kwargs: mock_s3)
    shift = rs.Redshift("", "", "", "",
                        aws_access_key_id="access_key",
                        aws_secret_access_key="secret_key")
    return shift


@contextmanager
def temp_test_directory():
    try:
        directory = tempfile.mkdtemp()
        yield directory

    finally:
        shutil.rmtree(directory)


def assert_execute(shift, expected):
    """Helper for asserting an executed SQL statement on mock connection"""
    shift.conn.cursor().execute.assert_called_with(expected)


def test_random_password(shift):
    for password in [shift.random_password() for i in range(0, 6, 1)]:
        assert len(password) < 65
        assert len(password) > 7
        for char in r'''\/'"@ ''':
            assert char not in password


def test_jsonpaths(shift):

    test_dict_1 = {"one": 1, "two": {"three": 3}}
    expected_1 = {"jsonpaths": ["$['one']", "$['two']['three']"]}
    assert expected_1 == shift.gen_jsonpaths(test_dict_1)

    test_dict_2 = {"one": [0, 1, 2], "a": {"b": [0]}}
    expected_2 = {"jsonpaths": ["$['a']['b'][1]", "$['one'][1]"]}
    assert expected_2 == shift.gen_jsonpaths(test_dict_2, 1)


def chunk_checker(file_paths):
    """Ensure that we wrote and can read all 16 integers"""
    expected_numbers = list(range(1, 17, 1))
    result_numbers = []
    for filepath in file_paths:
        with gzip.open(filepath, 'rb') as f:
            decoded = f.read().decode("utf-8")
            res = [json.loads(x)["a"] for x in decoded.split("\n")
                   if x != ""]
            result_numbers.extend(res)

    assert expected_numbers == result_numbers


def test_chunk_json_slices(shift, json_data):

    data = json_data
    with temp_test_directory() as dpath:
        for slices in range(1, 19, 1):
            with shift.chunked_json_slices(data, slices, dpath) \
                    as (stamp, paths):

                assert len(paths) == slices
                chunk_checker(paths)

            with shift.chunked_json_slices(data, slices, dpath) \
                    as (stamp, paths):

                assert len(paths) == slices
                chunk_checker(paths)


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


def check_key_calls(s3keys, slices):
    """Helper for checking keys have been called correctly"""

    # Ensure we wrote the correct number of files, with the correct extensions
    assert len(s3keys) == (slices + 2)
    extensions = slices*["gz"]
    extensions.extend(["manifest", "jsonpaths"])
    extensions.sort()

    res_ext = [v.split(".")[-1] for v in s3keys.keys()]
    res_ext.sort()

    assert res_ext == extensions

    # Ensure each had contents set from file once, closed once
    for val in s3keys.values():
        val.set_contents_from_file.assert_called_once_with(ANY)
        val.close.assert_called_once_with()


def get_manifest_and_jsonpaths_keys(s3keys):
    manifest = ["s3://com.simple.mock/{}".format(x)
                for x in s3keys.keys() if "manifest" in x][0]
    jsonpaths = ["s3://com.simple.mock/{}".format(x)
                 for x in s3keys.keys() if "jsonpaths" in x][0]
    return manifest, jsonpaths


def test_copy_to_json(shift, json_data):

    jsonpaths = shift.gen_jsonpaths(json_data[0])

    # With cleanup
    shift.copy_json_to_table("com.simple.mock",
                             "tmp/tests/",
                             json_data,
                             jsonpaths,
                             "foo_table",
                             slices=5)

    # Get our mock bucket
    bukkit = shift.s3conn.get_bucket("foo")
    # 5 slices, one manifest, one jsonpaths
    check_key_calls(bukkit.s3keys, 5)
    mfest, jpaths = get_manifest_and_jsonpaths_keys(bukkit.s3keys)

    expect_creds = "aws_access_key_id={};aws_secret_access_key={}".format(
        "access_key", "secret_key")
    expected = """
            COPY foo_table
            FROM '{manifest}'
            CREDENTIALS '{creds}'
            JSON '{jsonpaths}'
            MANIFEST
            GZIP
            TIMEFORMAT 'auto';
            """.format(manifest=mfest, creds=expect_creds,
                       jsonpaths=jpaths)

    assert_execute(shift, expected)

    # Did we clean up?
    assert set(bukkit.recently_deleted_keys) == set(bukkit.s3keys.keys())

    # Without cleanup
    bukkit.reset()
    shift.copy_json_to_table("com.simple.mock",
                             "tmp/tests/",
                             json_data,
                             jsonpaths,
                             "foo_table",
                             slices=4,
                             clean_up_s3=False)

    bukkit = shift.s3conn.get_bucket("foo")
    # 4 slices
    check_key_calls(bukkit.s3keys, 4)

    # Should not have cleaned up S3
    assert bukkit.recently_deleted_keys == []

    # Do not cleanup local
    bukkit.reset()
    with temp_test_directory() as dpath:
        shift.copy_json_to_table("com.simple.mock",
                                 "tmp/tests/",
                                 json_data,
                                 jsonpaths,
                                 "foo_table",
                                 slices=10,
                                 local_path=dpath,
                                 clean_up_local=False)
        bukkit = shift.s3conn.get_bucket("foo")
        check_key_calls(bukkit.s3keys, 10)
        assert len(os.listdir(dpath)) == 10

def test_throws_s3_error(monkeypatch, mock_redshift, mock_s3):
    monkeypatch.setattr('psycopg2.connect',
                        lambda *args, **kwargs: mock_redshift)
    monkeypatch.setattr('shiftmanager.s3.S3.get_s3_connection',
                        lambda *args, **kwargs: mock_s3)
    shift = rs.Redshift()

    with pytest.raises(rs.S3ConnectionError):
        shift.copy_json_to_table("foo", "foo", {}, {}, "foo")
