# -*- coding: utf-8 -*-
"""
Test fixture definitions.

These fixture are automatically imported for test files in this directory.
"""

import collections

from mock import MagicMock, PropertyMock
import pytest
import psycopg2


@pytest.fixture
def mock_connection():
    mock_connection = PropertyMock()
    mock_connection.return_value = mock_connection
    mock_connection.__enter__ = MagicMock()
    mock_connection.__exit__ = MagicMock()
    return mock_connection


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


def mogrify(self, batch, parameters=None, execute=False):
    if isinstance(parameters, collections.Mapping):
        parameters = dict([
            (key, psycopg2.extensions.adapt(val).getquoted().decode('utf-8'))
            for key, val in parameters.items()])
    elif isinstance(parameters, collections.Sequence):
        parameters = [
            psycopg2.extensions.adapt(val).getquoted()
            for val in parameters]
    if parameters:
        return batch % parameters
    return batch


@pytest.fixture
def shift(monkeypatch, mock_connection, mock_s3):
    """Patch psycopg2 with connection mocks, return conn"""
    import shiftmanager.redshift as rs

    monkeypatch.setattr('shiftmanager.Redshift.connection', mock_connection)
    monkeypatch.setattr('shiftmanager.Redshift.get_s3_connection',
                        lambda *args, **kwargs: mock_s3)
    monkeypatch.setattr('shiftmanager.Redshift.mogrify', mogrify)
    monkeypatch.setattr('shiftmanager.Redshift.execute', MagicMock())
    shift = rs.Redshift("", "", "", "",
                        aws_access_key_id="access_key",
                        aws_secret_access_key="secret_key")
    shift.s3_conn = mock_s3
    return shift
