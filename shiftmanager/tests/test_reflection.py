#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Tests for ReflectionMixin.

Test Runner: PyTest
"""

import sqlalchemy as sa
import pytest


@pytest.fixture
def table():
    return sa.Table("my_table", sa.MetaData(),
                    sa.schema.Column("col1", sa.INTEGER))


def cleaned(statement):
    text = str(statement)
    stripped_lines = [line.strip() for line in text.split('\n')]
    joined = '\n'.join([line for line in stripped_lines if line])
    return joined


class SqlTextMatcher(object):

    def __init__(self, text):
        self.text = text

    def __eq__(self, text):
        print(cleaned(self.text))
        print(cleaned(text))
        return cleaned(self.text) == cleaned(text)


def test_dedupe(shift, table):
    statement = shift.deep_copy(table, distinct=True, copy_privileges=False)
    expected = """
    LOCK TABLE my_table;
    ALTER TABLE my_table RENAME TO my_table$outgoing;
    CREATE TABLE my_table (
    col1 INTEGER
    )
    ;
    INSERT INTO my_table SELECT DISTINCT * from my_table$outgoing;
    DROP TABLE my_table$outgoing
    """
    assert(cleaned(statement) == cleaned(expected))


def test_cascade(shift, table):
    statement = shift.deep_copy(table, cascade=True, copy_privileges=False)
    expected = """
    LOCK TABLE my_table;
    ALTER TABLE my_table RENAME TO my_table$outgoing;
    CREATE TABLE my_table (
    col1 INTEGER
    )
    ;
    INSERT INTO my_table SELECT * from my_table$outgoing;
    DROP TABLE my_table$outgoing CASCADE
    """
    assert(cleaned(statement) == cleaned(expected))
