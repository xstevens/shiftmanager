#!/usr/bin/env python

"""
Mixin classes for working with Postgres database exports
"""

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import psycopg2

from shiftmanager.memoized_property import memoized_property
from shiftmanager import util

class PostgresMixin(object):
    """The Postgres interaction base class for `Redshift`."""

    @memoized_property
    def pg_connection(self):
        """A `psycopg2.connect` connection to Postgres.

        Instantiation is delayed until the object is first used.
        """
        print("Connecting to %s..." % self.pg_host)
        return psycopg2.connect(user=self.pg_user,
                                host=self.pg_host,
                                port=self.pg_port,
                                database=self.pg_database,
                                password=self.pg_password)

    def execute_and_commit_single_statement(self, statement):
        """Execute single Postgres statement"""
        with self.pg_connection as conn:
            with conn.cursor() as cur:
                cur.execute(statement)

    def get_postgres_connection(self, database=None, user=None, password=None,
                                host=None, port=5432):
        """
        Create a `psycopg2.connect` connection to Redshift.
        """

        self.pg_user = user
        self.pg_host = host
        self.pg_port = port
        self.pg_database = database
        self.pg_password = password
        return self.pg_connection

    def copy_table_to_csv(self, table_name, csv_file_path):
        """
        Use Postgres to COPY the given table_name to a csv file at the given
        csv_path.

        Additionally fetches the row count of the given `table_name` for
        further processing.

        Parameters
        ----------
        table_name: str
            Table name to be written to CSV
        csv_file_path: str
            File path for the CSV to be written to by Postgres

        Returns
        -------
        row_count: int
        """

        copy = "COPY {table_name} TO '{csv_file_path}' DELIMITER ',' CSV;"
        formatted_statement = copy.format(table_name=table_name,
                                          csv_file_path=csv_file_path)
        self.execute_and_commit_single_statement(formatted_statement)

        row_count_select = "SELECT COUNT(*) from {table_name};".format(
            table_name=table_name)
        with self.pg_connection as conn:
            with conn.cursor() as cur:
                cur.execute(row_count_select)
                row_count = [r for r in cur][0][0]
        return row_count

    def get_csv_chunk_generator(self, csv_file_path, row_count, chunks):
        """
        Given the csv_file_path, return string chunks of the CSV with
        `chunk_size` rows per chunk.

        Parameters
        ----------
        csv_file_path: str
            File path for the CSV written by Postgres
        row_count: int
            Number of rows in the CSV
        chunks: int
            Number of chunks to return
        """

        # Get chunk boundaries
        left_closed_boundary = util.linspace(0, row_count, chunks)
        left_closed_boundary.append(row_count - 1)
        right_closed_boundary = left_closed_boundary[1:]
        final_boundary_index = len(right_closed_boundary) - 1

        # We're going to allocate a large buffer for this- lets read as fast
        # as possible
        chunk_lines = []
        boundary_index = 0
        boundary = right_closed_boundary[boundary_index]
        with open(csv_file_path, "r", 1048576) as f:
            for count, row in enumerate(f):
                chunk_lines.append(row)
                if count == boundary:
                    if boundary_index != final_boundary_index:
                        boundary_index += 1
                        boundary = right_closed_boundary[boundary_index]
                    yield "".join(chunk_lines)
                    chunk_lines = []


    def write_csv_chunk_to_S3(self, chunk, s3_key_path):
        """
        Given a string chunk that represents a piece of a CSV file, write
        the chunk to chunk_file_path

        Parameters
        ----------
        chunk: str
            String blob representing a chunk of a larger CSV.
        s3_key_name:
            The key path to write the chunk to
        """
        pass
