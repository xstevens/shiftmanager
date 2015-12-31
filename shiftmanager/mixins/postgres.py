#!/usr/bin/env python

"""
Mixin classes for working with Postgres database exports
"""

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import psycopg2

from shiftmanager.memoized_property import memoized_property

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

        Parameters
        ----------
        table_name: str
            Table name to be written to CSV
        csv_file_path: str
            File path for the CSV to be written to by Postgres
        """
        copy = "COPY {table_name} TO '{csv_file_path}' DELIMITER ',' CSV;"
        formatted_statement = copy.format(table_name=table_name,
                                          csv_file_path=csv_file_path)
        self.execute_and_commit_single_statement(formatted_statement)

    def get_csv_chunk_generator(self, csv_file_path, chunks):
        """
        Given the csv_file_path, split the CSV into chunks number of string
        blobs.

        Parameters
        ----------
        csv_file_path: str
            File path for the CSV written by Postgres
        chunks: int
            Number of chunks to split the CSV file into
        """
        pass

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
