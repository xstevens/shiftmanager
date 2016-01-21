#!/usr/bin/env python

"""
Mixin classes for working with Postgres database exports
"""

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
from datetime import datetime
import json
from tempfile import mkstemp
import os

import psycopg2

from shiftmanager.memoized_property import memoized_property
from shiftmanager.mixins.s3 import S3Mixin
from shiftmanager import util


class PostgresMixin(S3Mixin):
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

    def create_connection(self, database=None, user=None, password=None,
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

        # We're going to allocate a large buffer for this- let's read as fast
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

    def write_csv_chunk_to_S3(self, chunk, bucket, s3_key_path):
        """
        Given a string chunk that represents a piece of a CSV file, write
        the chunk to a Boto s3 key

        Parameters
        ----------
        chunk: str
            String blob representing a chunk of a larger CSV.
        bucket: boto.s3.bucket.Bucket
            The bucket we're writing to
        s3_key_path: str
            The key path to write the chunk to
        """
        boto_key = bucket.new_key(s3_key_path)
        boto_key.set_contents_from_string(chunk, encrypt_key=True)

    def generate_copy_statement(self, table_name, manifest_key_path):
    # does the keypath have a complete bucket name and all?
    # write the rest of this function in the morning
        pass

    def copy_table_to_redshift(self, table_name, bucket_name, key_prefix, slices, cleanup=True):

        """
        Write the contents of a Postgres table to Redshift.
        The table will be written to the given bucket under the given
        key prefix. If cleanup=True, all files will be deleted after copy.

        Parameters
        ----------
        table_name: str
            Table name to be written to CSV
        bucket_name: str
            The name of the S3 bucket we're writing to
        key_prefix: str
            The key path within the bucket to write to
        slices: int
            The number of slices in user's Redshift cluster, used to
            split CSV into chunks for parallel data loading
        cleanup: bool, default True
            Specifies whether data will remain in S3 after job completes
        """
        if not self.table_exists(table_name):
            raise ValueError("This table_name does not exist in Redshift!")

        bucket = self.get_bucket(bucket_name)
        if not key_prefix.endswith("/"):
            final_key_prefix = "".join([key_prefix, "/"])
        else:
            final_key_prefix = key_prefix

        try:
            fp, csv_temp_path = mkstemp()
            row_count = self.copy_table_to_csv(table_name, csv_temp_path)
            chunk_generator = self.get_csv_chunk_generator(csv_temp_path,
                                                           row_count, slices)
            backfill_timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
            manifest_entries = []
            for count, chunk in enumerate(chunk_generator):
                chunk_name = "_".join([backfill_timestamp, "chunk",
                                       str(count)])
                complete_key_path = "".join([final_key_prefix,
                                             chunk_name, '.csv'])
                self.write_csv_chunk_to_S3(chunk, bucket, complete_key_path)
                s3_path = (complete_key_path
                           if complete_key_path.startswith("/")
                           else "".join(["/", complete_key_path]))
                manifest_entries.append({
                    'url': "".join(['s3://', bucket.name, s3_path]),
                    'mandatory': 'true'
                })

            manifest = {'entries': manifest_entries}
            manifest_key_path = "".join([final_key_prefix,
                                         backfill_timestamp, ".manifest"])
            from pprint import pprint;import pytest;pytest.set_trace()
            manifest_key = bucket.new_key(manifest_key_path)
            manifest_key.set_contents_from_string(json.dumps(manifest),
                                                  encrypt_key=True)



        finally:
            os.close(fp)
            os.remove(csv_temp_path)
