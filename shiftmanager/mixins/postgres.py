"""
Mixin classes for working with Postgres database exports
"""

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import codecs
from datetime import datetime
import gzip
import json
import os

import psycopg2

from shiftmanager.memoized_property import memoized_property
from shiftmanager.mixins.s3 import S3Mixin


class PostgresMixin(S3Mixin):
    """The Postgres interaction base class for `Redshift`."""

    @memoized_property
    def pg_connection(self):
        """A `psycopg2.connect` connection to Postgres.

        Instantiation is delayed until the object is first used.
        """

        print("Connecting to %s..." % self.pg_args['host'])
        return psycopg2.connect(**self.pg_args)

    def pg_execute_and_commit_single_statement(self, statement):
        """Execute single Postgres statement"""
        with self.pg_connection as conn:
            with conn.cursor() as cur:
                cur.execute(statement)

    def create_pg_connection(self, **kwargs):
        """
        Create a `psycopg2.connect` connection to Redshift.

        See https://www.postgresql.org/docs/current/static/\
libpq-connect.html#LIBPQ-PARAMKEYWORDS
        for supported parameters.
        """

        # Use 'localhost' as default host rather than unix socket
        if 'host' not in kwargs:
            kwargs['host'] = 'localhost'

        self.pg_args = kwargs
        return self.pg_connection

    def pg_copy_table_to_csv(self, csv_file_path, pg_table_name=None,
                             pg_select_statement=None):
        """
        Use Postgres to COPY the given table_name to a csv file at the given
        csv_path.

        Additionally, fetch the row count of the given table_name for
        further processing.

        Parameters
        ----------
        csv_file_path: str
            File path for the CSV to be written to by Postgres
        pg_table_name: str
            Optional Postgres table name to be written to CSV if user
            does not want to specify subset
        pg_select_statement: str
            Optional select statement if user wants to specify subset of table


        Returns
        -------
        row_count: int
        """
        copy = ' '.join([
            "COPY {pg_table_or_select}",
            "TO PROGRAM 'gzip > {csv_fp}'",
            "DELIMITER ','",
            "FORCE QUOTE *",
            "CSV;"])

        if pg_select_statement is None and pg_table_name is not None:

            formatted_statement = copy.format(
                pg_table_or_select=pg_table_name,
                csv_fp=csv_file_path)

        elif pg_select_statement is not None and pg_table_name is None:

            if not (pg_select_statement.startswith("(") and
                    pg_select_statement.endswith(")")):
                pg_select_statement = "(" + pg_select_statement + ")"

            formatted_statement = copy.format(
                pg_table_or_select=pg_select_statement,
                csv_fp=csv_file_path)

        else:
            ValueError("Please enter a table name or a select statement.")

        with self.pg_connection as conn:
            with conn.cursor() as cur:
                cur.execute(formatted_statement)
                row_count = cur.rowcount

        return row_count

    def get_csv_chunk_generator(self, csv_file_path,
                                chunk_max_bytes=134217728):
        """
        Given the csv_file_path and an optional max_bytes_per_chunk, yield
        string chunks of roughly that size (default: 128MB)

        Parameters
        ----------
        csv_file_path: str
            File path for the CSV written by Postgres
        chunk_max_bytes: int
            The approximate maximum number of bytes per chunk

        Yields
        ------
        str
        """
        with gzip.open(csv_file_path, 'rb') as zf:
            reader = codecs.getreader('utf-8')
            chunk_count = 1
            chunk_lines = []
            for line in reader(zf):
                chunk_lines.append(line)
                if zf.tell() > (chunk_max_bytes * chunk_count):
                    yield u"".join(chunk_lines)
                    chunk_lines = []
                    chunk_count += 1

    @property
    def aws_credentials(self):
        if self.aws_account_id and self.aws_role_name:
            template = ('aws_iam_role=arn:aws:iam::'
                        '{aws_account_id}:role/{role_name}')
            return template.format(aws_account_id=self.aws_account_id,
                                   role_name=self.aws_role_name)
        else:
            key_id = 'aws_access_key_id={};'.format(self.aws_access_key_id)
            secret_key_id = 'aws_secret_access_key={}'.format(
                self.aws_secret_access_key)
            template = '{key_id}{secret_key_id}'
            if self.security_token:
                template += ";token={security_token}".format(
                    security_token=self.security_token)
            return template.format(key_id=key_id,
                                   secret_key_id=secret_key_id)

    def _create_copy_statement(self, table_name, manifest_key_path):
        """Create Redshift copy statement for given table_name and
        the provided manifest_key_path.

        Parameters
        ----------
        table_name: str
            Redshift table name to COPY to
        manifest_key_path: str
            Complete S3 path to .manifest file

        Returns
        -------
        str
        """
        return """copy {table_name}
                  from '{manifest_key_path}'
                  credentials '{aws_credentials}'
                  manifest
                  csv
                  gzip;""".format(table_name=table_name, 
                                  manifest_key_path=manifest_key_path,
                                  aws_credentials=self.aws_credentials)

    def copy_table_to_redshift(self, redshift_table_name,
                               bucket_name, key_prefix, slices,
                               pg_table_name=None, pg_select_statement=None,
                               temp_file_dir=None, cleanup_s3=True,
                               manifest_max_keys=64):
        """
        Write the contents of a Postgres table to Redshift.
        Write the table to the given bucket under the given
        key prefix.

        Parameters
        ----------
        redshift_table_name: str
            Redshift table to which CSVs are to be written
        bucket_name: str
            The name of the S3 bucket to be written to
        key_prefix: str
            The key path within the bucket to write to
        slices: int
            The number of slices in user's Redshift cluster, used to
            split CSV into chunks for parallel data loading
        pg_table_name: str
            Optional Postgres table name to be written to CSV if user
            does not want to specify subset
        pg_select_statement: str
            Optional select statement if user wants to specify subset of table
        temp_file_dir: str
            Optional Specify location of temporary files
        cleanup_s3: bool
            Optional Clean up S3 location on failure. Defaults to True.
        """
        if not self.table_exists(redshift_table_name):
            raise ValueError("This table_name does not exist in Redshift!")

        bucket = self.get_bucket(bucket_name)
        # All keys written to S3 in the event cleanup is needed
        all_s3_keys = []

        if not key_prefix.endswith("/"):
            final_key_prefix = "".join([key_prefix, "/"])
        else:
            final_key_prefix = key_prefix

        csv_temp_path = os.path.join(temp_file_dir, redshift_table_name + ".gz")
        use_existing = False
        if os.path.exists(csv_temp_path):
            answer = input("Would you like to use the existing database dump file ([y]/n)? ")
            use_existing = answer == 'n' or answer == 'no'

        if not use_existing:
            self.pg_copy_table_to_csv(
                csv_temp_path, pg_table_name=pg_table_name,
                pg_select_statement=pg_select_statement)
        chunk_generator = self.get_csv_chunk_generator(csv_temp_path)
        backfill_timestamp = datetime.utcnow().strftime(
            "%Y-%m-%d_%H-%M-%S")

        manifest_entries = []
        for count, chunk in enumerate(chunk_generator):
            chunk_name = "_".join([backfill_timestamp, "chunk",
                                   str(count)])
            # write the chunk gzip compressed to the local filesystem
            compressed_chunk_path = os.path.join(temp_file_dir,
                                                 chunk_name + '.gz')
            with gzip.open(compressed_chunk_path, 'wt', encoding='utf-8') as ccf:
                ccf.write(chunk)
            complete_key_path = "".join([final_key_prefix,
                                         chunk_name, '.csv.gz'])
            # upload compressed chunk file to S3
            print('Writing {} to S3 {} ...'.format(compressed_chunk_path,
                                                   complete_key_path))
            self.write_file_to_s3(compressed_chunk_path, bucket,
                                  complete_key_path)
            # remove chunk file after uploaded to s3
            os.remove(compressed_chunk_path)

            all_s3_keys.append(complete_key_path)

            s3_path = (complete_key_path
                       if complete_key_path.startswith("/")
                       else "".join(["/", complete_key_path]))
            manifest_entries.append({
                'url': "".join(['s3://', bucket.name, s3_path]),
                'mandatory': True
            })

        start_idx = 0
        num_entries = len(manifest_entries)
        while (start_idx < num_entries):
            end_idx = min(num_entries, start_idx + manifest_max_keys)
            print("Using manifest_entries: start=%d, end=%d" % 
                  (start_idx, end_idx))
            entries = manifest_entries[start_idx:end_idx]
            manifest = {'entries': entries}
            manifest_key_path = "".join([final_key_prefix,
                                         backfill_timestamp,
                                         str(start_idx), "-", str(end_idx),
                                         ".manifest"])
            manifest_key = bucket.new_key(manifest_key_path)
            all_s3_keys.append(manifest_key_path)

            print('Writing .manifest file to S3...')
            manifest_key.set_contents_from_string(json.dumps(manifest),
                                                  encrypt_key=True)
            complete_manifest_path = "".join(['s3://', bucket.name,
                                              manifest_key_path])
            copy_statement = self._create_copy_statement(
                redshift_table_name, complete_manifest_path)

            print('Copying from S3 to Redshift...')
            try:
                self.execute(copy_statement)
                start_idx = end_idx
            except:
                # Clean up S3 bucket in the event of any exception
                if cleanup_s3:
                    print("Error writing to Redshift! Cleaning up S3...")
                    for key in all_s3_keys:
                        bucket.delete_key(key)
                raise
        os.remove(csv_temp_path)
