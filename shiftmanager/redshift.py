#!/usr/bin/env python

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from contextlib import closing, contextmanager
import datetime
from functools import wraps
import gzip
import itertools
import json
import os
import os.path
import random
import string
from subprocess import check_output

import psycopg2

from shiftmanager import util
from shiftmanager.s3 import S3

# Redshift distribution styles
DISTSTYLES_BY_INDEX = {
    0: 'EVEN',
    1: 'KEY',
    8: 'ALL',
}


class S3ConnectionError(Exception):

    def __init__(self, msg):
        """Exception thrown if no S3 creds provided"""
        self.msg = msg


def check_s3_connection(f):
    """
    Check class for S3 connection, try to connect if one is not present.
    """
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        if not self.aws_access_key_id and not self.aws_secret_access_key:
            msg = ("No S3 Credentials present! You can set credentials with "
                   "the'set_aws_credentials' method on the Redshift class.")
            raise S3ConnectionError(msg)
        return f(self, *args, **kwargs)
    return wrapper


class Redshift(S3):
    """Interface to Redshift"""

    def __init__(self, database=None, user=None, password=None, host=None,
                 port=5439, aws_access_key_id=None,
                 aws_secret_access_key=None,):
        """
        The entry point for all Redshift operations in Shiftmanager.
        This class will default to environment params for all arguments.

        For methods requiring S3,  aws keys are not required if you have
        environmental params set for boto to pick up:
        http://boto.readthedocs.org/en/latest/s3_tut.html#creating-a-connection

        Parameters
        ----------
        database: str
            envvar equivalent: PGDATABASE
        user: str
            envvar equivalent: PGUSER
        password: str
            envvar equivalent: PGPASSWORD
        host: str
            envvar equivalent: PGHOST
        port: int
            envvar equivalent: PGPORT
        aws_access_key_id: str
        aws_secret_access_key: str
        """

        self.set_aws_credentials(aws_access_key_id, aws_secret_access_key)
        if aws_access_key_id and aws_secret_access_key:
            self.s3conn = self.get_s3_connection(aws_access_key_id,
                                                 aws_secret_access_key)

        database = database or os.environ.get('PGDATABASE')
        user = user or os.environ.get('PGUSER')
        password = password or os.environ.get('PGPASSWORD')
        host = host or os.environ.get('PGHOST')
        port = port or os.environ.get('PGPORT')

        print('Connecting to Redshift...')
        self.conn = psycopg2.connect(database=database, user=user,
                                     password=password, host=host,
                                     port=port)

        self.cur = self.conn.cursor()

    @staticmethod
    @contextmanager
    def chunked_json_slices(data, slices, directory=None, clean_on_exit=True):
        """
        Given an iterator of dicts, chunk them into `slices` and write to
        temp files on disk. Clean up when leaving scope.

        Parameters
        ----------
        data: iter of dicts
            Iterable of dictionaries to be serialized to chunks
        slices: int
            Number of chunks to generate
        dir: str
            Dir to write chunks to. Will default to $HOME/.shiftmanager/tmp/
        clean_on_exit: bool, default True
            Clean up chunks on disk when context exits

        Returns
        -------
        stamp: str
            Timestamp that prepends the filenames of chunks written to disc
        chunk_files: list
            List of filenames
        """

        # Ensure that files get cleaned up even on raised exception
        try:
            num_data = len(data)
            chunk_range_start = util.linspace(0, num_data, slices)
            chunk_range_end = chunk_range_start[1:]
            chunk_range_end.append(None)
            stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S%f")

            if not directory:
                user_home = os.path.expanduser("~")
                directory = os.path.join(user_home, ".shiftmanager", "tmp")

            if not os.path.exists(directory):
                os.makedirs(directory)

            chunk_files = []
            range_zipper = list(zip(chunk_range_start, chunk_range_end))
            for i, (inclusive, exclusive) in enumerate(range_zipper):

                # Get either a inc/excl slice,
                # or the slice to the end of the range
                if exclusive is not None:
                    sliced = data[inclusive:exclusive]
                else:
                    sliced = data[inclusive:]

                newlined = ""
                for doc in sliced:
                    newlined = "{}{}\n".format(newlined, json.dumps(doc))

                filepath = "{}.gz".format("-".join([stamp, str(i)]))
                write_path = os.path.join(directory, filepath)
                current_fp = gzip.open(write_path, 'wb')
                current_fp.write(newlined.encode("utf-8"))
                current_fp.close()
                chunk_files.append(write_path)

            yield stamp, chunk_files

        finally:
            if clean_on_exit:
                for filepath in chunk_files:
                    os.remove(filepath)

    @staticmethod
    def random_password(length=64):
        """
        Return a strong and valid password for Redshift.

        Constraints:
         - 8 to 64 characters in length.
         - Must contain at least one uppercase letter, one lowercase letter,
           and one number.
         - Can use any printable ASCII characters (ASCII code 33 to 126)
           except ' (single quote), \" (double quote), \\, /, @, or space.
         - See http://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_USER.html

        """
        rand = random.SystemRandom()
        invalid_chars = r'''\/'"@ '''
        valid_chars_set = set(
            string.digits +
            string.ascii_letters +
            string.punctuation
        ) - set(invalid_chars)
        valid_chars = list(valid_chars_set)
        chars = [rand.choice(string.ascii_uppercase),
                 rand.choice(string.ascii_lowercase),
                 rand.choice(string.digits)]
        chars += [rand.choice(valid_chars) for x in range(length - 3)]
        rand.shuffle(chars)
        return ''.join(chars)

    @staticmethod
    def gen_jsonpaths(json_doc, list_idx=None):
        """
        Generate Redshift jsonpath file for given JSON document or dict.

        If an array is present, you can specify an index to use for that
        field in the jsonpaths result. Right now only a single index is
        supported.

        Results will be ordered alphabetically by default.

        Parameters
        ----------
        json_doc: str or dict
            Dictionary or JSON-able string
        list_idx: int
            Index for array position

        Returns
        -------
        Dict
        """
        if isinstance(json_doc, str):
            parsed = json.loads(json_doc)
        else:
            parsed = json_doc

        paths_set = util.recur_dict(set(), parsed, list_idx=list_idx)
        paths_list = list(paths_set)
        paths_list.sort()
        return {"jsonpaths": paths_list}

    def _execute_and_commit(self, statement):
        """Execute and commit given statement"""
        self.cur.execute(statement)
        self.conn.commit()

    def create_user(self, username, password):
        """
        Create a new user account.
        """

        statement = """
        CREATE USER {0}
        PASSWORD '{1}'
        IN GROUP analyticsusers;
        ALTER USER {0}
        SET wlm_query_slot_count TO 4;
        """.format(username, password)

        self._execute_and_commit(statement)

    def set_password(self, username, password):
        """
        Set a user's password.
        """

        statement = """
        ALTER USER {0}
        PASSWORD '{1}';
        """.format(username, password)

        self._execute_and_commit(statement)

    def dedupe(self, table):
        """
        Remove duplicate entries from *table* on *host* using DISTINCT.

        Uses the slowest of the deep copy methods (temp table + truncate),
        but this avoids dropping the original table, so
        all keys and grants on the original table are preserved.

        See
        http://docs.aws.amazon.com/redshift/latest/dg/performing-a-deep-copy.html
        """

        temptable = "{}_copied".format(table)

        statement = """
        -- make all updates to this table block
        LOCK {table};

        -- CREATE TABLE LIKE copies the dist key
        CREATE TEMP TABLE {temptable} (LIKE {table});

        -- move the data
        INSERT INTO {temptable} SELECT DISTINCT * FROM {table};
        DELETE FROM {table};  -- slower than TRUNCATE, but transaction-safe
        INSERT INTO {table} (SELECT * FROM {temptable});
        DROP TABLE {temptable};
        """.format(table=table, temptable=temptable)

        self._execute_and_commit(statement)

    @check_s3_connection
    def copy_json_to_table(self, bucket, keypath, data, jsonpaths, table,
                           slices=32, clean_up_s3=True, local_path=None,
                           clean_up_local=True):
        """
        Given a list of JSON-able dicts, COPY them to the given `table_name`

        This function will partition the blobs into `slices` number of files,
        write them to the s3 `bucket`, write the jsonpaths file, COPY them to
        the table, then optionally clean up everything in the bucket.

        Parameters
        ----------
        bucket: str
            S3 bucket for writes
        keypath: str
            S3 key path for writes
        data: iterable of dicts
            Iterable of JSON-able dicts
        jsonpaths: dict
            Redshift jsonpaths file. If None, will autogenerate with
            alphabetical order
        table: str
            Table name for COPY
        slices: int
            Number of slices in your cluster. This many files will be generated
            on S3 for efficient COPY.
        clean_up_s3: bool
            Clean up S3 bucket after COPY completes
        local_path: str
            Local path to write chunked JSON. Defaults to
            $HOME/.shiftmanager/tmp/
        clean_up_local: bool
            Clean up local chunked JSON after COPY completes.
        """

        print("Fetching S3 bucket {}...".format(bucket))
        bukkit = self.get_bucket(bucket)

        # Keys to clean up
        s3_sweep = []

        # Ensure S3 cleanup on failure
        try:
            with self.chunked_json_slices(data, slices, local_path,
                                          clean_up_local) \
                    as (stamp, file_paths):

                manifest = {"entries": []}

                print("Writing chunks...")
                for path in file_paths:
                    filename = os.path.basename(path)
                    # Strip leading slash
                    if keypath[0] == "/":
                        keypath = keypath[1:]

                    data_keypath = os.path.join(keypath, filename)
                    data_key = bukkit.new_key(data_keypath)
                    s3_sweep.append(data_keypath)

                    with open(path, 'rb') as f:
                        data_key.set_contents_from_file(f)

                    manifest_entry = {
                        "url": "s3://{}/{}".format(bukkit.name, data_keypath),
                        "mandatory": True
                    }
                    manifest["entries"].append(manifest_entry)
                    data_key.close()

                stamped_path = os.path.join(keypath, stamp)

                def single_dict_write(ext, single_data):
                    kpath = "".join([stamped_path, ext])
                    complete_path = "s3://{}/{}".format(bukkit.name, kpath)
                    key = bukkit.new_key(kpath)
                    self.write_dict_to_key(single_data, key, close=True)
                    s3_sweep.append(kpath)
                    return complete_path

                print("Writing .manifest file...")
                mfest_complete_path = single_dict_write(".manifest", manifest)

                print("Writing jsonpaths file...")
                jpaths_complete_path = single_dict_write(".jsonpaths",
                                                         jsonpaths)

            creds = "aws_access_key_id={};aws_secret_access_key={}".format(
                self.aws_access_key_id, self.aws_secret_access_key)

            statement = """
            COPY {table}
            FROM '{manifest_key}'
            CREDENTIALS '{creds}'
            JSON '{jpaths_key}'
            MANIFEST
            GZIP
            TIMEFORMAT 'auto';
            """.format(table=table, manifest_key=mfest_complete_path,
                       creds=creds, jpaths_key=jpaths_complete_path)

            print("Performing COPY...")
            self._execute_and_commit(statement)

        finally:
            if clean_up_s3:
                bukkit.delete_keys(s3_sweep)


class TableDefinitionStatement(object):
    """
    Container for pulling a table definition from Redshift and modifying it.
    """
    def __init__(self, conn, tablename,
                 distkey=None, sortkey=None, diststyle=None, owner=None):
        """
        Pulls creation commands for *tablename* from Redshift.

        The *conn* parameter should be a psycopg2 Connection object.

        The basic CREATE TABLE statement is generated by a call to the
        `pg_dump` executable. Current values of dist and sort keys are
        determined by queries to system tables.

        The other parameters, if set, will modify various properties
        of the table.
        """
        self.tablename = tablename

        output = check_output(['pg_dump', '--schema-only',
                               '--table', tablename,
                               'analytics'])
        lines = output.split('\n')
        self.sets = [line for line in lines
                     if line.startswith('SET ')]
        self.grants = [line for line in lines
                       if line.startswith('REVOKE ')
                       or line.startswith('GRANT ')]
        self.alters = [line for line in lines
                       if line.startswith('ALTER TABLE ')
                       and not line.startswith('ALTER TABLE ONLY')]
        if owner:
            self.alters.append('ALTER TABLE {0} OWNER to {1};'
                               .format(tablename, owner))
            self.grants.append('GRANT ALL ON TABLE {0} TO {1};'
                               .format(tablename, owner))
        create_start_index = [i for i, line in enumerate(lines)
                              if line.startswith('CREATE TABLE ')][0]
        create_end_index = [i for i, line in enumerate(lines)
                            if i > create_start_index
                            and line.startswith(');')][0]
        self.create_lines = lines[create_start_index:create_end_index]

        with closing(conn.cursor()) as cur:
            query_template = """
            SELECT \"column\" from pg_table_def
            WHERE tablename = '{0}'
            AND {1} = {2}
            """
            cur.execute(query_template.format(tablename, 'distkey', "'t'"))
            result = cur.fetchall()
            self.distkey = distkey or result and result[0][0] or None
            cur.execute(query_template.format(tablename, 'sortkey', "1"))
            result = cur.fetchall()
            self.sortkey = sortkey or result and result[0][0] or None

            query_template = """
            SELECT reldiststyle FROM pg_class
            WHERE relname = '{0}'
            """
            cur.execute(query_template.format(tablename))
            self.diststyle = (diststyle or
                              DISTSTYLES_BY_INDEX[cur.fetchone()[0]])

        if distkey and not diststyle:
            self.diststyle = 'KEY'
        if self.diststyle.upper() in ['ALL', 'EVEN']:
            self.distkey = None

    def definition(self):
        """
        Returns the full SQL code to recreate the table.
        """
        names = {
            'tablename': self.tablename,
            'oldtmp': 'temp_old_' + self.tablename,
            'newtmp': 'temp_new_' + self.tablename,
        }

        create_lines = self.create_lines[:]
        create_lines[0] = create_lines[0].replace(self.tablename, '{newtmp}')
        create_lines.append(") DISTSTYLE {0}".format(self.diststyle))
        if self.distkey:
            create_lines.append("  DISTKEY({0})".format(self.distkey))
        if self.sortkey:
            create_lines.append("  SORTKEY({0})".format(self.sortkey))
        create_lines[-1] += ';\n'

        all_lines = itertools.chain(
            # Alter the original table ASAP, so that it gets locked for
            # reads/writes outside the transaction.
            ["SET search_path = analytics, pg_catalog;\n"],
            ["ALTER TABLE {tablename} RENAME TO {oldtmp};"],
            create_lines,
            ["INSERT INTO {newtmp} (SELECT * FROM {oldtmp});"],
            ["ALTER TABLE {newtmp} RENAME TO {tablename};"],
            ["DROP TABLE {oldtmp};\n"],
            self.alters,
            [''],
            self.grants)

        return '\n'.join(all_lines).format(**names)


def random_password():
    """Helper function for password generation"""
    return Redshift.random_password()
