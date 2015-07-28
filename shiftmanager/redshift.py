#!/usr/bin/env python

from contextlib import closing, contextmanager
import itertools
import os
import random
import string
from subprocess import check_output

from boto.s3.connection import S3Connection
import psycopg2

# Redshift distribution styles
DISTSTYLES_BY_INDEX = {
    0: 'EVEN',
    1: 'KEY',
    8: 'ALL',
}


class Shift(object):
    """Interface to Redshift and S3"""

    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None,
                 database=None, user=None, password=None, host=None,
                 port=5439, connect_s3=True):
        """
        The entry point for all Redshift and S3 operations in Shiftmanager.
        This class will default to environment params for all arguments.

        The aws keys are not required if you have environmental params set
        for boto to pick up:
        http://boto.readthedocs.org/en/latest/s3_tut.html#creating-a-connection

        Parameters
        ----------
        aws_access_key_id: str
        aws_secret_access_key: str
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
        connect_s3: bool
            Make S3 connection. If False, Redshift methods will still work
        """

        if connect_s3:
            if aws_access_key_id and aws_secret_access_key:
                self.s3conn = S3Connection(aws_access_key_id,
                                           aws_secret_access_key)
            else:
                self.s3conn = S3Connection()

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
        self.bucket_cache = {}

    @staticmethod
    @contextmanager
    def redshift_transaction(self, database=None, user=None, password=None,
                             host=None, port=5439):
        """
        Helper function for wrapping a connection in a context block

        Parameters
        ----------
        database: str
        user: str
        password: str
        host: str
        port: int
        """
        database = database or "public"
        with closing(psycopg2.connect(database=database, user=user,
                                      password=password, host=host,
                                      port=port)) as conn:
            cur = conn.cursor()

            # Make sure we create tables in the `database` schema
            cur.execute("SET search_path = {}".format(database))

            # Return the connection and cursor to the calling function
            yield conn, cur

            conn.commit()

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
            string.letters +
            string.punctuation
        ) - set(invalid_chars)
        valid_chars = list(valid_chars_set)
        chars = [rand.choice(string.ascii_uppercase),
                 rand.choice(string.ascii_lowercase),
                 rand.choice(string.digits)]
        chars += [rand.choice(valid_chars) for x in xrange(length - 3)]
        rand.shuffle(chars)
        return ''.join(chars)

    def _get_bucket_from_cache(self, buckpath):
        """Get bucket from cache, or add to cache if does not exist"""
        if buckpath not in self.bucket_cache:
            self.bucket_cache[buckpath] = self.s3conn.get_bucket(buckpath)
        return self.bucket_cache[buckpath]

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

    def copy_json_to_table(self, bucket, json_iter, table_name, slices=32,
                           clean_up=True):
        """
        Given a list of JSON blobs, COPY them to the given `table_name`

        This function will partition the blobs into `slices` number of files,
        write them to the s3 `bucket`, create a jsonpaths file, COPY them to
        the table, then optionally clean up everything in the bucket.

        Parameters
        ----------
        bucket: str
            S3 bucket for writes
        json_list: iterable
            Iterable of JSON documents
        table_name: str
            Table name for COPY
        slices: int
            Number of slices in your cluster. This many files will be generated
            on S3 for efficient COPY.
        clean_up: bool
            Clean up S3 bucket after COPY completes
        """
        pass


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
