"""
Defines a Redshift class which encapsulates a database connection
and utility functions for managing that database.
"""

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
import re
import string
from subprocess import check_output

import sqlalchemy
from sqlalchemy.schema import CreateTable
from sqlalchemy_views import CreateView

from shiftmanager import queries, util
from shiftmanager.privileges import grants_from_privileges
from shiftmanager.s3 import S3

# Redshift distribution styles
DISTSTYLES_BY_INDEX = {
    0: 'EVEN',
    1: 'KEY',
    8: 'ALL',
}


# Regex for SQL identifiers (valid table and column names)
SQL_IDENTIFIER_RE = re.compile(r"""
   [_a-zA-Z][\w$]*  # SQL standard identifier
   |                # or
   (?:"[^"]+")+     # SQL delimited (quoted) identifier
""", re.VERBOSE)


def _get_relation_key(name, schema):
    if schema is None:
        return name
    else:
        return schema + "." + name


def _get_schema_and_relation(key):
    if '.' not in key:
        return (None, key)
    identifiers = SQL_IDENTIFIER_RE.findall(key)
    if len(identifiers) == 1:
        return (None, key)
    elif len(identifiers) == 2:
        return identifiers
    raise ValueError("%s does not look like a valid relation identifier")


def check_s3_connection(f):
    """
    Check class for S3 connection, try to connect if one is not present.
    """
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        if not self.s3conn:
            print("Connecting to S3."
                  "\nIf you have not set your credentials in"
                  " the environment or on the class, you can use the "
                  "set_aws_credentials method")
            self.s3conn = self.get_s3_connection()
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
        self.s3conn = None

        database = database or os.environ.get('PGDATABASE')
        user = user or os.environ.get('PGUSER')
        password = password or os.environ.get('PGPASSWORD')
        host = host or os.environ.get('PGHOST')
        port = port or os.environ.get('PGPORT')

        print('Connecting to Redshift...')
        url = sqlalchemy.engine.url.URL(
            drivername='redshift+psycopg2',
            username=user,
            password=password,
            host=host,
            port=port,
            database=database,
        )
        self.engine = sqlalchemy.create_engine(url)
        self.conn = self.engine.connect()
        self.meta = sqlalchemy.MetaData()
        self.meta.bind = self.engine
        self._all_privileges = {}

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
        return self.conn.execute(sqlalchemy.text(statement))

    def execute(self, statement):
        """
        Execute and commit `statement` using this instance's connection.

        Parameters
        ----------
        statement : str or sqlalchemy.Statement
            The SQL statement or statement batch to execute.
        """
        try:
            statement = sqlalchemy.text(statement)
        except TypeError:
            pass
        return self.conn.execute(statement)

    def create_user(self, username, password, createuser=False, createdb=False,
                    groups=None, valid_until=None, **parameters):
        """
        Create a new user account.
        """
        statement = ("CREATE USER {username} PASSWORD '{password}'"
                     .format(username=username, password=password))
        if createuser:
            statement += " CREATEUSER"
        if createdb:
            statement += " CREATEDB"
        if groups:
            statement += " IN GROUP "
            statement += ', '.join(groups)
        self._execute_and_commit(statement)
        if parameters:
            self.alter_user(username, **parameters)

    def alter_user(self, username, password=None,
                   createuser=None, createdb=None,
                   rename=None, **parameters):
        """
        Alter an existing user account.
        """
        statement = "ALTER USER %s " % username
        options = []
        if password:
            options.append("PASSWORD '%s'" % password)
        if createuser is not None:
            if createuser:
                options.append("CREATEUSER")
            else:
                options.append("NOCREATEUSER")
        if createdb is not None:
            if createdb:
                options.append("CREATEDB")
            else:
                options.append("NOCREATEDB")
        if rename:
            options.append("RENAME TO %s" % rename)
        for param, value in parameters.items():
            if value is None:
                options.append("RESET %s" % param)
            else:
                options.append("SET %s = %s" % (param, value))
        statement += ', '.join(options)
        self._execute_and_commit(statement)

    def set_password(self, username, password):
        """
        Set a user's password.
        """
        statement = queries.set_password.format(username=username,
                                                password=password)
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

            statement = queries.copy_from_s3.format(
                table=table, manifest_key=mfest_complete_path,
                creds=creds, jpaths_key=jpaths_complete_path)

            print("Performing COPY...")
            self._execute_and_commit(statement)

        finally:
            if clean_up_s3:
                bukkit.delete_keys(s3_sweep)

    def get_table_names(self, schema=None, **kwargs):
        """
        Return a list naming all tables and views which exist in `schema`.
        """
        return self.engine.dialect.get_table_names(self.engine, schema,
                                                   **kwargs)

    def reflected_table(self, name, *args, **kwargs):
        """
        Return a sqlalchemy.Table reflected from the database.

        This is simply a convenience method which passes arguments to the
        Table constructor, so you may override various properties of the
        existing table. In particular, Redshift-specific attributes like
        distkey and sorkey can be set through ``redshift_*`` kwargs
        (``redshift_distkey='col1'``,
        ``redshift_interleaved_sortkey=('col1', 'col2')``, etc.)

        The return value is suitable input for the `table_definition`
        or `deep_copy` methods, useful for changing the structure of an
        existing table.

        See Also
        --------
        http://docs.sqlalchemy.org/en/rel_1_0/core/reflection.html#overriding-reflected-columns
        http://redshift-sqlalchemy.readthedocs.org/en/latest/ddl-compiler.html
        """
        kw = kwargs.copy()
        kw['autoload'] = True
        return sqlalchemy.Table(name, self.meta, *args, **kw)

    def reflected_privileges(self, relation, schema=None, use_cache=True):
        """
        Return a str containing the necessary SQL statements
        to recreate all privileges for `relation`.

        Parameters
        ----------
        relation: str or sqlalchemy.Table
            The table or view to reflect
        schema: str
            The database schema in which to look for `relation`
            (only used if `relation` is str)
        use_cache: boolean
            Use cached results for the privilege query, if available
        """
        return ';\n'.join(self._privilege_statements(relation, use_cache))

    def table_definition(self, table, schema=None,
                         copy_privileges=True, use_cache=True):
        """
        Return a str containing the necessary SQL statements
        to recreate `table`.

        Parameters
        ----------
        table: str or sqlalchemy.Table
            The table to reflect
        schema: str
            The database schema in which to look for `table`
            (only used if `table` is str)
        copy_privileges: boolean
            Reflect ownership and grants on the existing table
            and include them in the return value
        use_cache: boolean
            Use cached results for the privilege query, if available
        """
        try:
            CreateTable(table)
        except AttributeError:
            table = self.reflected_table(table, schema=schema)
        statements = [str(CreateTable(table).compile(self.engine))]
        if copy_privileges:
            statements += self._privilege_statements(table, use_cache)
        return ';\n'.join(statements)

    def view_definition(self, view, schema=None,
                        copy_privileges=True, use_cache=True,
                        **kwargs):
        """
        Return a str containing the necessary SQL statements
        to recreate `view`.

        Parameters
        ----------
        view: str or sqlalchemy.Table
            The view to reflect
        schema: str
            The database schema in which to look for `view`
            (only used if `view` is str)
        copy_privileges: boolean
            Reflect ownership and grants on the existing view
            and include them in the return value
        use_cache: boolean
            Use cached results for the privilege query, if available
        """
        try:
            CreateTable(view)
        except AttributeError:
            view = self.reflected_table(view, schema=schema)
        definition = self.engine.dialect.get_view_definition(
            self.engine, name=view.name, schema=view.schema, **kwargs)
        create_statement = str(CreateView(view, definition)
                               .compile(self.engine))
        statements = []
        if copy_privileges:
            statements += self._privilege_statements(view, use_cache)
        return create_statement + ';\n'.join(statements)

    def deep_copy(self, table, schema=None,
                  copy_privileges=True, use_cache=True,
                  cascade=False, distinct=False, **kwargs):
        """
        Return a str containing the necessary SQL statements
        to perform a deep copy of `table`.

        This method can be used to simply sort and clean
        an unvacuumable table, or it can be used to migrate
        to a revised table structure. You can use the
        `reflected_table` method with overrides to generate a new
        table structure, then pass that revised object in as `table`.

        Additional keyword arguments will be captured by `kwargs`
        and passed to the `reflected_table` method.

        Parameters
        ----------
        table: str or sqlalchemy.Table
            The table to reflect
        schema: str
            The database schema in which to look for `table`
            (only used if `table` is str)
        copy_privileges: boolean
            Reflect ownership and grants on the existing table
            and include them in the return value
        use_cache: boolean
            Use cached results for the privilege query, if available
        """
        try:
            CreateTable(table)
        except AttributeError:
            table = self.reflected_table(table, schema=schema,
                                         **kwargs)
        preparer = self.engine.dialect.identifier_preparer
        table_name = preparer.format_table(table)
        print(table_name)
        outgoing_name = table_name + '$outgoing'
        table_definition = self.table_definition(table, None,
                                                 copy_privileges, use_cache)
        insert = "INSERT INTO {table} SELECT "
        if distinct:
            insert += "DISTINCT "
        insert += "* from {outgoing}"
        drop = "DROP TABLE {outgoing}"
        if cascade:
            drop += " CASCADE"
        statements = (
            "LOCK TABLE {table}",
            "ALTER TABLE {table} RENAME TO {outgoing}",
            table_definition,
            insert,
            drop,
        )
        return ';\n'.join(statements).format(table=table_name,
                                             outgoing=outgoing_name)

    def _cache_privileges(self):
        result = self._execute_and_commit(queries.all_privileges)
        self._all_privileges = {}
        for r in result:
            key = _get_relation_key(r.relname, r.schema)
            self._all_privileges[key] = r

    def _privilege_statements(self, relation, use_cache):
        if not use_cache or not self._all_privileges:
            self._cache_privileges()
        priv_info = self._all_privileges[relation.key]
        statements = [("ALTER {type} OWNER TO {owner}"
                       .format(type=priv_info.type.upper(),
                               owner=priv_info.owner_name))]
        statements += grants_from_privileges(priv_info.privileges,
                                             relation.key)
        return statements
