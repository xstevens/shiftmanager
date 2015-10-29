NOTICE
======

THINGS HAVE MOVED AROUND.

You probably want to look at `shiftmanager-simple
<https://github.banksimple.com/klukas/shiftmanager-simple>`_,
a project that provides nice wrappers for using shiftmanager with Simple's
Redshift setup.

We're trying to clean this repo up to get the project to a state where
it's ready to release publicly.

.. figure:: chadvader.jpg
   :alt: Chad Vader, Shift Manager

shiftmanager
============

Admin tools for Amazon Redshift.


Installation
------------

NOT TRUE YET: Install ``shiftmanager`` from PyPI::

  pip install shiftmanager


Basic Usage
-----------

Get started by creating a `Redshift` instance with your cluster details::

  from shiftmanager import Redshift
  redshift = Redshift(host='myhost', user='myuser', password='mypass')

Or provide connection parameters via environment variables::

  # Assumes PGHOST, PGUSER, and PGPASSWORD are set.
  from shiftmanager import Redshift
  redshift = Redshift()

A database connection will be established the first time it's needed::

  >>> statement = redshift.alter_user('chad', wlm_query_slot_count=2)
  Connecting to myhost...

Methods that generate database commands will return a SQL string.
You can review the statement and execute the changes in an additional step::

  >>> statement = redshift.alter_user('chad', wlm_query_slot_count=2)
  >>> print(statement)
  ALTER USER chad SET wlm_query_slot_count = 2
  >>> redshift.execute(statement)

Or execute the statement within the method call by specifying
the ``execute`` keyword argument::

  redshift.alter_user('chad', wlm_query_slot_count=2, execute=True)

In some cases, the returned SQL might not be a single statement
but rather a *batch* of multiple statements.
To provide some safety in these cases, the `execute` method
(whether invoked explicitly or through a keyword argument)
always initiates a transaction, performing a rollback if any
statement produces an error.


Creating Users
--------------

Easily generate strong passwords with `random_password`
and create new user accounts with `create_user`::

  password = redshift.random_password()
  # Create a new superuser account
  statement = redshift.create_user('newuser', password, createuser=True)

To modify existing accounts, use `alter_user`.


Schema Reflection, Deep Copies, Deduping, and Migrations
--------------------------------------------------------

``shiftmanager`` provides several features that reflect existing schema
structure from your cluster, powered by
`sqlalchemy-redshift <https://sqlalchemy-redshift.readthedocs.org>`_,
a Redshift dialect for SQLAlchemy.

Use `table_definition` as a ``pg_dump`` replacement
that understands Redshift-specific structure like distkeys, sortkeys,
and compression encodings::

  >>> batch = redshift.table_definition('my_table', schema='my_schema')
  >>> print(batch)
  CREATE TABLE my_schema.my_table (
          id CHAR(36) ENCODE lzo,
          email_address VARCHAR(256) ENCODE raw
  ) DISTSTYLE KEY DISTKEY (id) SORTKEY (id)

  ;
  ALTER TABLE my_schema.my_table OWNER TO chad;
  GRANT ALL ON my_schema.my_table TO clarissa

Reflecting table structure can be particularly useful when performing
deep copies.
`Amazon's documentation on deep copies
<http://docs.aws.amazon.com/redshift/latest/dg/performing-a-deep-copy.html>`_
suggests four potential strategies, but advises:

  Use the original table DDL. If the CREATE TABLE DDL is available,
  this is the best method.

The `deep_copy` method codifies this best practice, using `table_definition`
behind the scenes to recreate the relevant DDL::

  >>> batch = redshift.deep_copy('my_table', schema='my_schema')
  >>> print(batch)
  LOCK TABLE my_schema.my_table;
  ALTER TABLE my_schema.my_table RENAME TO my_table$outgoing;

  CREATE TABLE my_schema.my_table (
          id CHAR(36) ENCODE lzo,
          email_address VARCHAR(256) ENCODE raw
  ) DISTSTYLE KEY DISTKEY (id) SORTKEY (id)

  ;
  ALTER TABLE my_schema.my_table OWNER TO chad;
  GRANT ALL ON my_schema.my_table TO clarissa

  INSERT INTO my_schema.my_table SELECT * from my_schema.my_table$outgoing;
  DROP TABLE my_schema.my_table$outgoing

To remove duplicate records while recreating the table,
pass in the ``distinct=True`` keyword argument.

`deep_copy` can also be used to migrate an existing table to a new structure,
providing a convenient way to alter distkeys, sortkeys, and column encodings.
Use the `reflected_table` method to generate a modified
`sqlalchemy.schema.Table` object, and pass that in rather than a table name::

  >>> kwargs = dict(redshift_distkey='email_address', redshift_sortkey=('email_address', 'id'))
  >>> table = redshift.reflected_table('my_table', schema='my_schema', **kwargs)
  >>> batch = redshift.deep_copy(table)
  >>> print(batch)
  LOCK TABLE my_schema.my_table;
  ALTER TABLE my_schema.my_table RENAME TO my_table$outgoing;

  CREATE TABLE my_schema.my_table (
          id CHAR(36) ENCODE lzo,
          email_address VARCHAR(256) ENCODE raw
  ) DISTSTYLE KEY DISTKEY (email_address) SORTKEY (email_address, id)

  ;
  ALTER TABLE my_schema.my_table OWNER TO chad;
  GRANT ALL ON my_schema.my_table TO clarissa

  INSERT INTO my_schema.my_table SELECT * from my_schema.my_table$outgoing;
  DROP TABLE my_schema.my_table$outgoing

If you pass ``analyze_compress=True`` to `deep_copy`, compression encodings
will be updated in the resultant table based on results of running
ANALYZE COMPRESSION to determine optimal encodings for the existing data.


S3 Stuff
--------

Configuring shiftmanager For Your Environment
---------------------------------------------


Acknowledgments
---------------

Thanks to `Blame Society Productions <http://youtube.com/blamesocietyfilms>`_
for letting us use a screenshot from *Chad Vader: Day Shift Manager*
as our banner image.
