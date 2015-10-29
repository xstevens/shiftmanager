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

Then connect to your cluster using the `Redshift` class::

  from shiftmanager import Redshift
  redshift = Redshift(host='myhost', user='myuser', password='mypass')

Or connect using environment variables::

  # Assumes PGHOST, PGUSER, and PGPASSWORD are set.
  from shiftmanager import Redshift
  redshift = Redshift()


Creating Users
--------------

Easily generate strong passwords and create new user accounts::

  password = redshift.random_password()
  # Create a new superuser account
  redshift.create_user('newuser', password, createuser=True)

Deep Copies, Deduping, and Modifying Table Structure
----------------------------------------------------


S3 Stuff
--------

Configuring shiftmanager For Your Environment
---------------------------------------------
