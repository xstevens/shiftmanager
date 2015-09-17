# shiftmanager

![Chad Vader, Shift Manager](chadvader.jpg)

Tool for managing Redshift: creating users, distributing credentials, migrating tables, etc.

## Installation

It's assumed that you've installed GPG and imported public keys from [knox](https://github.banksimple.com/ops/knox). It's also assumed that you have you have superuser access to the dev and prod Redshift clusters and that your username/password is the same on both.

To install this package, you must first install PostgreSQL's development libraries:

```
brew install postgresql
```

Then:

```
make install
```

## Creating a User Account or Resetting a Password

We distribute Redshift credentials to new users via a document on Google Drive.

Fire up your favorite Python interpreter (`ipython` recommended), but make sure you have environment variables `PGUSER` and `PGPASSWORD` set for the process, probably via `chpst` or [`envcrypt`](https://github.banksimple.com/analytics/sup/blob/master/dev-setup.md#credentials). Your session will look something like:
```python
>>> redshift_username = 'newuser'
>>> gdrive_username = 'newuser' # as in newuser@simple.com
>>> groups = ['analyticsusers']
>>> user_kwargs = dict(groups=groups, wlm_query_slot_count=2)

>>> from shiftmanager import Redshift, creds
>>> devshift = Redshift(host="dev-data-pipeline.cuxrn97vbxid.us-east-1.redshift.amazonaws.com")
>>> prodshift = Redshift(host="prod-data-pipeline.cuxrn97vbxid.us-east-1.redshift.amazonaws.com")
>>> password = prodshift.random_password()

>>> devshift.create_user(redshift_username, password, **user_kwargs)
>>> prodshift.create_user(redshift_username, password, **user_kwargs)

>>> creds.post_user_creds_to_gdrive(gdrive_username, redshift_username, password)
Successfully created creds for user 'newuser' and sent a notification email.
```

Tell the user that their creds are in the mail, and you're done. The doc will get dumped in the top-level folder of your Google Drive account, but feel free to move it into a subdirectory.

The first time you run `post_user_creds_to_gdrive`, a browser window should open for an authorization workflow. Choose your `@simple.com` Google user account and accept the permissions for the "Secure Cred Distribution" app. The result gets cached as `access_token.json` with an expiration date a few months in the future.

If you need to reset a password, use `set_password` method rather than `create_user`.

## Modifying Tables

When a table has too many columns to vacuum, it's time to perform a deep copy:
```python
>>> from shiftmanager import Redshift, creds
>>> devshift = Redshift(host="dev-data-pipeline.cuxrn97vbxid.us-east-1.redshift.amazonaws.com")
>>> statement = devshift.deep_copy('transaction', schema='analytics')
>>> # you should look at the statement and make sure it's reasonable before proceeding
>>> devshift.execute(statement)
```

The same method can be used to dedupe:
```python
>>> from shiftmanager import Redshift
>>> devshift = Redshift(host="dev-data-pipeline.cuxrn97vbxid.us-east-1.redshift.amazonaws.com")
>>> statement = devshift.deep_copy('transaction', schema='analytics', dedupe=True)
>>> # you should look at the statement and make sure it's reasonable before proceeding
>>> devshift.execute(statement)
```

Or to change the structure of the table:
```python
>>> from shiftmanager import Redshift
>>> devshift = Redshift(host="dev-data-pipeline.cuxrn97vbxid.us-east-1.redshift.amazonaws.com")
>>> statement = devshift.deep_copy('transaction', schema='analytics', redshift_distkey='user_id', redshift_sortkey=('user_id', 'timestamp'))
>>> # you should look at the statement and make sure it's reasonable before proceeding
>>> devshift.execute(statement)
```



## Encrypting Something.

We shouldn't really need this anymore.

As in the previous section, fire up a Python interpreter with `PGUSER` and `PGPASSWORD` set:
```python
>>> dev = "dev-data-pipeline.cuxrn97vbxid.us-east-1.redshift.amazonaws.com"
>>> prod = "prod-data-pipeline.cuxrn97vbxid.us-east-1.redshift.amazonaws.com"
>>> service_name = 'newservice'

>>> import shiftmanager as sm
>>> devshift = sm.redshift.Shift(host=dev)
>>> prodshift = sm.redshift.Shift(host=prod)
>>> devpass = devshift.random_password()
>>> prodpass = prodshift.random_password()

>>> devshift.create_user(service_name, devpass)
>>> prodshift.create_user(service_name, prodpass)

>>> cleartext = sm.creds.text_for_service_cred_upload_request(service_name, devpass, prodpass)
>>> ciphertext = sm.creds.encrypted_for_s3_uploaders(cleartext)
Encypting for the following uids:
    Simple Security <security@simple.com>
    Matt Moyer <mattmoyer@gmail.com>
    Matt Moyer (Security Engineer at Simple) <moyer@simple.com>
    Steven Surgnier <steven.surgnier@simple.com>
>>> print(ciphertext)
```

You'll now have a block of ASCII-armored GPG ciphertext on your console. Paste this into a [new gist](https://github.banksimple.com/gist), create the gist as "secret", and send a private message on Slack to @moyer or other appropriate credentialed individual.
