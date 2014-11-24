# shiftmanager

![Chad Vader, Shift Manager](chadvader.jpg)

Tool for creating Redshift users, distributing credentials, etc.

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

## Creating a User Account

We distribute Redshift credentials to new users via a document on Google Drive.

Fire up your favorite Python interpreter (`ipython` recommended), but make sure you have environment variables `PGUSER` and `PGPASSWORD` set for the process, probably via `chpst` or [`envcrypt`](https://github.banksimple.com/analytics/sup/blob/master/dev-setup.md#credentials). Your session will look something like:
```python
>>> redshift_username = 'newuser'
>>> gdrive_username = 'newuser' # as in newuser@simple.com

>>> import shiftmanager as manage
>>> password = manage.random_password()

>>> manage.create_user(manage.DB_HOSTS['dev' ], redshift_username, password)
>>> manage.create_user(manage.DB_HOSTS['prod'], redshift_username, password)

>>> manage.post_user_creds_to_gdrive(gdrive_username, redshift_username, password)
Successfully created creds for user 'newuser' and sent a notification email.
```

Tell the user that their creds are in the mail, and you're done. The doc will get dumped in the top-level folder of your Google Drive account, but feel free to move it into a subdirectory.

The first time you run `post_user_creds_to_gdrive`, a browser window should open for an authorization workflow. Choose your `@simple.com` Google user account and accept the permissions for the "Secure Cred Distribution" app. The result gets cached as `access_token.json` with an expiration date a few months in the future.

## Creating a Service Account

When a service needs to access Redshift, we create accounts under the name of the service and then store the associated passwords in S3. In order to get the passwords in S3, the security team prefers that we GPG-encode a text document containing the passwords, and publish that in a private gist.

As in the previous section, fire up a Python interpreter with `PGUSER` and `PGPASSWORD` set:
```python
>>> service_name = 'newservice'

>>> import shiftmanager as manage
>>> devpass = manage.random_password()
>>> prodpass = manage.random_password()

>>> manage.create_user(manage.DB_HOSTS['dev' ], service_name, devpass)
>>> manage.create_user(manage.DB_HOSTS['prod'], service_name, prodpass)

>>> cleartext = manage.text_for_service_cred_upload_request(service, devpass, prodpass)
>>> ciphertext = manage.encrypted_for_s3_uploaders(cleartext)
Encypting for the following uids:
    Simple Security <security@simple.com>
    Matt Moyer <mattmoyer@gmail.com>
    Matt Moyer (Security Engineer at Simple) <moyer@simple.com>
    Steven Surgnier <steven.surgnier@simple.com>
    Michael Ehlert (SKANKPIMPLE!) <michael@simple.com>
>>> print(ciphertext)
```

You'll now have a block of ASCII-armored GPG ciphertext on your console. Paste this into a [new gist](https://github.banksimple.com/gist), create the gist as "secret", and send a private message on IRC to @moyer or @mehlert asking them to upload to S3.
