#!/usr/bin/env python

import argparse

import oauth2client
from oauth2client import tools
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage

import httplib2
import apiclient.discovery
import apiclient.http
import apiclient.errors
import pkg_resources

import gnupg

from tempfile import NamedTemporaryFile

# OAuth 2.0 scope that will be authorized.
# Check https://developers.google.com/drive/scopes for all available scopes.
OAUTH2_SCOPE = 'https://www.googleapis.com/auth/drive'

# Location of the client secrets.
CLIENT_SECRETS = pkg_resources.resource_filename('shiftmanager',
                                                 'data/client_secrets.json')

# Local access token file
STORAGE_FILENAME = pkg_resources.resource_filename('shiftmanager',
                                                   'data/access_token.json')

# Metadata about the file.
MIMETYPE = 'text/plain'
TITLE = 'Redshift Creds - {email}'
DESCRIPTION = 'Instructions and creds for accessing the Redshift cluster'

# Credentials text file template
CREDSFILE_TEMPLATE = """\
# Redshift credentials for {gdrive_username}@simple.com

username:
{redshift_username}

password:
{password}

Redshift cluster hostname:
prod-data-pipeline.cuxrn97vbxid.us-east-1.redshift.amazonaws.com

Port number:
5439

To get started, take a look at the analyst setup guide on GitHub:
https://github.banksimple.com/analytics/sup/blob/master/analyst-setup.md

For accessing Redshift through command-line tools, you may want to set
the following environment variables:

PGUSER={redshift_username}
PGPASSWORD='{password}'
PGHOST=prod-data-pipeline.cuxrn97vbxid.us-east-1.redshift.amazonaws.com
PGPORT=5439
"""

ENCRYPTED_DOC_BOILERPLATE = """\
To decrypt, paste the following into a Terminal.app window
(include everything from 'gpg' to 'EOF'):

gpg -d <<EOF
{cyphertext}
EOF
"""

SERVICE_S3_UPLOAD_REQUEST_TEMPLATE = """\
We'd like to get some PGPASSWORD files into S3 for {service_name}:

echo "{devpass}" > PGPASSWORD
aws s3 cp PGPASSWORD s3://com-simple-dev/credentials/{service_name}/service-keys/PGPASSWORD

echo "{prodpass}" > PGPASSWORD
aws s3 cp PGPASSWORD s3://com-simple-prod/credentials/{service_name}/service-keys/PGPASSWORD
"""  # noqa

S3_UPLOADER_FINGERPRINTS = [
    "E034918ABA561DD06CAF46D8C4721EA079FF66A9",  # security@simple
    "4D51E7FEA4886C3A6BAA3A4C09A69F598386962C",  # moyer
    "BF7F08BCCB9A9427094E32876DBA10920728A5D5",  # steven
    "BEB36E5FB5B7572F7A8714E2F0D203EF70A4E423",  # max
    "6C6F4032059B07246503843DA54195C6AEA6CF00",  # xavier
    "06B067C2680D67E7B646989A66A7908C87CCE7F0",  # rob
    "8700648A0ADB63CFDF18B6388C29A9E5E8660399",  # klukas
]

KEY_SERVER = 'hkps://hkps.pool.sks-keyservers.net'


def post_user_creds_to_gdrive(gdrive_username, redshift_username, password):
    """
    Uploads a text file to Google Drive, shared with *gdrive_username*.

    The text is taken from a template, with usernames and passwords inserted
    as appropriate.
    """

    # In case the user included '@simple.com' in the username
    gdrive_username = gdrive_username.split('@')[0]

    email = '{0}@simple.com'.format(gdrive_username)

    flow = flow_from_clientsecrets(
        CLIENT_SECRETS,
        scope=OAUTH2_SCOPE,
        redirect_uri=oauth2client.client.OOB_CALLBACK_URN)

    storage = Storage(STORAGE_FILENAME)

    # Yeah, this isn't a command-line app, but this argparse flow was
    # the easiest method available.
    parser = argparse.ArgumentParser(parents=[tools.argparser])
    flags = parser.parse_args()

    credentials = storage.get()
    if credentials is None or credentials.invalid:
        credentials = tools.run_flow(flow, storage, flags)

    # Create an authorized Drive API client.
    http = httplib2.Http()
    credentials.authorize(http)
    drive_service = apiclient.discovery.build('drive', 'v2', http=http)

    # The body contains the metadata for the file.
    body = {
        'title': TITLE.format(**locals()),
        'description': DESCRIPTION.format(**locals()),
    }

    with NamedTemporaryFile() as gdoc:

        gdoc.write(CREDSFILE_TEMPLATE.format(**locals()))
        # Seek to the beginning so that there's something to read.
        gdoc.seek(0)

        # Insert a file. Files are comprised of contents and metadata.
        # MediaFileUpload abstracts uploading file contents from a
        # file on disk.
        media_body = apiclient.http.MediaFileUpload(
            gdoc.name,
            mimetype=MIMETYPE,
            resumable=True,
        )

        # Perform the request.
        new_file = drive_service.files().insert(
            body=body,
            media_body=media_body
        ).execute()

    # Add the target user as a reader.
    new_permission = {
        'value': email,
        'type': 'user',
        'role': 'reader',
    }
    drive_service.permissions().insert(
        fileId=new_file['id'],
        body=new_permission,
        emailMessage="",
    ).execute()

    print ("Successfully created creds for user '{0}' "
           "and sent a notification email.").format(redshift_username)


def encrypted_for_s3_uploaders(text):
    """
    Returns *text* encrypted with the public keys of folks known
    to have S3 upload privileges.
    """

    gpg = gnupg.GPG()

    keys = [key for key in gpg.list_keys()
            if key['fingerprint'] in S3_UPLOADER_FINGERPRINTS]

    print("Encypting for the following uids:")
    for key in keys:
        for uid in key['uids']:
            print("    " + uid)

    encrypted_data = gpg.encrypt(
        text,
        S3_UPLOADER_FINGERPRINTS,
        always_trust=True)

    return str(encrypted_data)


def encrypted_for_user(text, username):

    email = '{0}@simple.com'.format(username)
    gpg = gnupg.GPG()

    keys = [key for key in gpg.list_keys()
            if key_contains_email(key, email)]

    if not keys:
        searched_keys = gpg.search_keys(email, KEY_SERVER)
        keys = [key for key in searched_keys
                if key_contains_email(key, email)]
        if keys:
            key_ids = [key['keyid'] for key in keys]
            gpg.recv_keys(KEY_SERVER, *key_ids)
            # The user's keys are now imported, so try again
            return encrypted_for_user(text, username)
        else:
            raise ValueError("No key found for '{0}'".format(email))

    encrypted_data = gpg.encrypt(
        text,
        [key['fingerprint'] for key in keys],
        always_trust=True)

    return str(encrypted_data)


def key_contains_email(key_dict, email):
    target = '<' + email + '>'
    return any(uid.endswith(target) for uid in key_dict['uids'])


def text_for_service_cred_upload_request(service_name, devpass, prodpass):
    """
    Returns a string containing a request to security folks to upload
    passwords to appropriate locations in S3.
    """
    return SERVICE_S3_UPLOAD_REQUEST_TEMPLATE.format(**locals())
