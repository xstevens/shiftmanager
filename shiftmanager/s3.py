#!/usr/bin/env python

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from io import StringIO
import json
from ssl import CertificateError

from boto.s3.connection import S3Connection
from boto.s3.connection import OrdinaryCallingFormat

from shiftmanager.util import memoize


class S3(object):
    """S3 Interface Class"""

    def get_s3_connection(self, ordinary_calling_fmt=False):
        """
        Get new S3 Connection

        Parameters
        ----------
        ordinary_calling_fmt: bool
            Initialize connection with OrdinaryCallingFormat
        """

        kwargs = {}
        # Workaround https://github.com/boto/boto/issues/2836
        if ordinary_calling_fmt:
            kwargs["calling_format"] = OrdinaryCallingFormat()

        if self.aws_access_key_id and self.aws_secret_access_key:
            s3conn = S3Connection(self.aws_access_key_id,
                                  self.aws_secret_access_key,
                                  **kwargs)
        else:
            s3conn = S3Connection(**kwargs)

        return s3conn

    def write_dict_to_key(self, data, key, close=False):
        """
        Given a Boto S3 Key, write a given dict to that key as JSON.

        Parameters
        ----------
        data: dict
        key: boto.s3.Key
        close: bool, default False
            Close key after write
        """
        fp = StringIO()
        fp.write(json.dumps(data, ensure_ascii=False))
        fp.seek(0)
        key.set_contents_from_file(fp)
        if close:
            key.close()
        return key

    @memoize
    def get_bucket(self, bucket_name):
        """
        Get boto.s3.bucket. Caches existing buckets.

        Parameters
        ----------
        bucket_name: str
        """
        try:
            bucket = self.s3conn.get_bucket(bucket_name)
        except CertificateError as e:
            # Addressing https://github.com/boto/boto/issues/2836
            dot_msg = ("doesn't match either of '*.s3.amazonaws.com',"
                       " 's3.amazonaws.com'")
            if dot_msg in e.message:
                self.s3conn = self.get_s3_connection(ordinary_calling_fmt=True)
                bucket = self.s3conn.get_bucket(bucket_name)
            else:
                raise

        return bucket
