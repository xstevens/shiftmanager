# -*- coding: utf-8 -*-
from redshift import random_password, create_user, set_password, dedupe
from creds import (post_user_creds_to_gdrive,
                   text_for_service_cred_upload_request,
                   encrypted_for_s3_uploaders)
