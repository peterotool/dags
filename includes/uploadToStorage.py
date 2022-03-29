#!/usr/bin/env python3
import os

import pandas as pd
from google.cloud import storage


def get_client(path_credential_key=None):
    if path_credential_key is None:
        path_credential_key = os.environ.get("PATH_CREDENTIAL_KEY")
    else:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path_credential_key

    if os.path.exists(path_credential_key):
        return storage.client.Client().from_service_account_json(path_credential_key)
    else:
        print("The path '{}' doesn't exist".format(path_credential_key))
        return None


def download_from_storage(bucket_name,
                          source_blob_name,
                          destination_file_name,
                          path_credential_key=None):
    storage_client = get_client(path_credential_key)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    print(
        "Blob {} downloaded to {}.".format(
            source_blob_name, destination_file_name
        )
    )


def upload_to_storage(bucket_name,
                      source_file_name,
                      destination_blob_name,
                      path_credential_key,
                      mime_type=None):
    storage_client = get_client(path_credential_key)  # config/conf/operation/edms-data-manager.json
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    if mime_type is not None:
        blob.upload_from_filename(source_file_name, content_type=mime_type)
    else:
        blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )
