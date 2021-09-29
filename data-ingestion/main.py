import pathlib

import pandas as pd
# Imports the Google Cloud client library
from google.cloud import storage

PATH = pathlib.Path().resolve()


def ingest(source_url):
    titanic_data = pd.read_csv(source_url)
    train_data_path = f"{PATH}/titanic_train_data.csv"
    titanic_data.to_csv(train_data_path)

    bucket_name = "raw-titanic-data"

    upload_blob(bucket_name, train_data_path, "train_data.csv")

    print("done")

    return


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )


if __name__ == "__main__":
    ingest(
        "https://storage.googleapis.com/kagglesdsdata/competitions/3136/26502/train.csv?GoogleAccessId=web-data@kaggle-161607.iam.gserviceaccount.com&Expires=1632327157&Signature=gwa3Sv1o%2FtbCa7pqzsWM4rnEKta168eZOl2Ox8ADtfrhJRdXV9ZbtD0YLfMkFGbJujUeGiQGw38r1h3RBKB9g4lfuC8wZObv9Ct9%2FGcE7MGqKtV4ljyvnRNnB6m1BlUA7FPCcq8z5pRwShyEadwl5V8urihPOG5OCmxZeTwTwAb%2BP1XUHy7J9NzCfbPsxbemCrcsUgXjz32hHR5f6uxEoJZUREgbQDCfJtSYv3qx7baQ2w71SIv55g2VRID4p9%2BFiyOZnhDvWV9ruKYOXWXoudE2MyaQHi3XWVx2Ha%2BPUhNEVK4teEY1gkFAwMfVQjHM2WZqRDXzwzoERsgB0d3SHA%3D%3D&response-content-disposition=attachment%3B+filename%3Dtrain.csv"
    )
