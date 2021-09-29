import pandas as pd
from google.cloud import storage


def fetch_train_data():
    bucket_name = "raw-titanic-data"
    source_file = "train_data.csv"
    train_data_path = "train_data.csv"
    download_blob(bucket_name, source_file, train_data_path)
    df = pd.read_csv(train_data_path)
    return df


def feature_data(dataframe):
    pass

    return dataframe


def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The ID of your GCS object
    # source_blob_name = "storage-object-name"

    # The path to which the file should be downloaded
    # destination_file_name = "local/path/to/file"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)

    # Construct a client side representation of a blob.
    # Note `Bucket.blob` differs from `Bucket.get_blob` as it doesn't retrieve
    # any content from Google Cloud Storage. As we don't need additional data,
    # using `Bucket.blob` is preferred here.
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    print(
        "Downloaded storage object {} from bucket {} to local file {}.".format(
            source_blob_name, bucket_name, destination_file_name
        )
    )


if __name__ == "__main__":
    df = fetch_train_data()
    df = feature_data(df)
