from google.cloud import storage


def upload_file_to_gcs():
    bucket_name = "ecommerce-data-lake-hamza"

    #  upload processed file (NOT raw CSV)
    local_file_path = "/app/project/ecommerce_pipeline/data/processed.parquet"

    #  structured path in GCS
    gcs_path = "ecommerce/processed/processed.parquet"

    # Initialize client
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)

    blob.upload_from_filename(local_file_path)

    print(f" Uploaded to gs://{bucket_name}/{gcs_path}")


if __name__ == "__main__":
    upload_file_to_gcs()