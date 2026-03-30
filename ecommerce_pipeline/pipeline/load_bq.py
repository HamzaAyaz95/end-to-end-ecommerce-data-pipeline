from google.cloud import bigquery


def load_to_bq():
    client = bigquery.Client()

    table_id = "kestra-zoomcamp-v2.ecommerce.online_retail"

    uri = "gs://ecommerce-data-lake-hamza/ecommerce/raw/online_retail.parquet"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition="WRITE_TRUNCATE",
    )

    load_job = client.load_table_from_uri(
        uri,
        table_id,
        job_config=job_config
    )

    load_job.result()

    print("Loaded to BigQuery")


if __name__ == "__main__":
    load_to_bq()