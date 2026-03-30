from pipeline.extract import extract_data
from pipeline.transform import transform_data
from pipeline.load_gcs import upload_file_to_gcs

def main():

    # extract data
    df = extract_data("data/online_retail.csv")

    # transform data
    df = transform_data(df)

    # save as parquet
    df.to_parquet("data/online_retail.parquet", index=False)

    

    upload_file_to_gcs(
    bucket_name="ecommerce-data-lake-hamza",
    local_file_path="data/online_retail.parquet",
    gcs_path="ecommerce/raw/online_retail.parquet"
)
    print("Pipeline completed successfully!")

if __name__ == "__main__":
    main()