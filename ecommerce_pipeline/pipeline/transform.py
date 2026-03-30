import pandas as pd

def transform_data():
    # Read raw data
    df = pd.read_csv("/app/project/ecommerce_pipeline/data/online_retail.csv")

    # Transformations
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
    df["revenue"] = df["Quantity"] * df["UnitPrice"]

    # Save processed file
    output_path = "/app/project/ecommerce_pipeline/data/processed.parquet"
    df.to_parquet(output_path)

    print(f"Transformed data saved to {output_path}")

if __name__ == "__main__":
    transform_data()