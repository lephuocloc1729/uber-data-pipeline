import hashlib
import pandas as pd


def ingest_data(path="/opt/airflow/data/raw/uber_data.csv"):
    df = pd.read_csv(path)

    # Function to generate a hash-based trip_id
    def generate_trip_id(row):
        trip_info = f"{row['tpep_pickup_datetime']}-{row['tpep_dropoff_datetime']}-{row['VendorID']}"
        return hashlib.md5(trip_info.encode()).hexdigest()

    # Applying the function to each row to generate trip_id
    df["trip_id"] = df.apply(generate_trip_id, axis=1)

    return df
