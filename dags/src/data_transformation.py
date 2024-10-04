import pandas as pd


def process_data(df):
    dim_vendor = create_dim_vendor(df)
    dim_ratecode = create_dim_ratecode(df)
    dim_payment_type = create_dim_payment_type(df)
    fact_taxi_trips = create_fact_taxi_trips(df)

    return {
        "dim_vendor": dim_vendor,
        "dim_ratecode": dim_ratecode,
        "dim_payment_type": dim_payment_type,
        "fact_taxi_trips": fact_taxi_trips
    }


def create_dim_vendor(df):
    dim_vendor = pd.DataFrame()
    dim_vendor["vendor_id"] = df["VendorID"].drop_duplicates(
    ).reset_index(drop=True)
    vendor_name_mapping = {
        1: "Vendor A", 2: "Vendor B"
    }
    dim_vendor["vendor_name"] = dim_vendor["vendor_id"].map(
        vendor_name_mapping)
    return dim_vendor


def create_dim_ratecode(df):
    dim_ratecode = pd.DataFrame()
    dim_ratecode["ratecode_id"] = df["RatecodeID"].drop_duplicates(
    ).reset_index(drop=True)
    ratecode_description_mapping = {
        1: "A",
        3: "C",
        2: "B",
        5: "E",
        4: "D",
        6: "F"
    }
    dim_ratecode["ratecode_description"] = dim_ratecode["ratecode_id"].map(
        ratecode_description_mapping)
    return dim_ratecode


def create_dim_payment_type(df):
    dim_payment_type = pd.DataFrame()
    dim_payment_type["payment_type_id"] = df["payment_type"].drop_duplicates(
    ).reset_index(drop=True)
    payment_type_description_mapping = {
        1: "Credit Card",
        2: "Cash",
        3: "No Charge",
        4: "Dispute",
        5: "Unknown",
        6: "Voided Trip",
    }
    dim_payment_type["payment_type_description"] = dim_payment_type["payment_type_id"].map(
        payment_type_description_mapping)
    return dim_payment_type


def create_fact_taxi_trips(df):
    fact_taxi_trips = pd.DataFrame()

    fact_taxi_trips = df[["trip_id", "VendorID", "RatecodeID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "pickup_longitude", "pickup_latitude", "dropoff_longitude",
                          "dropoff_latitude", "passenger_count", "trip_distance", "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "total_amount"]]
    fact_taxi_trips.rename(columns={"tpep_pickup_datetime": "pickup_datetime", "tpep_dropoff_datetime": "dropoff_datetime",
                           "VendorID": "vendor_id", "RatecodeID": "ratecode_id"}, inplace=True)
    return fact_taxi_trips
