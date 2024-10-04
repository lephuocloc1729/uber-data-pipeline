def load_data(processed_tables):
    for table_name, table in processed_tables.items():
        table.to_csv(f"/opt/airflow/data/processed/{table_name}.csv")
