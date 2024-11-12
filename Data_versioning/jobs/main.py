from data_transfer import *


# Creating the warehouse tables in Iceberg that I will insert the transfered data into with the same schema
def build_data_vault(spark):
    spark.sql("""
    CREATE TABLE IF NOT EXISTS my_catalog.data_vault.hub_location (
        location_hk STRING,
        record_source STRING,
        load_date DATE
    )
    USING iceberg
    """)

    spark.sql("""
    CREATE TABLE IF NOT EXISTS my_catalog.data_vault.hub_vendor (
        vendor_hk STRING,
        record_source STRING,
        load_date DATE
    )
    USING iceberg
    """)

    spark.sql("""
    CREATE TABLE IF NOT EXISTS my_catalog.data_vault.hub_trip (
        trip_hk STRING,
        record_source STRING,
        load_date DATE
    )
    USING iceberg
    """)

    spark.sql("""
    CREATE TABLE IF NOT EXISTS my_catalog.data_vault.hub_payment (
        payment_hk STRING,
        payment_type STRING,
        record_source STRING,
        load_date DATE
    )
    USING iceberg
    """)

    spark.sql("""
    CREATE TABLE IF NOT EXISTS my_catalog.data_vault.link_trip_details (
        trip_hk STRING,
        vendor_hk STRING,
        pickup_location_hk STRING,
        dropoff_location_hk STRING,
        payment_hk STRING,
        record_source STRING,
        load_date DATE
    )
    USING iceberg
    """)

    spark.sql("""
    CREATE TABLE IF NOT EXISTS my_catalog.data_vault.satellite_trip_attributes (
        trip_hk STRING,
        passenger_count INT,
        trip_distance DOUBLE,
        ratecodeid INT,
        store_and_fwd_flag STRING,
        pickup_datetime_utc TIMESTAMP,
        dropoff_datetime_utc TIMESTAMP,
        trip_duration_minutes DOUBLE,
        record_source STRING,
        load_date DATE
    )
    USING iceberg
    """)

    spark.sql("""
    CREATE TABLE IF NOT EXISTS my_catalog.data_vault.satellite_payment_details (
        trip_hk STRING,
        fare_amount DOUBLE,
        extra DOUBLE,
        mta_tax DOUBLE,
        tip_amount DOUBLE,
        tolls_amount DOUBLE,
        improvement_surcharge DOUBLE,
        total_amount DOUBLE,
        congestion_surcharge DOUBLE,
        airport_fee DOUBLE,
        record_source STRING,
        load_date DATE
    )
    USING iceberg
    """)


if __name__ == '__main__':
    spark = init_spark()
    df = read_data(spark, '/home/iceberg/data', 'yellow_tripdata_2024-02.parquet')
    df = transfer_data(df)
    df = standardize_data(df)
    data_vault_hubs(df, 'yellow_tripdata_2024-02')
    data_vault_links(df, 'yellow_tripdata_2024-02')
    data_vault_sattelites(df, 'yellow_tripdata_2024-02')
    build_data_vault(spark)
    insert_data(df)