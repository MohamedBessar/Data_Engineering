from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, FloatType, StringType
from pyspark.sql.functions import when, lit, current_date, to_utc_timestamp, sha2, concat, lit, current_date, col, unix_timestamp


# Init the spark session that I will use
# The path written in the docker compose volumes as ./warehouse is /home/iceberg/warehouse
def init_spark():
    spark = SparkSession.builder \
        .appName("DataVaultIceberg") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", "home/iceberg/warehouse")  \
        .getOrCreate()
    return spark

def read_data(spark, path, file_name):
    df = spark.read.parquet(f'{path}/{file_name}')
    return df

def transfer_data(df):
    # Rename columns to lowercase and snake_case
    df = df.toDF(*[c.lower() for c in df.columns])

    # Convert column types
    df = df.withColumn("passenger_count", df["passenger_count"].cast(IntegerType())) \
        .withColumn("trip_distance", df["trip_distance"].cast(FloatType())) \
        .withColumn("vendorid", df["vendorid"].cast(IntegerType())) \
        .withColumn("ratecodeid", df["ratecodeid"].cast(IntegerType())) \
        .withColumn("pulocationid", df["pulocationid"].cast(IntegerType())) \
        .withColumn("dolocationid", df["dolocationid"].cast(IntegerType())) \
        .withColumn("payment_type", df["payment_type"].cast(IntegerType()))

    # Fill missing values with default values
    df = df.fillna({
        'passenger_count': 1,          # Default to 1 passenger if missing
        'trip_distance': 0.0,          # Default to 0 if distance is missing
        'fare_amount': 0.0,
        'extra': 0.0,
        'mta_tax': 0.0,
        'tip_amount': 0.0,
        'tolls_amount': 0.0,
        'improvement_surcharge': 0.0,
        'total_amount': 0.0,
        'congestion_surcharge': 0.0,
        'airport_fee': 0.0
    })
    return df

def standardize_data(df):
    # Standardize categorical fields
    df = df.withColumn("store_and_fwd_flag", when(df["store_and_fwd_flag"] == "Y", "Yes")
                                            .when(df["store_and_fwd_flag"] == "N", "No")
                                            .otherwise("Unknown"))
    
    # Convert pickup and dropoff times to UTC
    df = df.withColumn("pickup_datetime_utc", to_utc_timestamp("tpep_pickup_datetime", "America/New_York")) \
        .withColumn("dropoff_datetime_utc", to_utc_timestamp("tpep_dropoff_datetime", "America/New_York"))

    # Generate a unique Trip_ID based on pickup and dropoff details
    df = df.withColumn("trip_id", sha2(concat("tpep_pickup_datetime", "pulocationid", "dolocationid"), 256))

    # Calculate trip duration in minutes
    df = df.withColumn("trip_duration_minutes", 
                    (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60)


    return df

def data_vault_hubs(df, file_name):
    # Hub_Location: Unique location keys representing physical locations
    hub_location = df.select("pulocationid").union(df.select("dolocationid")).distinct() \
        .withColumnRenamed("pulocationid", "location_hk") \
        .withColumn("record_source", lit(file_name)) \
        .withColumn("load_date", current_date())
        
    # Hub_Vendor: Unique vendor identifiers
    hub_vendor = df.select("vendorid").distinct() \
        .withColumnRenamed("vendorid", "vendor_hk") \
        .withColumn("record_source", lit(file_name)) \
        .withColumn("load_date", current_date())
    
    hub_trip = df.withColumn("trip_hk", sha2(concat("tpep_pickup_datetime", "pulocationid", "dolocationid"), 256)) \
        .select("trip_hk").distinct() \
        .withColumn("record_source", lit(file_name)) \
        .withColumn("load_date", current_date())
    # Hub_Payment: Generate unique payment_hk by hashing payment attributes for consistency
    hub_payment = df.withColumn("payment_hk", sha2(concat("payment_type", sha2(concat("tpep_pickup_datetime", "pulocationid", "dolocationid"), 256)), 256)) \
        .select("payment_hk", "payment_type").distinct() \
        .withColumn("record_source", lit(file_name)) \
        .withColumn("load_date", current_date())
    
    return hub_location, hub_vendor, hub_trip, hub_payment

def data_vault_links(df, file_name):
    # Link_Trip_Details: Connects trip_hk with vendor_hk, pickup_location_hk, dropoff_location_hk, and payment_hk
    link_trip_details = df.withColumn("trip_hk", sha2(concat("tpep_pickup_datetime", "pulocationid", "dolocationid"), 256)) \
        .withColumn("payment_hk", sha2(concat("payment_type", "trip_hk"), 256)) \
        .select("trip_hk", "vendorid", "pulocationid", "dolocationid", "payment_hk") \
        .withColumnRenamed("vendorid", "vendor_hk") \
        .withColumnRenamed("pulocationid", "pickup_location_hk") \
        .withColumnRenamed("dolocationid", "dropoff_location_hk") \
        .withColumn("record_source", lit(file_name)) \
        .withColumn("load_date", current_date())
    
    return link_trip_details

def data_vault_sattelites(df, file_name):
    # Satellite_Trip_Attributes: Stores trip-specific attributes
    sat_trip_attributes = df.withColumn("trip_hk", sha2(concat("tpep_pickup_datetime", "pulocationid", "dolocationid"), 256)) \
        .select("trip_hk", "passenger_count", "trip_distance", "ratecodeid", "store_and_fwd_flag", 
                to_utc_timestamp("tpep_pickup_datetime", "America/New_York").alias("pickup_datetime_utc"), 
                to_utc_timestamp("tpep_dropoff_datetime", "America/New_York").alias("dropoff_datetime_utc"),
                ((unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60).alias("trip_duration_minutes")) \
        .withColumn("record_source", lit(file_name)) \
        .withColumn("load_date", current_date())
    
    # Satellite_Payment_Details: Stores payment-specific attributes linked to each trip
    sat_payment_details = df.withColumn("trip_hk", sha2(concat("tpep_pickup_datetime", "pulocationid", "dolocationid"), 256)) \
        .select("trip_hk", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", 
                "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee") \
        .withColumn("record_source", lit(file_name)) \
        .withColumn("load_date", current_date())
    
    return sat_trip_attributes, sat_payment_details

    
def insert_data(df):
    hub_location, hub_vendor, hub_trip, hub_payment  = data_vault_hubs(df, 'yellow_tripdata_2024-02')
    hub_location.write.format("iceberg") \
    .mode("append") \
    .saveAsTable("my_catalog.data_vault.hub_location")

    hub_vendor.write.format("iceberg") \
    .mode("append") \
    .save("my_catalog.data_vault.hub_vendor")

    hub_trip.write.format("iceberg") \
    .mode("append") \
    .save("my_catalog.data_vault.hub_trip")

    hub_payment.write.format("iceberg") \
    .mode("append") \
    .save("my_catalog.data_vault.hub_payment")

    link_trip_details = data_vault_links(df, 'yellow_tripdata_2024-02')
    link_trip_details.write.format("iceberg") \
    .mode("append") \
    .save("my_catalog.data_vault.link_trip_details")

    sat_trip_attributes, sat_payment_details = data_vault_sattelites(df, 'yellow_tripdata_2024-02')
    sat_trip_attributes.write.format("iceberg") \
    .mode("append") \
    .save("my_catalog.data_vault.satellite_trip_attributes")

    sat_payment_details.write.format("iceberg") \
    .mode("append") \
    .save("my_catalog.data_vault.satellite_payment_details")
