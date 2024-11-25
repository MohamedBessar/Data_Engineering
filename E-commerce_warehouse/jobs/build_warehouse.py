from pyspark.sql import SparkSession

def build_trans_fact(spark):
    # Define fact_transactions schema
    fact_trans_schema = """
        CREATE OR REPLACE TABLE my_catalog.star_schema.fact_transactions (
            user_id INT,
            product_id INT,
            category_id BIGINT,
            event_type STRING,
            price FLOAT,
            total_price FLOAT,
            event_time TIMESTAMP,
            year INT,
            month INT,
            day INT
        )
        USING iceberg
        PARTITIONED BY (year, month)
    """
    spark.sql(fact_trans_schema)


def build_user_dim(spark):
    # Define dim_vendor schema
    dim_user_schema = """
        CREATE OR REPLACE TABLE my_catalog.star_schema.dim_users (
            user_id INT,
            user_session STRING
        )
        USING iceberg
    """
    # Execute the query to create the empty dim_user table
    spark.sql(dim_user_schema)

def build_category_dim(spark):
    # Define dim_category schema
    dim_category_schema = """
        CREATE OR REPLACE TABLE my_catalog.star_schema.dim_category (
            category_id BIGINT,
            category_code STRING
        )
        USING iceberg
    """
    spark.sql(dim_category_schema)


def build_product_dim(spark):
    # Define dim_product schema
    dim_product_schema = """
        CREATE OR REPLACE TABLE my_catalog.star_schema.dim_products (
            product_id INT,
            category_id BIGINT,
            category_code STRING,
            brand STRING
        )
        USING iceberg
    """
    spark.sql(dim_product_schema)


def build_datetime_dim(spark):
    # Define dim_datetime schema
    dim_datetime_schema = """
        CREATE OR REPLACE TABLE my_catalog.star_schema.dim_date_time (
            year INT,
            month INT,
            day INT,
            hour INT
        )
        USING iceberg
    """
    spark.sql(dim_datetime_schema)

if __name__ == '__main__':
    # The path written in the docker compose volumes as ./warehouse is /home/iceberg/warehousedef init_spark():
    spark = SparkSession.builder \
        .appName("Iceberg_warehouse") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", "home/iceberg/warehouse")  \
        .getOrCreate()

    try:
        build_trans_fact(spark)
        print("Trips Fact table has been created successfully.")
    except Exception as e:
        print("Error in building Fact table: ", e)
    
    try:
        build_user_dim(spark)
        print("User dimesion table has been created successfully.")
    except Exception as e:
        print("Error in building User dimension table: ", e)

    try:
        build_category_dim(spark)
        print("Category dimesion table has been created successfully.")
    except Exception as e:
        print("Error in building category Dimension table: ", e)

    try:
        build_product_dim(spark)
        print("Product dimension table has been created successfully.")
    except Exception as e:
        print("Error in building Category dimension table: ", e)


    try:
        build_datetime_dim(spark)
        print("Datetime dimension table has been created successfully.")
    except Exception as e:
        print("Error in building Datetime dimension table: ", e)
