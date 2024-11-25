from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType
from pyspark.sql.functions import year, month,hour, day, col, lit

def prepare_fact_table(df):
    df = df.withColumn("total_price", col("price") * lit(1)) \
           .withColumn("year", year("event_time")) \
           .withColumn("month", month("event_time")) \
           .withColumn("day", day("event_time"))
    return df.select("user_id", "product_id", "event_type", "price", "category_id",
                     "total_price", "event_time", "year", "month", "day")


def prepare_user_dimension(df):
    return df.select("user_id", "user_session").distinct()

def prepare_product_dimension(df):
    # Creates the product dimension table with unique product attributes.
    return df.select("product_id", "category_id", "category_code", "brand").distinct()

def prepare_category_dimension(df):
    # Creates the category dimension table with unique category information.
    return df.select("category_id", "category_code").distinct()

def prepare_time_dimension(df):
    return  df.withColumn("year", year("event_time")) \
            .withColumn("hour", hour("event_time")) \
           .withColumn("month", month("event_time")) \
           .withColumn("day", day("event_time"))\
            .select("year", "month", "day", "hour").distinct()


def write_to_iceberg_table(df, table_name): # Global function to make it easier to write into any table
    df.write.format("iceberg").mode("overwrite").save(f"{table_name}")



def load_trans_fact(df):
    fact_df = prepare_fact_table(df)
    print('Loading data into the fact table... ')
    write_to_iceberg_table(fact_df, "my_catalog.star_schema.fact_transactions")


def load_product_dim(df):
    print('Loading data into the product dimension... ')
    product_df = prepare_product_dimension(df)
    write_to_iceberg_table(product_df, "my_catalog.star_schema.dim_products")


def load_user_dim(df):
    print('loading data into the user dimension... ')
    user_df = prepare_user_dimension(df)
    write_to_iceberg_table(user_df, "my_catalog.star_schema.dim_users")

def load_category_dim(df):
    print("Loading data into category dimension... ")
    category_df = prepare_category_dimension(df)
    write_to_iceberg_table(category_df, "my_catalog.star_schema.dim_category")


def load_datetime_dim(df):
    print('loading data into the datetime dimension... ')
    time_df = prepare_time_dimension(df)
    write_to_iceberg_table(time_df, "my_catalog.star_schema.dim_date_time")


if __name__ == '__main__':
    # The path written in the docker compose volumes as ./warehouse is /home/iceberg/warehousedef init_spark():
    spark = SparkSession.builder \
        .appName("Iceberg_warehouse") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", "home/iceberg/warehouse")  \
        .getOrCreate()
    

    df = spark.read.csv('/home/iceberg/resources/cleaned', header=True, inferSchema=True)

    try:
        load_trans_fact(df)
        print('Data loaded successfully into the fact table')
    except Exception as e:
        print('Erro loading data into fact table!!', e)
    
    try:
        load_user_dim(df)
        print('Data loaded successfully into the user dimension')
    except Exception as e:
        print('Erro loading data into vendor dimension!!', e)

    try:
        load_category_dim(df)
        print('Data loaded successfully into the category dimension')
    except Exception as e:
        print('Erro loading data into payment dimension!!', e)

    try:
        load_product_dim(df)
        print('Data loaded successfully into the product dimension')
    except Exception as e:
        print('Erro loading data into location dimension!!', e)

    try:
        load_datetime_dim(df)
        print('Data loaded successfully into the datetime dimension')
    except Exception as e:
        print('Erro loading data into datetime dimension!!', e)