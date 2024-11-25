from pyspark.sql.functions import dayofweek, col
from pyspark.sql import SparkSession

def read_data(spark, path, file_name):
    df = spark.read.csv(
        f'{path}/{file_name}',
        header=True,
        mode="DROPMALFORMED",
        encoding="UTF-8"
    )
    return df

def clean_data(df):

    # Clean the data by dropping rows with null values.
    df.fillna({"category_code": "Unknown", "brand": "Unknown"})

    # Standardize the column names by converting them to lowercase.
    df = df.toDF(*[c.lower() for c in df.columns])

    # Assuming event_time is a timestamp, and price is a float
    df = df.withColumn("event_time", col("event_time").cast("timestamp"))
    df = df.withColumn("category_id", col("category_id").cast("bigint"))
    df = df.withColumn("price", col("price").cast("float"))

    # Add a 'day_of_week' feature extracted from the event_time.
    df = df.withColumn("day_of_week", dayofweek("event_time"))

    # Remove duplicates
    df = df.dropDuplicates()
    df.write.csv('/home/iceberg/resources/cleaned', mode='overwrite', header=True)
    return df

if __name__ == '__main__':
    
    # The path written in the docker compose volumes as ./warehouse is /home/iceberg/warehousedef init_spark():
    spark = SparkSession.builder \
        .appName("Iceberg_warehouse") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", "home/iceberg/warehouse")  \
        .getOrCreate()
    try:
        df = read_data(spark, '/home/iceberg/resources','2019-Oct.csv')
        print('Data is extracted successfully')
        print('Cleaning the data... ')
    except Exception as e:
        print('Error extracting the data: ', e)

    try:
        df = clean_data(df)
        print('Data  is cleaned and ready ... ')
        df.show(5)
    except Exception as e:
        print('Error Cleaning the data !!', e)
    
