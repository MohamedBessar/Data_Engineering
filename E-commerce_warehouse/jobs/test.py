from pyspark.sql import SparkSession


if __name__ == '__main__':
    # The path written in the docker compose volumes as ./warehouse is /home/iceberg/warehousedef init_spark():
    spark = SparkSession.builder \
        .appName("Iceberg_warehouse") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", "home/iceberg/warehouse")  \
        .getOrCreate()
    
    sas_token = "sp=racweo&st=2024-11-24T14:07:47Z&se=2024-11-26T22:07:47Z&sip=0.0.0.0-255.255.255.255&spr=https&sv=2022-11-02&sr=b&sig=pj6bRjnCdksD3nnhZPsLQGPoBW99vFfEzhvdAWBqJCs%3D"
    storage_account_name = "cattoospark"
    container_name = "e-commerce"
    file_name = "data.csv"

    file_path = f"wasb://{storage_account_name}.blob.core.windows.net/{container_name}/{file_name}?{sas_token}"

    spark.conf.set("spark.hadoop.fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(file_path)

    df.show()
