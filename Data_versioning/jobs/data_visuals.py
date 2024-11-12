from main import init_spark

spark = init_spark()
# View the snapshot history for the link_trip_details table
spark.sql("SELECT * FROM my_catalog.data_vault.link_trip_details.snapshots").show()
