import pandas as pd
import sqlalchemy
import argparse
import os
import pyarrow.parquet as pq



def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet'
    db = params.db
    table_name = params.table_name
    parquet_name = 'nyc.parquet'
    csv_name = 'nyc.csv'
    
    # Download Parquet
    os.system(f"wget {url} -O {parquet_name}")

    # Convert it to csv
    # Read the Parquet file into a Pandas DataFrame
    df = pq.read_table(parquet_name).to_pandas()

    # Export the DataFrame to a CSV file
    df.to_csv(csv_name, index=False)
    engine = sqlalchemy.create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}', echo=False)

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    while True: 
        df = next(df_iter)

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name='yellow_taxi_date', con=engine, if_exists='append')
        print('Inserted 100000 rows Sucessfully!')


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest data from csv to postgres')

    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table we will write the results in')
    args = parser.parse_args()
    
    main(args)