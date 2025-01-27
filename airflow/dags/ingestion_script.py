#!/usr/bin/env python
# coding: utf-8

"""
First rudimentary pipeline of the course. The idea of this pipeline is to run it in a docker container,
and ingest a parquet table to a postgres instance run in another container.

Note that this is intended for pedagogic reasons, and to test locally. There are other much more powerful
tools that allow doing exactly this.
"""

# Import dependencies
from time import time
import argparse
import os

from sqlalchemy import create_engine

def ingest(user, password, host, db, port, table_name, file_name):

    # Create a postgres engine and connect
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()
    print('Connected to the Postgres db')
    print(file_name)

    if file_name.endswith('.parquet'):
        # Ingest a .parquet file to postgres

        # Import pyarrow
        import pyarrow.parquet as pq

        # Set chunk size
        chunk_size = 100000

        # Upload parquet file
        parquet_file = pq.ParquetFile(file_name)

        for row_group_index in range(parquet_file.num_row_groups):

            # This reads a single row group into an Arrow Table
            table = parquet_file.read_row_group(row_group_index)
            
            # Now produce RecordBatches of up to 'chunk_size' rows each
            batches = table.to_batches(max_chunksize=chunk_size)

            for i, batch in enumerate(batches):
                # Iterate over the table and upload it in chunks
                t_start = time()
                
                # Convert each batch to a Pandas DataFrame
                df_chunk = batch.to_pandas()
                
                if (row_group_index == 0) and (i == 0):
                    # Drop the table if it already exists
                    df_chunk.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

                # Upload the chunk to the Postgres database
                df_chunk.to_sql(name=table_name, con=engine, if_exists='append')

                t_end = time()
                print(f'Upload row group {row_group_index}, batch {i}: {df_chunk.shape}, took {t_end - t_start:.3f} seconds')

        print('Finished ingesting data into the postgres database')

    elif file_name.endswith('.csv') or file_name.endswith('.csv.gz'):
        
        # Ingest a .csv file to postgres

        print('Ingesting to Postgres')

        # Import pandas
        import pandas as pd

        df_iter = pd.read_csv(file_name, iterator=True, chunksize=100000)

        df = next(df_iter)

        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

        df.to_sql(name=table_name, con=engine, if_exists='append')


        while True: 

            try:
                t_start = time()
                
                df = next(df_iter)

                df.to_sql(name=table_name, con=engine, if_exists='append')

                t_end = time()

                print('Inserted another chunk, took %.3f seconds' % (t_end - t_start))

            except StopIteration:
                print('Finished ingesting data into the postgres database')
                break