from hdfs import InsecureClient
import pandas as pd
import os
import time

# HDFS client setup
client = InsecureClient('http://localhost:9870/', user='root')

local_file = 'output_log_summary.csv'
hdfs_path = 'HDFS_Proj/output_log_summary.csv' 

while True:
    try:
        df = pd.read_csv(local_file)

        with client.write(hdfs_path, encoding='utf-8', overwrite=True) as writer:
            df.to_csv(writer, index=False)

        print(f"Updated {hdfs_path} on HDFS")
    except Exception as e:
        print(f"Error: {e}")

    time.sleep(300) 
