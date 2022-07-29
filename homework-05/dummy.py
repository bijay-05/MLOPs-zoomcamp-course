import os
import pandas as pd

DEFAULT_S3_ENDPOINT_URL = 'http://localhost:4566'
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL",DEFAULT_S3_ENDPOINT_URL)
options={
        'client_kwargs':{
            'endpoint_url': S3_ENDPOINT_URL
        }
    }
filename = "s3://nyc-duration/out/2021-01.parquet"
df = pd.read_parquet(filename, storage_options=options)

df.to_parquet("predictions.parquet", engine='pyarrow',
 compression=None, index=False)