import pandas as pd
from batch import prepare_data
from datetime import datetime

def dt(hr,min,sec=0):
    return datetime(2021,1,1,hr,min,sec)

data = [
        (None,None,dt(1,2),dt(1,10)),
        (1,1,dt(1,2),dt(1,10)),
        (1,1,dt(1,2,0),dt(1,2,50)),
        (1,1,dt(1,2,0),dt(2,2,1)),
    ]
columns = ["PULocationID","DOLocationID","pickup_datetime","dropOff_datetime"]
df = pd.DataFrame(data, columns=columns)

df_prep = prepare_data(df=df,categorical=["PULocationID","DOLocationID"])
S3_ENDPOINT_URL = "http://localhost:4566"
options = {
    'client_kwargs':{
        'endpoint_url':S3_ENDPOINT_URL
    }
}
input_file="s3://nyc-duration/in/2021-01.parquet"
df_prep.to_parquet(input_file,
    engine='pyarrow',
    compression=None,
    index=False,
    storage_options=options
)