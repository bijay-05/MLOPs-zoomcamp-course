import pandas as pd
from datetime import datetime
from main import main
S3_ENDPOINT_URL='http://localhost:4566'

def dt(hr,min,sec=0):
    return datetime(2021,1,1,hr,min,sec)

def prepare_data(df,categorical):
    df['duration'] = df.dropOff_datetime - df.pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df

def main(year,month):
    y_pred = main(year,month)
    preds = pd.series(y_pred)
    print(preds.sum())

if __name__=="__main__":
    main(year=2021,month=1)

    