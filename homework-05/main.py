#!/usr/bin/env python
# coding: utf-8
import os
import sys
import pickle
import pandas as pd

# we are using default s3 endpoint hardcoded, instead it should not be made default
# otherwise there is no point is using conditionals

def read_data(filename, categorical):
    DEFAULT_S3_ENDPOINT_URL = 'http://localhost:4566'
    S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL",DEFAULT_S3_ENDPOINT_URL)
    options={
        'client_kwargs':{
            'endpoint_url': S3_ENDPOINT_URL
        }
    }
    if S3_ENDPOINT_URL:
        df = pd.read_parquet(filename, storage_options=options)
    else:
        df = pd.read_parquet(filename)
    
    df['duration'] = df.dropOff_datetime - df.pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df

def get_input_path(year, month):
    #default_input_pattern = 'https://raw.githubusercontent.com/alexeygrigorev/datasets/master/nyc-tlc/fhv/fhv_tripdata_{year:04d}-{month:02d}.parquet'
    default_input_pattern = 's3://nyc-duration/in/{year:04d}-{month:02d}.parquet'
    input_pattern = os.getenv('INPUT_FILE_PATTERN', default_input_pattern)
    return input_pattern.format(year=year, month=month)


def get_output_path(year, month):
    #default_output_pattern = 's3://nyc-duration-prediction-alexey/taxi_type=fhv/year={year:04d}/month={month:02d}/predictions.parquet'
    default_output_pattern = 's3://nyc-duration/out/{year:04d}-{month:02d}.parquet'
    output_pattern = os.getenv('OUTPUT_FILE_PATTERN', default_output_pattern)
    return output_pattern.format(year=year, month=month)


def prepare_data(df, categorical):
    df['duration'] = df.dropOff_datetime - df.pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df

def main(year:int, month:int):
    input_file = get_input_path(year,month)
    output_file = get_output_path(year,month)
    #input_file = f'https://raw.githubusercontent.com/alexeygrigorev/datasets/master/nyc-tlc/fhv/fhv_tripdata_{year:04d}-{month:02d}.parquet'
    #output_file = f's3://nyc-duration-prediction-alexey/taxi_type=fhv/year={year:04d}/month={month:02d}/predictions.parquet'
    #output_file = f"taxi_fhv_year={year:04d}_month={month:02d}.parquet"

    categorical = ['PULocationID', 'DOLocationID']

    df = read_data(input_file, categorical=categorical)
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')


    with open('model.bin', 'rb') as f_in:
        dv, lr = pickle.load(f_in)

    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)


    print('predicted mean duration:', y_pred.mean())
    print(y_pred.sum())

    df_result = pd.DataFrame()
    df_result['ride_id'] = df['ride_id']
    df_result['predicted_duration'] = y_pred
    S3_ENDPOINT_URL = 'http://localhost:4566'
    options = {
        'client_kwargs':{
            'endpoint_url': S3_ENDPOINT_URL
        }
    }
    #df_result.to_parquet(output_file, engine='pyarrow', index=False, storage_options=options)
    return y_pred


if __name__== "__main__":
    year = int(sys.argv[1])
    month = int(sys.argv[2])
    main(year=year,month=month)
    # 43.15