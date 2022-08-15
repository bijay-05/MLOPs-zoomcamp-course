from prepare_data import main
import pandas as pd
import os
from getpass import getpass
import sys
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from xgboost import XGBRegressor
import mlflow
import pickle

from prefect import flow, task

@task
def get_inputs_outputs(df, input_cols, target):
    '''
    inputs: dataframe, input columns which are independent variables,
    target is dependent variable or in our case `fare_amount` which
    we need to predict 
    '''
    train_set, val_set = train_test_split(df, test_size=0.15, random_state=42)
    train_inputs = train_set[input_cols]
    train_targets = train_set[target]
    val_inputs = val_set[input_cols]
    val_targets = val_set[target]
    return train_inputs, val_inputs, train_targets, val_targets

@task
def train_model_log_experiment(inputs, val_inputs, targets, val_targets):
    '''
    inputs: train and valid sets
    outputs: logs experiment in DagsHub MLFLOW, dumps the model and 
    return RMSE value
    '''
    mlflow.set_experiment("nyc-taxi-fare-prediction")
    models = {
        "LRMODEL": LinearRegression(),
        "XGBOOST": XGBRegressor()
    }
    for name, model in models.items():
        with mlflow.start_run():
            model.fit(inputs, targets)
            val_pred = model.predict(val_inputs)
            rmse = mean_squared_error(val_targets, val_pred, squared=False)
            mlflow.log_metric("rmse", rmse)
            mlflow.log_params("Model_Name", name)
        with open(f"models/{name}.bin", "wb") as f_out:
            pickle.dump(model, f_out)
        print(f"RMSE for {name} model: ", rmse)

@flow
def mainn(filename):
    '''
    this is the main function which performs 
    all model training and experiment logging
    '''
    os.environ['MLFLOW_TRACKING_USERNAME'] = input('Enter your DAGsHub username: ')
    os.environ['MLFLOW_TRACKING_PASSWORD'] = getpass('Enter your DAGsHub access token: ')
    os.environ['MLFLOW_TRACKING_PROJECTNAME'] = input('Enter your DAGsHub project name: ')
    
    mlflow.set_tracking_uri(f'https://dagshub.com/' + os.environ['MLFLOW_TRACKING_USERNAME'] + '/' + os.environ['MLFLOW_TRACKING_PROJECTNAME'] + '.mlflow')

    df = main(file_name=filename)
    input_cols = ["passenger_count","pickup_datetime_month","pickup_datetime_year","pickup_datetime_hour", "trip_distance"]
    target = "fare_amount"
    inputs, val_inputs, targets, val_targets = get_inputs_outputs(df,input_cols,target)

    train_model_log_experiment(inputs,val_inputs,targets,val_targets)


if __name__=="__main__":

    year = int(sys.argv[1])
    
    file_name = f"data_{year}.parquet"
    mainn(filename=file_name)