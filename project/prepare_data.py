import pandas as pd
import numpy as np

def add_dateparts(df, col):
    '''
    Takes df and column name as input
    and outputs modified df with
    various date parts
    '''
    df[col +'_year'] = df[col].dt.year
    df[col +'_month'] = df[col].dt.month
    df[col +'_day'] = df[col].dt.day
    df[col +'_weekday'] = df[col].dt.weekday
    df[col +'_hour'] = df[col].dt.hour
    return df    

def read_df(filename):
    '''
    Takes filename as input, 
    performs few transformations as per analysis
    carried out in notebook
    and outputs the modified dataframe
    '''
    df = pd.read_parquet(filename, engine="pyarrow")

    df = add_dateparts(df, "pickup_datetime")
    df = df.dropna() # just remove the rows with null values

    # the below values to filter out records is randomly chosen in accordance to analysis 
    # performed in the notebook with train set
    df = df.loc[(df["pickup_longitude"]>-80)&(df["pickup_longitude"]<0),]
    df = df.loc[(df["dropoff_longitude"]>-80)&(df["dropoff_longitude"]<0),]
    df = df.loc[(df["pickup_latitude"]>0)&(df["pickup_latitude"]<50),]
    df = df.loc[(df["dropoff_latitude"]>0)&(df["dropoff_latitude"]<50),]

    # now round off the values
    df = df.round(4)
    
    # checking passenger_count
    df = df.loc[df["passenger_count"]<7,]

    # checking target variable `fare_amount`
    df = df.loc[df["fare_amount"]>0,]

    return df

def haversine_np(lon1, lat1, lon2, lat2):
  '''calculate the great circle distance between two points on the earth(specified in decimal degrees)
     All args must be of equal length.
  '''
  lon1, lat1, lon2, lat2  = map(np.radians, [lon1, lat1, lon2, lat2])

  dlon = lon2 - lon1
  dlat = lat2 - lat1

  a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2

  c = 2 * np.arcsin(np.sqrt(a))
  km = 6367 * c
  return km


def add_features(df):
    '''
    This function calculates the distance between two locations
    whose longitude and latitude values are given
    '''

    df['trip_distance'] = haversine_np(df['pickup_longitude'],
                                      df['pickup_latitude'],
                                      df['dropoff_longitude'],
                                      df['dropoff_latitude'])
    return df

def main(file_name):
    '''
    the returns the modified dataframe
    and we will call this function in
    training pipeline
    '''
    df = read_df(filename=file_name)

    df = add_features(df=df)

    return df



