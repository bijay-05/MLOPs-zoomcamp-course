from multiprocessing.spawn import prepare
from batch import prepare_data
from datetime import datetime
import pandas as pd


def dt(hr,min,sec=0):
    return datetime(2021,1,1,hr,min,sec)

def test_output():
    data = [
        (None,None,dt(1,2),dt(1,10)),
        (1,1,dt(1,2),dt(1,10)),
        (1,1,dt(1,2,0),dt(1,2,50)),
        (1,1,dt(1,2,0),dt(2,2,1)),
    ]
    columns = ["PULocationID","DOLocationID","pickup_datetime","dropOff_datetime"]
    df = pd.DataFrame(data, columns=columns)

    df_prep = prepare_data(df=df,categorical=["PULocationID","DOLocationID"])

    num_rows = len(df_prep)

    assert num_rows == 2