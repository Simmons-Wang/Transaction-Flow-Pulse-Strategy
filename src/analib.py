import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import datetime
import time
import ftx
import pickle


def get_historical_data(market_name, start, end, resolution):
    client = ftx.FtxClient()
    result = []
    t = start
    while t <= end:
        print(t)
        tt = datetime.datetime.timestamp(t)
        res = client.get_historical_data(market_name=market_name,
                                         resolution=int(resolution),
                                         limit=int(1440 * 60 / resolution),
                                         end_time=tt)
        t += datetime.timedelta(days=1)
        result.append(pd.DataFrame(res))
        time.sleep(2)

    return pd.concat(result, axis=0)


def loaddata(fname):
    with open(fname, 'rb') as handle:
        b = pickle.load(handle)
    return b


def savedata(data, fname):
    with open(fname, 'wb') as handle:
        pickle.dump(data, handle, protocol=pickle.HIGHEST_PROTOCOL)

