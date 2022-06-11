import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime
import src.analib as al
from tqdm import tqdm


def dataLoader(exchange, path):
    flow = pd.read_csv(path, index_col=[0])
    flow = flow.applymap(lambda x: x.lower() if type(x) == str else x)
    flow['time'] = pd.to_datetime(flow['time'])
    flow['value'].fillna(0, inplace=True)
    flow.sort_values(by='time', inplace=True)
    flow.reset_index(drop=True, inplace=True)

    FlowIn = flow.loc[flow['to'].str.contains(exchange), :]
    FlowOut = flow.loc[flow['from'].str.contains(exchange), :]
    return FlowIn, FlowOut


# $$ inpluse

def gaussPulse(t, sigma, ak):
    return ak * np.exp(-t**2/(2*np.power(sigma, 2)))


def genPulseflow(flow, threshold, window, graininess = datetime.timedelta(seconds=1)):
    pastTime = None
    signal_df = pd.Series([[] for i in range(window)]).to_frame()
    signal_list = []
    time_list = []
    for r in tqdm(flow.index):
        row = flow.loc[r]
        nowTime = row['time']
        if pastTime:
            Delta = int((nowTime - pastTime) / graininess)
            if Delta >= 100:
                signal_df = pd.Series([[] for i in range(window)]).to_frame()
            elif Delta == 0:
                pass
            else:
                signal_df = signal_df.shift(Delta)
                signal_df.loc[0: Delta-1, 0] = [[] for i in range(Delta)]
        v = row['value']
        if v < threshold:
            pass
        else:
            signal_df.loc[0, 0].append(v)

        signal = signal_df.apply(lambda x:
                                 sum([gaussPulse(float(x.name) / window, 0.5, i / threshold) for i in x[0]])
                                 if x[0] else 0, axis=1).sum()
        signal_list.append(signal)
        time_list.append(nowTime)
        pastTime = nowTime

    return pd.Series(signal_list, index=time_list)


def signalAdjust(signal, underlying, threshold=3, fre=5):
    fig, axs = plt.subplots(1, 2, figsize=(20, 7))
    axs[0].plot(signal)
    signal = signal[signal > threshold]
    pastTime = None
    res = []
    axs[1].plot(underlying, color='r')
    for i in signal.index:
        if pastTime:
            if i - pastTime <= datetime.timedelta(minutes=fre):
                continue
            else:
                axs[1].axvline(i)
                pastTime = i
                res.append(i)
        else:
            axs[1].axvline(i)
            res.append(i)
            pastTime = i
    plt.xticks(rotation=90)
    plt.show()
    return res


if __name__ == '__main__':
    flowIn ,flowOut = dataLoader('binance 14')
    threshold = 200
    window = 300
    pulse = genPulseflow(flowOut,threshold, window)
    ethPrice = al.loaddata('./data/ETHprice20220609.pkl')
    underlying = ethPrice['close']

    signal = signalAdjust(pulse, underlying)
