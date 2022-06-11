import src.flowAna as fa
import src.eventDrivenBackTest as edbt
import src.analib as al
import config
import os
import pandas as pd
import datetime


def main():
    os.chdir(config.localPath)
    ethPrice = al.loaddata(config.pricePath)
    underlying = ethPrice['close']
    flowIn ,flowOut = fa.dataLoader('binance 14', config.flowPath)
    threshold = 200
    window = 300
    start = 10000000
    backtestPrice = underlying.to_frame(name='ETH/USD')

    initPulse = fa.genPulseflow(flowOut,threshold, window)

    ma5 = underlying.rolling(5).mean()
    ma30 = underlying.rolling(30).mean()
    signal_baseline = pd.concat([ma5, ma30], axis=1)
    signal_baseline.columns = ['ma5', 'ma30']
    signal_baseline['signal'] = signal_baseline.apply(lambda x: 1 if x['ma5'] > x['ma30'] else -1, axis=1)
    signal_baseline['signal'] = 2 * signal_baseline['signal'].shift(1) + signal_baseline['signal']
    signal_baseline = signal_baseline.query('signal == -1')
    signal_baseline = pd.DataFrame(dtype=float, index=signal_baseline.index, columns=['ETH/USD']).fillna(1)

    baselineResult = edbt.backTest(signal_baseline, underlying,
                                   datetime.timedelta(seconds=300),
                                   backtestPrice, backtestPrice, backtestPrice)
    baseline_nv = baselineResult['account_df']['total'] / start

    info_list = []
    for t in [2, 2.25, 2.5, 2.75, 3]:
        pulse = fa.signalAdjust(initPulse, underlying,threshold=t)
        pulse = [i.tz_localize('UTC') for i in pulse]
        timeCorresponding = edbt.timelineMerge(pulse, underlying)
        signal = pd.DataFrame(dtype=float, index=list(timeCorresponding), columns=['ETH/USD']).fillna(-1)
    
        backtestResult = edbt.backTest(signal, underlying,
                                       datetime.timedelta(seconds=300),
                                       backtestPrice, backtestPrice, backtestPrice)
        nv = backtestResult['account_df']['total'] / start
        info_s, nv_df = edbt.analyse(nv, save_path='./plot/test.png', benchmark=baseline_nv)
        info_list.append(info_s.copy(deep=True))

    info = pd.concat(info_list, axis=1)
    info.columns = [2, 2.25, 2.5, 2.75, 3]
    info.to_csv('./data/performance.csv')


if __name__ == '__main__':
    main()

