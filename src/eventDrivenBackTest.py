import numpy as np
import datetime
import pandas as pd
import os
from src import analib
import matplotlib.pyplot as plt

# modify, record the corresponding time point instead of value
def dropDuplicatedIndex(df):
    return df[~df.index.duplicated(keep='first')]

def timelineMerge(signal: list, underlying: pd.Series):
    """when the signal time and underlying time can not be corresponded one by one,
    find the closest next time in the underlying data"""
    signal_timeline = pd.Series(dtype='object',index=signal)
    underlying_timeline = pd.Series(underlying.index, index=underlying.index)
    timeline_plus = signal_timeline.append(underlying_timeline).sort_index()
    res = dropDuplicatedIndex(timeline_plus).fillna(method='bfill').reindex(signal_timeline.index)
    return res


def pointAna(signal, underlying, window, plot=True):
    timeUnit = datetime.timedelta(seconds=1)
    price_list = []
    for s in signal:
        price = underlying.loc[s: s + window * timeUnit] / underlying.loc[s]
        price_list.append(list(price))

    price_df = pd.DataFrame(price_list, index=signal).T
    miu = price_df.mean(axis=1)
    sigma = price_df.std(axis=1)

    down = miu - 1.96 * sigma / np.sqrt(len(signal))
    up = miu + 1.96 * sigma / np.sqrt(len(signal))

    ratio = price_df.apply(lambda x: len(x[x>1]), axis=1) / len(signal)
    average_loss = price_df.apply(lambda x: x[x<=1].mean(), axis=1)
    average_ret = price_df.apply(lambda x: x[x>1].mean(), axis=1)

    # 加胜率，平均损失等

    if plot:
        fig, axs= plt.subplots(1, 3, figsize=(40 ,12))
        axs[0].plot(miu)
        axs[0].plot(down, color='red', linestyle='--')
        axs[0].plot(up, color='red', linestyle='--')

        axs[1].bar(ratio.index, ratio)
        axs[2].plot(average_loss, color='red')
        axs[2].plot(average_ret, color='green')
        plt.show()

    return [miu, up, down, ratio]


# 每日可用现金按份额比例分配给需要买入的股票
def backTest(signal, underlying, window, buy_price, sell_price, stats_price, start_money=10000000, channel_num=2):
    """
    :param channel_num: 通道数
    :param positions: 每日调仓份额df，时间×股票
    :param start_money: 起始资金
    :return: 每日账户总体情况，最终结束时持仓
    """
    positions = pd.DataFrame(dtype='float', index=underlying.index, columns=['ETH/USD']).fillna(0)
    account_list = []  # 记录每日的现金，总资金，股票，收益率等，用来concat
    cash_account = start_money  # 初始资金
    stk_account = 0
    hold_record = []
    hold_stks = pd.DataFrame(columns=['position volume', 'now price', 'time'], index=positions.columns)  # 记录实际持仓状态
    hold_stks['position volume'] = 0
    for dt in positions.index:
        sellSignal = pd.DataFrame()
        buySignal = pd.DataFrame()
        if dt in signal.index:
            newSignal = signal.loc[dt]
            sellSignal = newSignal[newSignal < 0].to_frame(name='signal')
            sellSignal['sell price'] = sell_price.loc[dt, sellSignal.index]
            buySignal = newSignal[newSignal > 0].to_frame(name='signal')
            buySignal['buy price'] = buy_price.loc[dt, buySignal.index]

        td_stk = positions.loc[dt].replace(0, np.nan).dropna()  # 当日需要调仓的记录
        buy_data = td_stk[td_stk > 0].to_frame(name='buy volume')  # 当日买入的名单和份数

        sell_data = td_stk[td_stk < 0].to_frame(name='sell volume')  # 当日卖出的名单和份数

        # 卖出
        amt_sell = 0
        if len(sell_data) > 0:
            sell_data['sell price'] = sell_price.loc[dt, sell_data.index]
            sell_data['sell date'] = dt
            hold_stks.loc[sell_data.index, 'position volume'] += sell_data.loc[:, 'sell volume']
            amt_sell += (sell_data['sell price'] * sell_data['sell volume']).sum()  # 计算卖出总额

        if not sellSignal.empty:
            if dt < positions.index[channel_num]:
                sellSignal['sell volume'] = -1 * (min(start_money / channel_num, cash_account) / sellSignal['signal'].sum()) * \
                                           sellSignal['signal'] / sellSignal['sell price']  # 将可用总金额平均分配后买入量
            else:
                sellSignal['sell volume'] = -1 * (cash_account / channel_num / sellSignal['signal'].sum()) * sellSignal['signal'] / sellSignal[
                    'sell price']  # 将可用总金额平均分配后买入量
            hold_stks.loc[sellSignal.index, 'position volume'] += sellSignal.loc[:, 'sell volume']

            positions.loc[dt + window, sellSignal.index] -= sellSignal.loc[:, 'sell volume']

            amt_sell += (sellSignal['sell price'] * sellSignal['sell volume']).sum()  # 计算卖出总额

        cash_account -= amt_sell  # 更新现金账户

        # 买入
        amt_buy = 0
        if len(buy_data) > 0:
            buy_data['buy date'] = dt  # 将买入日期加入到该列
            buy_data['buy price'] = buy_price.loc[dt, buy_data.index]  # 买入价格
            hold_stks.loc[buy_data.index, 'position volume'] += buy_data.loc[:, 'buy volume']
            amt_buy += (buy_data['buy price'] * buy_data['buy volume']).sum()

        if not buySignal.empty:
            if dt < positions.index[channel_num]:
                buySignal['buy volume'] = 1 * (min(start_money / channel_num, cash_account) / buySignal['signal'].sum()) * \
                                           buySignal['signal'] / buySignal['buy price']  # 将可用总金额平均分配后买入量
            else:
                buySignal['buy volume'] = 1 * (cash_account / channel_num / buySignal['signal'].sum()) * buySignal['signal'] / buySignal[
                    'buy price']  # 将可用总金额平均分配后买入量
            hold_stks.loc[buySignal.index, 'position volume'] += buySignal.loc[:, 'buy volume']
            positions.loc[dt + window, buySignal.index] -= buySignal.loc[:, 'buy volume']

            amt_buy += (buySignal['buy price'] * buySignal['buy volume']).sum()  # 计算卖出总额

        cash_account -= amt_buy  # 可用现金的更新


        # 统计净值
        hold_stks['now price'] = stats_price.loc[dt, hold_stks.index]  # 更新当前价格
        hold_stks['time'] = dt
        stk_account = (hold_stks['position volume'] * hold_stks['now price']).sum()  # 股票账户当日收盘总额
        total_account = cash_account + stk_account  # 当日收盘账户总额
        # print('现金资产',cash_account,'股票资产',stk_account,'总资产',total_account)
        account_list.append(pd.Series([cash_account, stk_account, total_account, amt_buy, amt_sell],
                                      index=['cash', 'underlying', 'total', 'buy', 'sell']))
        hold_record.append(hold_stks.copy(deep=True))
    account_df = pd.concat(account_list, axis=1, keys=positions.index).T
    account_df['ret(%)'] = account_df['total'].pct_change() * 100
    account_df['turnover(%)'] = account_df['sell'] / account_df['underlying'].shift()
    ans = {'account_df': account_df,
           'hold_stks': pd.concat(hold_record)}
    return ans


def analyse(nv, save_path, benchmark):
    nv = nv / nv[0]
    ret = nv.pct_change().dropna()
    mean_ret = ret.mean() * 1440
    mean_std = ret.std() * np.sqrt(1440)
    sharp_ratio = mean_ret / mean_std
    dd_s = 1 - nv / nv.cummax()
    max_dd = dd_s.max()
    dd_date = dd_s.sort_values(ascending=False).index[0]

    bc = benchmark.reindex(nv.index)
    bc = bc / bc.iloc[0]
    bc_ret = bc.pct_change().dropna()
    excess_ret = ret - bc_ret
    ex_ret = excess_ret.mean() * 1440
    ex_std = excess_ret.std() * np.sqrt(1440)
    info_ratio = ex_ret / ex_std
    rel_nv = (1 + excess_ret).cumprod()
    rel_dd_s = 1 - rel_nv / rel_nv.cummax()
    rel_max_dd = rel_dd_s.max()
    rel_dd_date = rel_dd_s.sort_values(ascending=False).index[0]

    print('------absolute------')
    print('mean daily return:' + str(mean_ret))
    print('mean daily volatility:' + str(mean_std))
    print('Sharpe:' + str(sharp_ratio))
    print('max drawdown:' + str(max_dd))
    print('drawdown time:' + str(dd_date))
    print('------relative------')
    print('excess mean daily return:' + str(ex_ret))
    print('excess mean daily volatility:' + str(ex_std))
    print('excess Sharpe:' + str(info_ratio))
    print('excess max drawdown:' + str(rel_max_dd))
    print('excess drawdown time:' + str(rel_dd_date))

    nv_df = pd.concat([nv, bc, rel_nv], axis=1,
                      keys=['strategy', 'benchmark', 'relative'],
                      sort=False).fillna(1)
    nv_df['relative'].plot(figsize=(20, 10))
    plt.savefig(save_path)
    plt.show()

    info_s = pd.Series([ex_ret, ex_std, info_ratio, rel_max_dd, rel_dd_date],
                       index=['mean daily return', 'mean daily volatility','Sharpe', 'max drawdown', 'drawdown time'])
    return info_s, nv_df


if __name__ == '__main__':
    os.chdir(r'C:\Users\King\PycharmProjects\Blockchain_Flow')
    pulse = analib.loaddata('./data/signalSample.pkl')
    pulse = [i.tz_localize('UTC') for i in pulse]
    ethPrice = analib.loaddata('./data/ETHprice20220609.pkl')
    start = 10000000
    underlying = ethPrice['close']
    backtestPrice = underlying.to_frame(name='ETH/USD')
    timeCorresponding = timelineMerge(pulse, underlying)
    signal = pd.DataFrame(dtype=float, index=list(timeCorresponding), columns=['ETH/USD']).fillna(-1)

    underlying = ethPrice['close']

    backtestResult = backTest(signal, underlying, datetime.timedelta(seconds=600))
    nv = backtestResult['account_df']['total'] / start
    info_s, nv_df = analyse(nv, save_path='./plot/test.png', benchmark=underlying)

    ma5 = underlying.rolling(5).mean()
    ma30 = underlying.rolling(30).mean()
    signal_baseline = pd.concat([ma5, ma30], axis=1)
    signal_baseline.columns = ['ma5', 'ma30']
    signal_baseline['signal'] = signal_baseline.apply(lambda x: 1 if x['ma5'] > x['ma30'] else -1, axis=1)
    signal_baseline['signal'] = 2 * signal_baseline['signal'].shift(1) + signal_baseline['signal']
    signal_baseline = signal_baseline.query('signal == -1')
    signal_baseline = pd.DataFrame(dtype=float, index=signal_baseline.index, columns=['ETH/USD']).fillna(1)

    backtestResult = backTest(signal_baseline, underlying, datetime.timedelta(seconds=600))
    nv = backtestResult['account_df']['total'] / start
    # info_s, nv_df = analyse(nv, save_path='./plot/test.png', benchmark=underlying)



