import random
from pathlib import Path
from datetime import datetime, timedelta
from bs4 import BeautifulSoup as BSoup
import requests
import time
import pandas as pd
import pickle
import config
import src.analib as al


class flowTracker():
    def __init__(self,
                 output_path=config.flowPath,
                 ex_lst = ['binance', 'ftx', 'huobi'],
                 startDate = datetime(2022, 1, 1, 0, 0, 0)):
        # self.PROXIES = self.get_proxy(ind=False)
        self.cols = ['Hash', 'Method', 'block', 'time', 'age', 'from', 'direction', 'to', 'value', 'txnFee']
        self.ex_lst = ex_lst
        self.outputPath = output_path
        self.startDate = startDate
        self.headers = config.etherscanHeader
        self.valid_address = None


    def get_exchange_address(self):
        ex_dic = {}
        for ex in self.ex_lst:
            label_add = 'https://etherscan.io/accounts/label/%s' % ex
            req = requests.get(label_add, headers=self.headers)
            bs_obj = BSoup(req.content, 'html.parser')
            rows = bs_obj.find_all('table')[0].find('tbody').find_all('tr')
            adds = []

            for row in rows:
                cells = row.find_all('td')
                block = cells[0].get_text()
                adds = adds + [block]
            ex_dic[ex] = adds
        return ex_dic

    def check_active_exchange(self):
        ex_dic = self.get_exchange_address()
        ex_dic_valid = {}

        for key in ex_dic:
            add_lst = ex_dic[key]
            add_valid_lst = []
            for add in add_lst:
                print(key, add)
                exchange_add = 'https://etherscan.io/txs?a=%s' % add
                df_trans = self.get_curr_trans(exchange_add)
                time.sleep(2)
                if df_trans['time'].min() > self.startDate:  # if the address has been active from startDate.
                    add_valid_lst = add_valid_lst + [add]
            ex_dic_valid[key] = add_valid_lst
        return ex_dic_valid

    def save_valid_address(self):
        ex_dic_valid = self.check_active_exchange()
        self.valid_address = ex_dic_valid
        al.savedata(ex_dic_valid, config.valid_address)

    def get_freq_trans(self):
        """get all pages of txs from the beginning date"""
        if not self.valid_address:
            self.valid_address = al.loaddata(config.valid_address)
        if not Path(self.outputPath).exists():
            headCol = pd.DataFrame(
                    columns=['Hash', 'Method', 'block', 'time', 'age', 'from', 'direction', 'to', 'value', 'txnFee'])
            headCol.to_csv(self.outputPath,
                           encoding='utf-8',
                           mode='a',
                           header=True)
        else:
            pass

        for key in self.valid_address:
            for add in self.valid_address[key][:2]:
                print(key + ' ' + add)
                conti = True
                i = 0
                while conti:
                    exchange_add = 'https://etherscan.io/txs?a=%s' % add + '&p=%s' % i
                    df_trans = self.get_curr_trans(exchange_add)
                    try:
                        time.sleep(random.uniform(1, 2))
                        if (not df_trans.empty) and (df_trans['time'].min() > self.startDate):
                            df_trans.to_csv(self.outputPath,
                                            encoding='utf-8',
                                            mode='a',
                                            header=False)
                            i += 1
                        else:
                            conti = False
                            break
                    except:
                        return df_trans

    def get_curr_trans(self, exchange_add):
        """ge the last page of txs"""
        req = requests.get(exchange_add, headers=self.headers)
        bs_obj = BSoup(req.content, 'html.parser')
        try:
            rows = bs_obj.find_all('table')[0].find('tbody').find_all('tr')
        except IndexError:
            print(exchange_add)
            return pd.DataFrame()
        trans = []

        for row in rows:
            cells = row.find_all('td')
            try:
                txnHash = cells[1].get_text()
                method = cells[2].get_text()
                block = cells[3].get_text()
                time = cells[4].get_text()
                age = cells[5].get_text()
                fromadd = cells[6].get_text()
                direction = cells[7].get_text().strip()
                toadd = cells[8].get_text()
                val = cells[9].get_text()
                txnFee = cells[10].get_text()
            except IndexError:
                print(cells)
                continue

            trans.append([
                txnHash, method, block, time, age, fromadd, direction, toadd, val, txnFee
            ])

        df_trans = pd.DataFrame(trans)
        if df_trans.empty:
            return pd.DataFrame(columns=self.cols)
        df_trans.columns = self.cols
        df_trans['time'] = pd.to_datetime(df_trans['time'])
        df_trans['value'] = df_trans['value'].apply(lambda x: x.split(' Ether')[0])
        df_trans['value'] = df_trans['value'].apply(pd.to_numeric, errors='coerce')
        return df_trans