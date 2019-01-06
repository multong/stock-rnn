#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan  5 15:50:37 2019

@author: nosehyeon
"""
import click
import os
import pandas as pd
import pandas_datareader.data as web
from pandas_datareader._utils import RemoteDataError
from pandas_datareader.data import Options
#from pandas_datareader._utils import ImmediateDeprecationError
import matplotlib.pyplot as plt
import random
import time
import numpy as np

from datetime import datetime

#TESLA Data from Morningstar
#tesla = web.DataReader('TSLA', 'Quandl', '1980-01-01', '2018-03-01')
#print(tesla)

DATA_DIR = "data"
SP500_LIST_PATH = os.path.join(DATA_DIR, "constituents-financials.csv")
RANDOM_SLEEP_TIMES = (1, 5)


def _load_symbols():
#    _download_sp500_list()
    df_sp500 = pd.read_csv(SP500_LIST_PATH)
    df_sp500.sort_values('Market Cap', ascending=False, inplace=True)
    stock_symbols = df_sp500['Symbol'].unique().tolist()
    print ("Loaded %d stock symbols" % len(stock_symbols))
    return stock_symbols


def fetch_prices(symbol, out_name):
    
    start = datetime(1980, 1, 1)
    end = datetime.now()
    
    try:
#        option = Options(symbol, 'yahoo')
        data = web.DataReader(symbol, 'yahoo', start, end)
        
    except AttributeError:
        print ("AttributeError: Failed when fetching {}".format(symbol))
        return False
    
    except RemoteDataError:
        print ("RemoteDataError: Failed when fetching {}".format(symbol))
        return False
    except :
        print ("ImmediateDeprecationError: Failed when fetching {}".format(symbol))
        return False

    
    
    if data.empty:
        print ("Remove {} because the data set is empty.".format(out_name))
    else:
        print ("Fetching {} ...".format(symbol))
#        dates = data.iloc[:,0].tolist()
        dates = data.index.tolist()
        print ("# Fetched rows: %d [%s to %s]" % (data.shape[0], dates[0], dates[-1]))
        data.to_csv(out_name)
    

    sleep_time = random.randint(*RANDOM_SLEEP_TIMES)
    print ("Sleeping ... %ds" % sleep_time)
    time.sleep(sleep_time)
    return True
    

@click.command(help="Fetch stock prices data")
@click.option('--continued', is_flag=True)
def main(continued):
    
    random.seed(time.time())
    num_failure = 0
    

    symbols = _load_symbols()
    
    
    
    for idx, sym in enumerate(symbols):
        out_name = os.path.join(DATA_DIR, sym + ".csv")
        
        
#        print (out_name)
#        if continued and os.path.exists(out_name):
        if os.path.exists(out_name):
            print ("Fetched", sym)
            continue

        succeeded = fetch_prices(sym, out_name)
        num_failure += int(not succeeded)

        if idx % 10 == 0:
            print ("# Failures so far [%d/%d]: %d" % (idx + 1, len(symbols), num_failure))
                   
                   
if __name__ == "__main__":
    main()
        
        







