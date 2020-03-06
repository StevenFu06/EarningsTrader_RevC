import pandas_market_calendars as mcal
import pymongo as db
import pandas as pd
import datetime as dt
from library.stock.stock import Stock
import os
import time
import pickle
from library.database import Report
from pprint import PrettyPrinter
from library.datetime_additions import MarketDatetime
import numpy as np
import matplotlib.pyplot as plt
from library.rankbank.prepare import Prepare
import timeit


def ticker_path(path, ticker):
    return os.path.join(path, ticker + '.json')


json_db = 'E:\\Libraries\\Documents\\Stock Assistant\\database\\json_db - 15\\stocks'
csv_db = 'E:\\Libraries\\Documents\\Stock Assistant\\database\\csv_db - 15'
data = 'E:\\Libraries\\Documents\\Stock Assistant\\EarningsTrader_RevB\\data'
cluster = db.MongoClient('mongodb+srv://desktop:<password>@main-ojil5.azure.mongodb.net'
                         '/test?retryWrites=true&w=majority')
database = cluster['main']
collection = database['15 min interval']
key = 'bYoNpNAQNbpLSKQaMkcwrI68rniyZQDXL7B7aqYNPsHMrr0CRLIe3UYCfkHF'

setup = """
import datetime as dt
import pandas_market_calendars as mcal
date = dt.date(2020, 2, 28)
"""

time_code = """
cal = mcal.get_calendar('NASDAQ')
cal.valid_days(date, date)
"""

if __name__ == '__main__':
    nvda = Stock('NVDA').read_json(ticker_path(json_db, 'NVDA'))
    amd = Stock('AMD').read_json(ticker_path(json_db, 'AMD'))

    test = Prepare(dt.date(2020, 2, 20), 5, 5, end_time=dt.time(14, 30))

    start = time.time()
    print(test.prepare_intraday(nvda, 'normalize_by_first_value'))
    end = time.time()

    print(end-start)
