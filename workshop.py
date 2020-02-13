import os
import datetime as dt
from library.stock.stock import Stock
from library.stock.fetch import Intraday, ZachsApi
from library.database import JsonManager, Health
from library.earnings import YahooEarningsCalendar
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import pymongo as db
from bson import ObjectId
import pandas as pd
import pandas_market_calendars as mcal
import time


def ticker_path(ticker):
    return f'E:\\Libraries\\Documents\\Stock Assistant\\database\\15 min interval\\{ticker}.json'


revb_path = 'E:\\Libraries\\Documents\\Stock Assistant\\EarningsTrader_RevB\\data\\'
revc_path = 'E:\\Libraries\\Documents\\Stock Assistant\\database\\15 min interval\\'
test_db = 'E:\\Libraries\\Documents\\Stock Assistant\\database\\test\\'
incomplete = 'E:\\Libraries\\Documents\\Stock Assistant\\database\\incomplete\\'
cluster = db.MongoClient('mongodb+srv://desktop:<password>@main-ojil5.azure.mongodb.net'
                         '/test?retryWrites=true&w=majority')
database = cluster['main']
collection = database['15 min interval']
key = 'bYoNpNAQNbpLSKQaMkcwrI68rniyZQDXL7B7aqYNPsHMrr0CRLIe3UYCfkHF'

if __name__ == '__main__':
    db = JsonManager(
        test_db,
        incomplete_handler='raise_error',
        move_to=incomplete,
        api_key=key,
        surpress_message=True,
        parallel_mode='multiprocess',
        tolerance=1
    )
    db.Health


    # nasdaq = mcal.get_calendar('NASDAQ')
    # dates = nasdaq.schedule(start_date=dt.date(2020, 12, 14), end_date=dt.date(2020, 12, 30))
    # # dates.index = [
    # #     i.date()
    # #     for i in dates.index
    # # ]
    # print(dates['market_open'].to_list())