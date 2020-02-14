import os
import datetime as dt
from library.stock.stock import Stock
from library.stock.fetch import Intraday, ZachsApi
from library.database import JsonManager, Health
from library.earnings import YahooEarningsCalendar
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
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


def test(stock):
    health = Health([stock])
    health.intraday_checker(stock, 0)
    return health.report


if __name__ == '__main__':

    db = JsonManager(
        revc_path,
        incomplete_handler='raise_error',
        move_to=incomplete,
        api_key=key,
        surpress_message=True,
        parallel_mode='multiprocess',
        tolerance=1
    )
    stocks = []
    for ticker in db.all_tickers:
        stocks.append(Stock(ticker).read_legacy_csv(revb_path))
        # stocks.append(Stock(ticker).read_json(ticker_path(ticker)))

    with ProcessPoolExecutor() as executor:
        results = [
            executor.submit(test, stock)
            for stock in stocks
        ]
    for future in results:
        print(future.result())
