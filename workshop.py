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

if __name__ == '__main__':
    import json
    from library.earnings import Earnings, ZachsEarningsCalendar

    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    earnings = Earnings('earnings.json')
    print(len(earnings.all_stocks()))








