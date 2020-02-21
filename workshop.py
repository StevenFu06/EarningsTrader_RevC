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


def ticker_path(ticker):
    return f'E:\\Libraries\\Documents\\Stock Assistant\\database\\15 min interval\\{ticker}.json'


main_db = 'E:\\Libraries\\Documents\\Stock Assistant\\database'
revb_path = 'E:\\Libraries\\Documents\\Stock Assistant\\database\\data'
revc_path = 'E:\\Libraries\\Documents\\Stock Assistant\\database\\5 min interval\\'
test_db = 'E:\\Libraries\\Documents\\Stock Assistant\\database\\test\\'
incomplete = 'E:\\Libraries\\Documents\\Stock Assistant\\database\\incomplete\\'
cluster = db.MongoClient('mongodb+srv://desktop:<password>@main-ojil5.azure.mongodb.net'
                         '/test?retryWrites=true&w=majority')
database = cluster['main']
collection = database['15 min interval']
key = 'bYoNpNAQNbpLSKQaMkcwrI68rniyZQDXL7B7aqYNPsHMrr0CRLIe3UYCfkHF'

if __name__ == '__main__':
    pp = PrettyPrinter(indent=4)
    with open(os.path.join(main_db, 'cached_stocks.pkl'), 'rb') as read:
        stocks = pickle.load(read)

    start = time.time()

    report = Report(stocks)
    report.add_critiera('nan_checker', 0.5)
    report.add_critiera('date_checker', 0.5)
    report.add_critiera('outdated', dt.date(2020, 1, 20))
    report.add_critiera('missing_attrs')
    report.add_critiera('date_coherance')
    report.full_report()

    end = time.time()

    pp.pprint(report.report)
    print(f'took {end - start} seconds')
    report.save_report('report.json')


















