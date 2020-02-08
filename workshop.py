import os
import datetime as dt
from library.stock.stock import Stock
from library.stock.fetch import WorldTrade, ZachsApi
from library.database import Updater
from library.earnings import YahooEarningsCalendar
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import pymongo as db
from bson import ObjectId


def ticker_path(ticker):
    return f'E:\\Libraries\\Documents\\Stock Assistant\\database\\15 min interval\\{ticker}.json'


revb_path = 'E:\\Libraries\\Documents\\Stock Assistant\\EarningsTrader_RevB\\data\\'
revc_path = 'E:\\Libraries\\Documents\\Stock Assistant\\database\\15 min interval\\'
cluster = db.MongoClient('mongodb+srv://desktop:hipeople1S@main-ojil5.azure.mongodb.net'
                         '/test?retryWrites=true&w=majority')
database = cluster['main']
collection = database['15 min interval']
key = 'bYoNpNAQNbpLSKQaMkcwrI68rniyZQDXL7B7aqYNPsHMrr0CRLIe3UYCfkHF'

# update = Updater(key, 30)
# update.download_json('E:\\Libraries\\Documents\\Stock Assistant\\database\\15 min interval\\')

def test(arg):
    print('test')
    return list(range(arg))
def error():
    raise Exception
def sub(arg):
    with ThreadPoolExecutor() as executor:
        results = {
            'test1': executor.submit(test, arg),
            'test2': executor.submit(test, arg),
            'test3': executor.submit(test, error())
        }
    return results

with ThreadPoolExecutor() as executor:
    results = {
        'test1': executor.submit(test, error()),
        'test2': executor.submit(test, 3),
        'test3': executor.submit(test, 3)
    }
print(results['test1'])