import os
import datetime as dt
from library.stock.stock import Stock
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
stocks = os.listdir(revc_path)
