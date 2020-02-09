import os
import datetime as dt
from library.stock.stock import Stock
from library.stock.fetch import Intraday, ZachsApi
from library.database import JsonManager
from library.earnings import YahooEarningsCalendar
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import pymongo as db
from bson import ObjectId


def ticker_path(ticker):
    return f'E:\\Libraries\\Documents\\Stock Assistant\\database\\15 min interval\\{ticker}.json'


revb_path = 'E:\\Libraries\\Documents\\Stock Assistant\\EarningsTrader_RevB\\data\\'
revc_path = 'E:\\Libraries\\Documents\\Stock Assistant\\database\\15 min interval\\'
cluster = db.MongoClient('mongodb+srv://desktop:<password>@main-ojil5.azure.mongodb.net'
                         '/test?retryWrites=true&w=majority')
database = cluster['main']
collection = database['15 min interval']
key = 'bYoNpNAQNbpLSKQaMkcwrI68rniyZQDXL7B7aqYNPsHMrr0CRLIe3UYCfkHF'

wt = Intraday().dl_intraday('NVDA', key, 15, 30).to_dataframe()
print(wt.df_dict)