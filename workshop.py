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
import pandas as pd


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

# BYSI = Stock('NVDA')
# BYSI.read_json(os.path.join(test_db, 'NVDA.json'))
# print(BYSI.close.loc[~BYSI.close.index.duplicated(keep='first')])

nvda = Stock('NVDA').read_legacy_csv(revb_path, auto_populate=False)
nvda.fetch(key)
nvda.fetch(key)
nvda.fetch(key)
print(nvda.market_cap)

# db = JsonManager(
#     test_db,
#     api_key=key,
#     incomplete_handler='move',
#     move_to=incomplete,
# )
#
# db.update()
# print(db.blacklist)
