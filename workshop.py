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

if __name__ == '__main__':
    # db = JsonManager(
    #     test_db,
    #     incomplete_handler='move',
    #     move_to='incomplete',
    #     api_key=key,
    # )
    # db.update()
    # print(db.blacklist)

    raw = Intraday().dl_intraday('AUBN', key, 15, 30)
    df = pd.DataFrame.from_dict(raw.raw_intra_data['intraday'], orient='index')
    df.index = pd.to_datetime(df.index)

    times = list(set(df.index.time))
    times.sort()

    grouped = df.groupby(df.index.date)
    # data = [
    #     value['close'].to_list()
    #     for key, value in grouped
    # ]
    # print(data)

    for key, value in grouped:
        print(value['close'])

    # dates = list(set(df.index.date))
    # dates.sort()
    # times = list(set(df.index.time))
    # times.sort()
    # print(dates)
    # print(times)
