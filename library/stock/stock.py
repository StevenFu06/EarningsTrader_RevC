from library.stock import fetch
import pandas as pd
import pymongo as db
import os

"""Main stock api 
Interacts with erryting basically, a life saver, one stop shop for all stock data
"""


class Stock:

    def __init__(self, ticker: str, load_from):
        self.load_from = load_from
        self.ticker = ticker
        self._load_method()

    def _load_method(self):
        load_type = type(self.load_from)
        if load_type == db.database.Database:
            print('success')
        elif load_type == dict:
            pass
        elif os.path.exists(self.load_from) and load_type == str:
            if self.load_from[-5:] == '.json':
                pass
            elif os.path.exists(f'{self.load_from}/{self.ticker}/close.csv'):
                pass


if __name__ == '__main__':
    cluster = db.MongoClient('mongodb+srv://desktop:hipeople1S@main-ojil5.azure.mongodb'
                             '.net/test?retryWrites=true&w=majority')
    database = cluster['main']
    stocks = database['data']
    Stock('NVDA', database)