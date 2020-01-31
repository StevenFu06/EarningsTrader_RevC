from library.stock import fetch
import datetime as dt
import pandas as pd
import pymongo as db
import os

"""Main stock module 
Interacts with erryting basically, a life saver, one stop shop for all stock data
"""


class Stock:
    """stock api with shit loads of data

    Note:
        All df index and columns will be in datetime format if applicable

    Args:
        :arg INTRADAY (list): Attributes for wt.intraday attributes. Data type of dataframe
        :arg INFO (list): includes misc data for stocks/ single point entry that doesnt change based on date
        :arg HISTORICAL (list): any attributes that change based on date that needs to be tracked
        :arg MARKET_DICT (dict): convert market names to market tickers for consistency

    Parameter:
        :param ticker (str): ticker of interest
        :param load_from: load method, from mongo, json, fetch, update etc... Interacts with _load_method
    """

    INTRADAY = [
        'open',
        'close',
        'high',
        'low',
        'volume'
    ]
    INFO = [
        'market',
        'sector',
        'industry',
    ]
    HISTORICAL = [
        'market_cap',
        'avg_volume',
        'beta',
        'dividend',
    ]
    MARKET_DICT = {
        'NASDAQ': '^IXIC',
        'INDEXNASDAQ': '^IXIC',
        'INDEXNYSEGIS': '^NYA',
        'NYSEAMEX': '^XAX',
        '^XAX': '^XAX',
        'NYSE': '^NYA',
        'OTC': '^IXIC',
        'NYSEAMERICAN': '^NYA'
    }

    def __init__(self, ticker: str, load_from, **kwargs):
        self.load_from = load_from
        self.ticker = ticker
        self._load_method(**kwargs)

    def _load_method(self, **kwargs):
        """Automatically select load method including fetch and updating

        Options:
            fetch:
                Will get data from fetch.py and will not update anything. fetch will OVERWRITE
                any existing stock attributes

                :param kwargs: will take parameters to be passed into fetch from wt

            update:
                Will update the existing attributes and add new ones if any are none
        """

        if self.load_from == 'fetch':
            self._fetch_from_wt(kwargs.get('api_key'), kwargs.get('interval_of_data'), kwargs.get('range_of_data'))
            self._fetch_from_zachs()

    def _fetch_from_wt(self, api_key: str, interval_of_data: int, range_of_data=30):
        """Fetches data from world trade data, using fetch.py api

        Will set INTRADAY parameters and market
        Parameters
            :param api_key: api key to world trade
            :param interval_of_data: how often the data is. Available every 1, 5, 15, or 60
            :param range_of_data: Default is 30 days, but 30 days is not available for 1 minute (7 days)
        """
        range_of_data = 30 if range_of_data is None else range_of_data

        wt = fetch.WorldTrade.Intraday(api_key)
        wt.dl_intraday(self.ticker, interval_of_data, range_of_data)
        self.market = self.MARKET_DICT[wt.raw_intra_data['stock_exchange_short']]

        wt.to_dataframe()  # wt.df_dict has dataframe already converted to datetime
        for i in self.INTRADAY:
            setattr(self, i, wt.df_dict[i])

    def _fetch_from_zachs(self):
        """Fetches data from zachs using fetch.ZachsApi

        Will set HISTORICAL parameters, sector and industry
        """

        zachs = fetch.ZachsApi(self.ticker)
        for i in self.HISTORICAL:
            # Note index is datetime format
            temp_df = pd.Series(data=zachs.stock_activity[i],
                                index=[dt.datetime.now().date()],
                                name=i)
            setattr(self, i, temp_df)
        self.sector = zachs.sector
        self.industry = zachs.industry


if __name__ == '__main__':
    nvda = Stock('NVDA', 'fetch',
                 api_key='bYoNpNAQNbpLSKQaMkcwrI68rniyZQDXL7B7aqYNPsHMrr0CRLIe3UYCfkHF',
                 interval_of_data=5,
                 range_of_data=4)
    test = '2020-01-30'
    date_test = dt.date(2020, 1, 30)
    print(nvda.market_cap.loc[dt.datetime.now().date()])
    print(nvda.market_cap)
    print(nvda.market)
    print(nvda.industry)
