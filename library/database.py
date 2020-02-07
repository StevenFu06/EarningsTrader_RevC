from library.stock.stock import Stock
from library.stock.fetch import WorldTrade, ZachsApi
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import repeat

import os

"""Database management module

Note:
    This module will deal EXCLUSIVELY with handling of the stock database, earnings and other will be 
    handled by their respective classes/ modules. For example, if a stock is blacklisted, earnings.py will
    deal with missing stocks/ blacklisted stocks appropriately. 

Class:
    Updater: updates the given database including error checking when fetching data
    cleanup: clean/ error checks the existing database 
    change: changes 1 form of database to another?
"""


class Updater:
    """Updates and error checks the given database

    Note:
        First downloads from api then checks for error based on SAMPLE_TICKERS that are given/ default. Any
        that do not match the threashold will be handeld based on 'incomplete_handler' mode. Any errors that
        occur, i.e. world trade doesnt have a ticker, will also be handled by incomplete_handler

    Incomplete modes:
        delete: deletes the stock from database
        blacklist: adds the stock to a blacklist saved somewhere. If a list not given, ticker will be appened
        to an an empty list.
        move: move incomplete stocks to a different folder. If mongo, another collection needs to be given
        ignore: ignore incomplete stocks and continue as if they were compelete
        do_nothing: different from ignore, will skip the stock entirely, pretend it doesn't exist
        raise_exception: raise exception on finding incomplete stock

    Attributes:
        api_key (str): api key for world trade data
        range_of_data (int): range of data to be collected
        incomplete_handler (str): method of how incomplete data should be handeled
        threashold (float): threashold from sample stocks. If not met stock will be considered incomplete
        max_workers (int)(optional): maximum workers allowed for ThreadPoolExecutor
        blacklist (list)(optional): a literal blacklist in **LIST** type. If not given, and mode is blacklist
        blacklisted stock will be appened an empty array

    Parameters:
        :param api_key: api for world trade
        :param incomplete_handler: how incomplete files will be handled
        :param range_of_data: range of data to download, more information on fetch.py
        :param interval_of_data: time between data points
        :param wt: short for World Trade Intraday object
    """
    SAMPLE_TICKERS = ['NVDA', 'AMD', 'TSLA', 'AAPL']

    def __init__(self, api_key: str, range_of_data: int = 30, incomplete_handler: str = 'ignore', **kwargs):
        self.api_key = api_key
        self.range_of_data = range_of_data
        self.handler = incomplete_handler
        self.all_stocks = None
        self.threashold = 0
        self.downloaded = {}
        self._kwarg_options(kwargs)

    def _kwarg_options(self, kwargs):
        self.max_workers = kwargs.pop('max_workers', None)
        self.blacklist = kwargs.pop('blacklist', [])

    @staticmethod
    def _calculate_threshold(wt: WorldTrade.Intraday):
        return len(wt.dates) + len(wt.times) + wt.df_dict['close'].isna().sum().sum()

    def _fetch_wt(self, ticker: str, interval_of_data: int):
        """Download the data from world trade"""

        wt = WorldTrade.Intraday()
        wt.dl_intraday(ticker, self.api_key, interval_of_data, self.range_of_data)
        wt.to_dataframe()
        return wt

    def get_sample_data(self, interval_of_data):
        """Populate the expected num attributes by using sample tickers

        Needs to be run before all updates unless incomplete stocks are being ignored. Generates threashold
        to evaulate incomplete stocks.
        """
        # Create repeated interval_of_data for pool.map function
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for result in executor.map(self._fetch_wt, self.SAMPLE_TICKERS, repeat(interval_of_data)):
                self.threashold += self._calculate_threshold(result)
            self.threashold = self.threashold/len(self.SAMPLE_TICKERS)

    def download(self, tickers: list, interval_of_data: int):
        self.get_sample_data(interval_of_data)
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            results = {
                executor.submit(self._fetch_wt, ticker, interval_of_data): ticker
                for ticker in tickers
            }
            for future in as_completed(results):
                ticker = results[future]
                try:
                    wt = future.result()
                    if self._calculate_threshold(wt) >= self.threashold or self.handler == 'ignore':
                        self.downloaded[ticker] = wt
                    else:
                        self._incomeplete(ticker)
                except FileNotFoundError:
                    self._incomeplete(ticker)
                    continue

    def _incomeplete(self, ticker):
        if self.handler == 'do_nothing':
            pass
        elif self.handler == 'blacklist':
            self.blacklist.append(ticker)
        elif self.handler == 'raise_exception':
            raise FileNotFoundError(f'{ticker} was fetched with incomplete data')
