from library.stock.stock import Stock
from library.stock.fetch import WorldTrade, ZachsApi
from concurrent.futures import ThreadPoolExecutor
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
        that do not match the expected outcome will be handeld based on 'incomplete_handler' mode.

    Incomplete modes:
        delete: deletes the stock from database
        blacklist: adds the stock to a blacklist saved somewhere
        move: move incomplete stocks to a different folder. If mongo, another collection needs to be given
        ignore: ignore incomplete stocks and continue as if they were compelete

    Attributes:
        api_key (str): api key for world trade data
        range_of_data (int): range of data to be collected
        incomplete_handler (str): method of how incomplete data should be handeled
        expected_num_dates, times, nan (int) : generated from calling sample stocks and is the criteria
        used to filter incomplete stocks
    """
    SAMPLE_TICKERS = ['NVDA', 'AMD', 'TSLA', 'AAPL']

    def __init__(self, api_key: str, range_of_data: int = 30, incomplete_handler: str = 'ignore', **kwargs):
        self.api_key = api_key
        self.range_of_data = range_of_data
        self.handler = incomplete_handler
        self.kwargs = kwargs
        self.all_stocks = None
        self.expected_num_dates = 0
        self.expected_num_times = 0
        self.expected_num_nan = 0

    def get_sample_data(self, interval_of_data):
        """Populate the expected num attributes by using sample tickers

        Needs to be run before all updates unless incomplete stocks are being ignored. Generates criteria
        to evaulate incomplete stocks.
        """
        # For easy looping purpossssessssssssssssss
        attrs = ['expected_num_dates', 'expected_num_times', 'expected_num_nan']

        # Used for concurrent future for faster fetching of les data
        def num_data_points(ticker):
            wt = WorldTrade.Intraday()
            wt.dl_intraday(ticker, self.api_key, interval_of_data, self.range_of_data, surpress_message=True)
            wt.to_dataframe()
            return len(wt.dates), len(wt.times), wt.df_dict['close'].isna().sum().sum()

        # Calculate and set the expected_num attributes
        with ThreadPoolExecutor() as executor:
            for results in executor.map(num_data_points, self.SAMPLE_TICKERS):
                for i in range(len(attrs)):
                    setattr(self, attrs[i], getattr(self, attrs[i])+results[i])
        for attr in attrs:
            setattr(self, attr, getattr(self, attr)/len(self.SAMPLE_TICKERS))

    def json_database(self, path):
        pass

