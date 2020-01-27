import requests
import json
import datetime as dt
import pandas as pd
import numpy as np
import time as py_time
from bs4 import BeautifulSoup


"""
fetch.py is a module for getting/ connecting to internet and grabbing data.
Any and all programs/methods that involve connecting to the internet/ another 3rd party
provider will be done through fetch.py.
"""


class WorldTrade:
    """
    WorldTrade is meant to deal with all things involving World Trading Data's api. Meant to be a header/ title to make
    grouping of various World Trade downloading easier.
    """

    def __init__(self):
        pass

    class Intraday:
        """
        Intraday world trading data to a more easily manipulated.
        """
        intraday_attrs = ['high', 'low', 'close', 'open', 'volume']

        def __init__(self, api_key):
            self.raw_intra_data = None  # Raw json data downloaded from world trading data
            self.dates = []  # All dates for current raw file
            self.times = []  # All times for current raw file
            self.df_dict = {}  # intraday field of raw data containing df from json {volume:df, high:df...}
            self.url = None  # Full url used to download
            self.api_key = api_key  # key for account

        def dl_intraday(self, ticker, interval_of_data, range_of_data):
            self.url = f'https://intraday.worldtradingdata.com/api/v1/intraday?symbol={ticker}\
            &range={str(range_of_data)}&interval={str(interval_of_data)}&api_token={self.api_key}'

            data = requests.get(self.url)
            if data.status_code != 200:
                raise ConnectionError(f'code {data.status_code}')
            self.raw_intra_data = data.json()
            return self.raw_intra_data

        def save_raw(self, path, indent=4):
            with open(path, 'w') as save:
                json.dump(self.raw_intra_data, save, indent=indent)

        def to_dataframe(self):
            """
            Converts data under intraday key to a dictionary of DataFrames, filling in any missing values with np.nan
            """
            keys = self._rebuild_key()
            for attr in self.intraday_attrs:
                data = []
                for key in keys:
                    data.append(self._return_intraday(key))

                np_data = np.array(data).reshape([len(self.dates), len(self.times)])
                self.df_dict[attr] = pd.DataFrame(data=np_data, index=self.dates, columns=self.times)
            return self.df_dict

        def _find_index_col(self):
            """
            From the raw json file, extract date and time, row and column data
            """
            intraday = self.raw_intra_data['intraday']
            for i in intraday:
                datetime = dt.datetime.strptime(i, '%Y-%m-%d %H:%M:%S')
                self.dates.append(datetime.date())
                self.times.append(datetime.time())
            self.dates = list(sorted(set(self.dates)))
            self.times = list(sorted(set(self.times)))

        def _return_intraday(self, key):
            intraday = self.raw_intra_data
            try:
                return intraday[key]
            except KeyError:
                return np.nan

        def _rebuild_key(self):
            """
            The raw data is missing time data for certain days due to missing data/ holidays that cause stock market
            to close early. Rebuilding the key along with _return_intraday will fill missing time data with np.nan.
            """
            rebuilt = []
            for date in self.dates:
                for time in self.times:
                    rebuilt.append(dt.datetime.combine(date, time).strftime('%Y-%m-%d %H:%M:%S'))
            return rebuilt


if __name__ == '__main__':
    world_trade = WorldTrade.Intraday('bYoNpNAQNbpLSKQaMkcwrI68rniyZQDXL7B7aqYNPsHMrr0CRLIe3UYCfkHF')
    world_trade.dl_intraday('NVDA', 5, 30)
    world_trade.to_dataframe()
