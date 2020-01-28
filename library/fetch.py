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
    """WorldTrade is meant to deal with all things involving World Trading Data's api"""

    def __init__(self):
        pass

    class Intraday:
        """API for intraday function of world trading data"""

        intraday_attrs = ['high', 'low', 'close', 'open', 'volume']

        def __init__(self, api_key):
            self.raw_intra_data = None  # Raw json data downloaded from world trading data
            self.dates = []  # All dates for current raw file
            self.times = []  # All times for current raw file
            self.df_dict = {}  # intraday field of raw data containing df from json {volume:df, high:df...}
            self.url = None  # Full url used to download
            self.api_key = api_key  # key for account

        def dl_intraday(self, ticker, interval_of_data, range_of_data):
            """Download the data into a json format from world trade

            :param ticker: ticker of stock of interest
            :param interval_of_data: interval to download (i.e. 5 mins, 15 mins, 10 mins)
            :param range_of_data: range of data to download, max is 30 days
            """
            self.url = f'https://intraday.worldtradingdata.com/api/v1/intraday?symbol={ticker}\
            &range={str(range_of_data)}&interval={str(interval_of_data)}&api_token={self.api_key}'

            data = requests.get(self.url)
            if data.status_code != 200:
                raise ConnectionError(f'code {data.status_code}')
            self.raw_intra_data = data.json()
            return self.raw_intra_data

        def save_as_json(self, path, indent=4):
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
            """From the raw json file, extract date and time, row and column data"""

            intraday = self.raw_intra_data['intraday']
            for i in intraday:
                datetime = dt.datetime.strptime(i, '%Y-%m-%d %H:%M:%S')
                self.dates.append(datetime.date())
                self.times.append(datetime.time())
            self.dates = list(sorted(set(self.dates)))
            self.times = list(sorted(set(self.times)))

        def _return_intraday(self, key):
            intraday = self.raw_intra_data
            # Try except used for performance gains
            try:
                return intraday[key]
            except KeyError:
                return np.nan

        def _rebuild_key(self):
            """
            The raw data is missing time data for certain days due to missing data/ holidays cause stock market
            to close early. Rebuilding the key along with _return_intraday will fill missing time data with np.nan.
            """
            rebuilt = []
            for date in self.dates:
                for time in self.times:
                    rebuilt.append(dt.datetime.combine(date, time).strftime('%Y-%m-%d %H:%M:%S'))
            return rebuilt


class EarningsCalendar:
    """Earnings date information scraped from yahoo finance"""

    DELAY = 0.5  # delay in seconds between url calls
    CALL_TIME_DICT = {
        'Time Not Supplied': 'N/A',
        'Before Market Open': 'BMO',
        'After Market Close': 'AMC',
        'TAS': 'N/A'
    }

    def __init__(self, date):
        self.date = date.strftime('%Y-%m-%d')  # date of interest
        self.earnings = {'date': self.date, 'tickers': {}}  # Earnings list
        self.url = None  # url to yahoo
        self._soup = None  # html soup from beautiful soup
        self.offset = 0  # offset to be used in the url
        self.num_stocks = 0  # number of stocks that has earnings scheduled on date
        self.num_pages = 0  # number of pages of data there are, usually
        self.run()

    def getsoup(self):
        """returns soup in html format.

        The entire class shares a single soup variable, and gets constantly updated.
        self._soup should be not used outside of these functions, mainly used for easy implementation within class
        """
        self.url = f'https://finance.yahoo.com/calendar/earnings?day={self.date}&offset={self.offset}&size=100'
        yahoo = requests.get(self.url)
        if yahoo.status_code != 200:
            raise ConnectionError(f'code {yahoo.status_code}')
        src = yahoo.content
        self._soup = BeautifulSoup(src, 'html.parser')

    def pages(self):
        """gets number of pages and stocks that exists on the yahoo site"""

        self.getsoup()  # get soup to extract data
        results = self._soup.find('span', {'class': 'Mstart(15px) Fw(500) Fz(s)'}).text
        self.num_stocks = int(results[results.index('of') + 3: -len('results')])
        self.num_pages = int((self.num_stocks - 1) / 100)

    def filter_data(self):
        """Filters soup data and returns dict {ticker: BMO, ticker2: AMC, etc...}"""
        
        # Results white and results_alt are due to the fact that yahoo chart splits into grey and white
        # gets all the soup data from white lines
        results_white = self._soup.findAll('tr', {
            'class': 'simpTblRow Bgc($extraLightBlue):h BdB Bdbc($finLightGrayAlt) Bdbc($tableBorderBlue):h H(32px) '
                     'Bgc(white)'})

        # gets all the soup data from alternate (grey) lines
        results_alt = self._soup.findAll('tr', {
            'class': 'simpTblRow Bgc($extraLightBlue):h BdB Bdbc($finLightGrayAlt) Bdbc($tableBorderBlue):h H(32px) '
                     'Bgc($altRowColor)'})

        # filters results_white data into the main dict
        for i in results_white:
            ticker = i.find('a', {'class', 'Fw(600)'}).text
            time = i.find('td', {'class', 'Va(m) Ta(end) Pstart(15px) W(20%) Fz(s)'}).text
            time = self.CALL_TIME_DICT[time]
            self.earnings['tickers'][ticker] = time

        # filters results_alt data into the main dict
        for i in results_alt:
            ticker = i.find('a', {'class', 'Fw(600)'}).text
            time = i.find('td', {'class', 'Va(m) Ta(end) Pstart(15px) W(20%) Fz(s)'}).text
            time = self.CALL_TIME_DICT[time]
            self.earnings['tickers'][ticker] = time

    def run(self):
        """Bread and budder, generates the earnings list"""

        self.pages()
        self.filter_data()
        for i in range(1, self.num_pages + 1):
            py_time.sleep(self.DELAY)
            self.offset = 100 * i
            self.getsoup()
            self.filter_data()

    def save_as_json(self, path, indent=4):
        with open(path, 'wb') as save:
            save.dump(self.earnings, save, indent=indent)


if __name__ == '__main__':
    WorldTrade.Intraday('qwe').dl_intraday('123', 14, 14)
