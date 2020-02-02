import requests
import json
import datetime as dt
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

"""API hub for stock.py
fetch.py inside stock package is meant to be a directory for all stock related api 
third party interactions. Meant to be easily exported into another program instead
of constantly being tied to the earnings package as well.

Class:
    WorldTrade: api for world trade
    ZachsApi: api for Zachs
"""


class WorldTrade:
    """WorldTrade is meant to deal with all things involving World Trading Data's api"""

    def __init__(self):
        pass

    class Intraday:
        """API for intraday function of world trading data

        Args:
            :arg intraday_attrs (list): list of the attributes that get returned from world trade
            :arg raw_intra_data (json): the raw file without any processing from world trade data
            :arg dates (list): list of dt.date for current raw file. Are in datetime format
            :arg times (list): list of dt.time for current raw file Are in datetime format
            :arg df_dict (dict): Intraday field for raw data, converted into df {volume:df, high:df...}
            :arg url (str): url used
            :arg api_key (str): api key for world trade
        """
        intraday_attrs = ['high', 'low', 'close', 'open', 'volume']

        def __init__(self, api_key: str):
            self.raw_intra_data = None
            self.dates = []
            self.times = []
            self.df_dict = {}
            self.url = None
            self.api_key = api_key

        def dl_intraday(self, ticker: str, interval_of_data: int, range_of_data: int):
            """Download the data into a json format from world trade

            Parameters:
                :param ticker:(str) ticker of stock of interest
                :param interval_of_data:(int) interval to download (i.e. 5 mins, 15 mins, 10 mins)
                :param range_of_data:(int) range of data to download, max is 30 days
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
            """Raw json price data to intraday dict

            Converts data under intraday key to a dictionary of DataFrames, filling in any missing values with np.nan.
            {attr: df, attr2: df, attr3: df...}
            :return df_dict
            """
            self._find_index_col()
            keys = self._rebuild_key()
            for attr in self.intraday_attrs:
                data = [self._return_intraday(key, attr) for key in keys]
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

        def _return_intraday(self, key, attr):
            intraday = self.raw_intra_data['intraday']
            # Try except used for performance gains
            try:
                return float(intraday[key][attr])
            except KeyError:
                return np.nan

        def _rebuild_key(self):
            """
            The raw data is missing time data for certain days due to missing data/ holidays cause stock market
            to close early. Rebuilding the key along with _return_intraday will fill missing time data with np.nan.
            """
            rebuilt = [dt.datetime.combine(date, time).strftime('%Y-%m-%d %H:%M:%S')
                       for date in self.dates for time in self.times]
            return rebuilt


class ZachsApi:
    """ Get Sector information courtesy of Zach's

    Args:
        :arg sector (str): is the sector according to Zach's
        :arg industry (str): is the industry according to Zach's
        :arg stock_activity (int): Zach's stock activity chart in dict form
        :arg HEADERS (dict): is cause Zach is a lil bitch and doesnt like scraping
        :arg _convert_dict (dict): convert to a more usable dictionary key set
    """
    HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                             'Chrome/79.0.3945.130 Safari/537.36'}
    _convert_dict = {'Open': 'open', 'Day Low': 'day_low', 'Day High': 'day_high', '52 Wk Low': '52_wk_low',
                     '52 Wk High': '52_wk_high', 'Avg. Volume': 'avg_volume', 'Market Cap': 'market_cap',
                     'Dividend': 'dividend', 'Beta': 'beta'}

    def __init__(self, ticker: str):
        self.url = 'https://www.zacks.com/stock/quote/' + ticker
        self.sector = None
        self.industry = None
        self._soup = None
        self.stock_activity = {}
        self._run()

    def _run(self):
        self.getsoup()
        self._filter_sector()
        self._filter_stock_activity()

    def getsoup(self):
        data = requests.get(self.url, headers=self.HEADERS)
        if data.status_code != 200:
            raise ConnectionError(f'code {data.status_code}')
        src = data.content
        self._soup = BeautifulSoup(src, 'html.parser')

    def _filter_sector(self):
        """get the ticker sector and industry information"""

        results = self._soup.find_all('table', {'class': 'abut_top'})
        for result in results:
            self.sector = result.find_all('a')[0].text
            self.industry = result.find_all('a')[1].text

    def _filter_stock_activity(self):
        """Get the ticker all important stock information located under stock activity from zachs"""

        results = self._soup.find('section', {'id': 'stock_activity'}).find_all('td')
        for i in range(0, len(results) - 1, 2):
            key = self._convert_dict[results[i].text]
            self.stock_activity[key] = text_to_num(results[i + 1].text)


def text_to_num(text):
    """Converts text to number"""

    #  For that one dumbass avg volume field which uses , instead of spaces
    text = text.replace(',', '')
    if ')' in text:
        return text[text.index('(')+2:-1]  # for dividend where the format was 0.X ( 0.X%) which is stupid
    d = {'M': 6,'B': 9}
    if text[-1] in d:
        num, magnitude = text[:-1], text[-1]
        return float(num) * 10 ** d[magnitude]
    else:
        return float(text)


if __name__ == '__main__':
    wt = WorldTrade.Intraday('bYoNpNAQNbpLSKQaMkcwrI68rniyZQDXL7B7aqYNPsHMrr0CRLIe3UYCfkHF')
    wt.dl_intraday('NVDA', 5, 30)
    wt.to_dataframe()
    print(wt.df_dict)