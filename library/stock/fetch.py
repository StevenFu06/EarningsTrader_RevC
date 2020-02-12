import datetime as dt
import json
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup


"""API hub for stock.py
fetch.py inside stock package is meant to be a directory for all stock related api 
third party interactions. Meant to be easily exported into another program instead
of constantly being tied to the earnings package as well.

Class:
    WorldTrade: api for world trade
    ZachsApi: api for Zachs
"""


class Intraday:
    """API for intraday function of world trading data

    Attributes:
        raw_intra_data (json): the raw file without any processing from world trade data
        dates (list): list of dt.date for current raw file. Are in datetime format
        times (list): list of dt.time for current raw file Are in datetime format
        df_dict (dict): Intraday field for raw data, converted into df {volume:df, high:df...}
    """
    intraday_attrs = ['high', 'low', 'close', 'open', 'volume']
    URL = 'https://intraday.worldtradingdata.com/api/v1/intraday?symbol={ticker}' \
          '&range={range_of_data}&interval={interval_of_data}&api_token={api_key}'

    def __init__(self):
        self.raw_intra_data = None
        self.dates = []
        self.times = []
        self.df_dict = {}

    def dl_intraday(
            self,
            ticker: str,
            api_key: str,
            interval_of_data: int,
            range_of_data: int,
            surpress_message: bool = False
    ):
        """Download the data into a json format from world trade

        Parameters:
            :param ticker: ticker of stock of interest
            :param api_key: api key for world trading data
            :param interval_of_data: interval to download (i.e. 5 mins, 15 mins, 10 mins)
            :param range_of_data: range of data to download, max is 30 days
            :param surpress_message: supress the downloading message
        """
        if not surpress_message:  # Mainly for debugging purposes
            print(f'Downloading {ticker} from World Trading Data...')

        url = self.URL.format(
            ticker=ticker,
            range_of_data=range_of_data,
            interval_of_data=interval_of_data,
            api_key=api_key
        )
        data = requests.get(url)
        if data.status_code != 200:
            raise ConnectionError(f'code {data.status_code}')
        self.raw_intra_data = data.json()
        try:  # Check to see if the ticker was found, and intraday data is available
            self.raw_intra_data['intraday']
            return self
        except KeyError:
            raise FileNotFoundError(f'Error with ticker {ticker}: {self.raw_intra_data}')

    def raw_to_json(self, path, indent=4):
        """Save raw intra data to json"""

        with open(path, 'w') as save:
            json.dump(self.raw_intra_data, save, indent=indent)

    def read_raw_json(self, path):
        """Load raw_intra_data from file instead of world trade"""

        with open(path, 'r') as read:
            self.raw_intra_data = json.load(read)

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
        return self

    def _find_index_col(self):
        """From the raw json file, extract date and time, row and column data"""

        # Worldtrade raw data is in the form orient=index thankfully
        df = pd.DataFrame.from_dict(self.raw_intra_data['intraday'], orient='index')
        df.index = pd.to_datetime(df.index)

        self.dates = list(set(df.index.date))
        self.dates.sort()
        self.times = list(set(df.index.time))
        self.times.sort()

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

    Attributes:
        sector (str): is the sector according to Zach's
        industry (str): is the industry according to Zach's
        stock_activity (int): Zach's stock activity chart in dict form
        HEADERS (dict): is cause Zach is a lil bitch and doesnt like scraping
         _convert_dict (dict): convert to a more usable dictionary key set
        ticker: ticker to be downloaded
        suppress: setting on whether or not downloading message will be surpressed
    """
    HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                             'Chrome/79.0.3945.130 Safari/537.36'}
    _convert_dict = {'Open': 'open', 'Day Low': 'day_low', 'Day High': 'day_high', '52 Wk Low': '52_wk_low',
                     '52 Wk High': '52_wk_high', 'Avg. Volume': 'avg_volume', 'Market Cap': 'market_cap',
                     'Dividend': 'dividend', 'Beta': 'beta'}

    def __init__(self, ticker: str, surpress_message=False):
        self.url = 'https://www.zacks.com/stock/quote/' + ticker
        self.sector = None
        self.industry = None
        self._soup = None
        self.stock_activity = {}
        self.ticker = ticker
        self.suppress = surpress_message
        self._run()

    def _run(self):
        """Calls functions to populate attributes"""

        if not self.suppress:  # Mainly for debugging purposes
            print(f'Downloading {self.ticker} from Zachs...')
        self.getsoup()
        try:
            self._filter_sector()
            self._filter_stock_activity()
        except AttributeError:
            raise FileNotFoundError(f'{self.ticker} not found')

    def getsoup(self):
        """Fetches data from zachs"""

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

        def text_to_num(text):
            """Converts text to number"""
            #  For that one dumbass avg volume field which uses , instead of spaces
            text = text.replace(',', '')
            if ')' in text:
                return text[text.index('(') + 2:-1]  # for dividend where the format was 0.X ( 0.X%) which is stupid
            d = {'M': 6, 'B': 9}
            if text[-1] in d:
                num, magnitude = text[:-1], text[-1]
                return float(num) * 10 ** d[magnitude]
            else:
                return float(text)

        results = self._soup.find('section', {'id': 'stock_activity'}).find_all('td')
        for i in range(0, len(results) - 1, 2):  # Inc by 2 to get both category and value
            key = self._convert_dict[results[i].text]
            try:
                self.stock_activity[key] = text_to_num(results[i + 1].text)
            except ValueError:
                self.stock_activity[key] = results[i + 1].text


if __name__ == '__main__':
    wt = WorldTrade.Intraday()
    wt.dl_intraday('NVDA', 'bYoNpNAQNbpLSKQaMkcwrI68rniyZQDXL7B7aqYNPsHMrr0CRLIe3UYCfkHF', 5, 30)
    wt.to_dataframe()
    print(wt.df_dict)
