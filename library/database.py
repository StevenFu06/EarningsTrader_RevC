from library.stock.stock import Stock
from library.stock.fetch import ZachsApi, Intraday
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import repeat
import os

"""Database management module

Note:
    This module will deal EXCLUSIVELY with handling of the stock database, earnings and other will be 
    handled by their respective classes/ modules. For example, if a stock is blacklisted, earnings.py will
    deal with missing stocks/ blacklisted stocks appropriately. 

Class:
    JsonManager: Deals with the management of the local json database. 
        - Updater: updates and filters out incomplete data
        - Clenaer: Cleans the database of incomplete files/ corrupted data
"""


def calculate_threshold(wt: Intraday):
    wt.to_dataframe()
    return len(wt.dates) + len(wt.times) + wt.df_dict['close'].isna().sum().sum()


class JsonManager:
    SAMPLE_TICKERS = ['NVDA', 'AMD', 'TSLA', 'AAPL']

    def __init__(self, path_to_database: str, **kwargs):
        # Main variables
        self.dir_path = path_to_database
        self.threshold = 0
        self._is_valid()
        self._kwarg_setter(kwargs)

    def _kwarg_setter(self, kwargs):
        self.max_workers = kwargs.pop('max_workers', None)

        # Worldtrade api data
        self.api_key = kwargs.pop('api_key', None)
        self.range_of_data = kwargs.pop('range_of_data', 30)
        self.surpress_message = kwargs.pop('surpress_message', False)

        # For error checking
        self.blacklist = kwargs.pop('blacklist', [])
        self.incomplete_handler = kwargs.pop('incomplete_handler', 'blacklist')
        self.move_to = kwargs.pop('move_to', None)

    def _is_valid(self):
        try:
            self.all_stocks = [ticker[:-5] for ticker in os.listdir(self.dir_path)]
            validate = Stock(self.all_stocks[0])
            validate.read_json(self.path_builder(self.all_stocks[0]))
            self.interval_of_data = validate.interval_of_data
        except FileNotFoundError:
            raise FileNotFoundError('Database was invalid, local databases only')

    def path_builder(self, ticker):
        """Returns built ticker path using given database location"""

        return os.path.join(self.dir_path, ticker + '.json')

    def fetch_wt(self, ticker):
        wt = Intraday().dl_intraday(
            ticker,
            self.api_key,
            self.interval_of_data,
            self.range_of_data,
            surpress_message=self.surpress_message
        )
        return wt

    def get_sample_data(self):
        """Populate the expected num attributes by using sample tickers
        Needs to be run before all updates unless incomplete stocks are being ignored. Generates threashold
        to evaulate incomplete stocks.
        """
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for wt in executor.map(self.fetch_wt, self.SAMPLE_TICKERS):
                self.threshold += calculate_threshold(wt)
            self.threshold = self.threshold/len(self.SAMPLE_TICKERS)

    def update(self):
        self.get_sample_data()

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            message = [
                executor.submit(
                    self.download_and_save,
                    ticker, Stock(ticker).read_json(self.path_builder(ticker))
                )
                for ticker in self.all_stocks
            ]
            for future in message:
                print(future.result())

    def download_and_save(self, ticker: str, stock: Stock):
        """Downloads from api, filters, then saves to local json database

        Note:
            One issue is that only 1 error will ever be raised. WT > Zachs > Threshold. I.e. if world trade
            error is raised, theres no way to know if the other 2 checks will also raise errors. Example,
            if zachs fails and wt goes through, theres no way of telling if the wt data is complete.

            When connection error occurs, nothing happens to the stock, equal to incomplete_handler='do_nothing'
        """
        try:
            wt = self.fetch_wt(ticker)
        except FileNotFoundError:
            self.handler(ticker, stock, wt=False)
            return f'WorldTrade raised error occured when downloading {ticker}'
        except ConnectionError:
            return f'WorldTrade: Connection error with {ticker}. {ticker} was ignored'

        try:
            zachs = ZachsApi(ticker, surpress_message=self.surpress_message)
        except FileNotFoundError:
            self.handler(ticker, stock, zachs=False)
            return f'Zachs raised error occured when downloading {ticker}'
        except ConnectionError:
            return f' Zachs: Connection error with {ticker}. {ticker} was ignored'

        if calculate_threshold(wt) < self.threshold:
            self.handler(ticker, stock)
            return f'{ticker} did not meet threshold'
        else:
            stock._load_from_wt(wt)
            stock._load_from_zachs(zachs)
            stock.to_json(self.path_builder(ticker))
            return f'{ticker} successfully saved'

    def handler(self, ticker, stock: Stock, wt=True, zachs=True):

        if self.incomplete_handler != 'do_nothing':
            self.blacklist.append(ticker)

        if self.incomplete_handler == 'delete':
            os.remove(self.path_builder(ticker))

        elif self.incomplete_handler == 'raise_error':
            raise FileNotFoundError(f'{ticker} was not complete or error occured')

        elif self.incomplete_handler == 'move':
            if self.move_to is None:
                raise NotADirectoryError('No "move to" directory was given')
            else:
                os.rename(
                    self.path_builder(ticker),
                    os.path.join(self.move_to, ticker + '.json')
                )

        elif self.incomplete_handler == 'ignore':
            stock.fetch(
                self.api_key,
                range_of_data=self.range_of_data,
                zachs=zachs,
                world_trade=wt
            )
            stock.to_json(self.path_builder(ticker))








