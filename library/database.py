from library.stock.stock import Stock
from library.stock.fetch import ZachsApi, Intraday
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
import os
import warnings

"""Database management module

Note:
    This module will deal EXCLUSIVELY with handling of the stock database, earnings and other will be 
    handled by their respective classes/ modules. For example, if a stock is blacklisted, earnings.py will
    deal with missing stocks/ blacklisted stocks appropriately. 

Class:
    JsonManager: Deals with the management of the local json database. 
        - Updater/ downloader: updates and filters out incomplete data
        - Health: Reports health and cleans the database of incomplete files/ corrupted data
"""


def calculate_threshold(wt: Intraday):
    wt.to_dataframe()
    return wt.df_dict['close'].count().sum()


class JsonManager:
    """Json database manager for interacting with the LOCAL json database

    Functions:
        update: using concurrent futures update the database with error/ complete data checking
        download_and_save: downloads and saves saves the passed stock.
        WARNING: download_and_save will overwrite existing stock if nothing passed in stock param
        download_list: downloads the given ticker list, WILL check for existing stocks

    Modes:
        delete: Deletes the stock from the database
        raise_error: raises once handler is called
        move: moves the problem stock to a specified move_to location
        ignore: ignores the problem and TRIES to force update the stock

    Attributes:
        :param database_path: Path to the local database
        :param incomplete_handler: if error happens, mode that determines how to handle problem stocks
        :param threshold: threshold in which the stock will be considered incomplete. Determined with
        calculate threshold and get_sample_data.
        :param SAMPLE_TICKERS: sample tickers in which the threshold will be calculated from.

    Kwargs:
        :param tolerance: tolerance on incomplete stocks, default is 0. (0 - 1 percentage)
        :param blacklist: all problem stocks will be appened to this parameter. Could preload list.
        :param _thread_or_multiprocess: choose the multiprocessing mode, default is multithread
        :param max_workers: choose maximum amount of workers to use for parallel programing
        :param range_of_data: how far back to download the data from. Default is max 30 days
        :param api_key: this required if any files needs to be downloaded
        :param surpress_message: surpress the "Download from ___" message
        :param move_to: the directory where the stock will be moved if mode="move_to"
    """
    SAMPLE_TICKERS = ['NVDA', 'AMD', 'TSLA', 'AAPL']

    def __init__(self, path_to_database: str, **kwargs):
        # Main variables
        self.database_path = path_to_database
        self.incomplete_handler = 'blacklist'
        self.threshold = 0

        # Initialize additional settings + error checking
        self._is_valid()
        self._kwarg_setter(kwargs)

    def _kwarg_setter(self, kwargs):
        """Kwarg dictionary for any additional settings"""

        # Concurrent settings
        self._thread_or_multiprocess(kwargs.pop('parallel_mode', 'multithread'))
        self.max_workers = kwargs.pop('max_workers', None)

        # Worldtrade api data
        self.api_key = kwargs.pop('api_key', None)
        self.range_of_data = kwargs.pop('range_of_data', 30)
        self.surpress_message = kwargs.pop('surpress_message', False)

        # For error checking
        self.tolerance = kwargs.pop('tolerance', 0)
        self.blacklist = kwargs.pop('blacklist', [])
        self.incomplete_handler = kwargs.pop('incomplete_handler', 'blacklist')
        self.move_to = kwargs.pop('move_to', None)

    def _thread_or_multiprocess(self, mode):
        """Sets the parallel processing mode"""

        if mode == 'multithread':
            self.parallel_mode = ThreadPoolExecutor
        elif mode == 'multiprocess':
            self.parallel_mode = ProcessPoolExecutor

    def _is_valid(self):
        """Check if path is a valid database and sets the interval_of_data"""

        try:
            self.all_stocks = [ticker[:-5] for ticker in os.listdir(self.database_path)]
            validate = Stock(self.all_stocks[0])
            validate.read_json(self.path_builder(self.all_stocks[0]))
            self.interval_of_data = validate.interval_of_data
        except FileNotFoundError:
            raise FileNotFoundError('Database was invalid, local databases only')

    def path_builder(self, ticker):
        """Returns built ticker path using given database location"""

        return os.path.join(self.database_path, ticker + '.json')

    def fetch_wt(self, ticker):
        """Calls worldtrade api and downloads raw intraday data"""

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

        Needs to be run before all updates unless incomplete stocks are being ignored.
        Generates threshold to evaulate incomplete stocks.
        """
        self.threshold = 0
        with self.parallel_mode(max_workers=self.max_workers) as executor:
            for wt in executor.map(self.fetch_wt, self.SAMPLE_TICKERS):
                self.threshold += calculate_threshold(wt)
            self.threshold = self.threshold/len(self.SAMPLE_TICKERS)
        self.threshold = self.threshold*(1 - self.tolerance)

    def update(self):
        """Updates with error checking and logging messages"""

        self.get_sample_data()
        with self.parallel_mode(max_workers=self.max_workers) as executor:
            message = [
                executor.submit(
                    self.download_and_save,
                    ticker, Stock(ticker).read_json(self.path_builder(ticker))
                )
                for ticker in self.all_stocks
            ]
            for future in as_completed(message):
                print(future.result())

    def download_list(self, tickers: list):
        """Downloads and saves the list of tickers

        Will check if stock exists, before downloading. If exists, stock will be updated.
        """
        # Checks if the stock exists
        def try_load(ticker):
            try:
                return Stock(ticker).read_json(self.path_builder(ticker))
            except FileNotFoundError:
                print('New ticker')
                return Stock(ticker)
        # Will use this dictionary to load the stock object instead of dynamically loading
        stock_dict = {
            ticker: try_load(ticker)
            for ticker in tickers
        }

        # Basically the same code as update
        self.get_sample_data()
        with self.parallel_mode(max_workers=self.max_workers) as executor:
            message = [
                executor.submit(
                    self.download_and_save,
                    ticker, stock_dict[ticker]
                )
                for ticker in tickers
            ]
            for future in as_completed(message):
                print(future.result())

    def download_and_save(self, ticker: str, stock: Stock = None):
        """Downloads from api, filters, then saves to local json database

        Note:
            One issue is that only 1 error will ever be raised. WT > Zachs > Threshold. I.e. if world trade
            error is raised, theres no way to know if the other 2 checks will also raise errors. Example,
            if zachs fails and wt goes through, theres no way of telling if the wt data is complete.

            When unknown error occurs, nothing happens to the stock. Ignore the stock doesnt even get
            passed into the handler

            WARNING: WILL OVERWRITE existing stock if preloaded stock is not passed into stock param.
        Parameters:
            :param ticker: ticker of interest
            :param stock: if nothiing is passed, a new stock will be initiated.
        """
        if stock is None:
            stock = Stock(ticker)
            if ticker in self.all_stocks:
                warnings.formatwarning = lambda msg, *args: f'{msg}\n'
                warnings.warn(f'{ticker} exists in database. {ticker} will be overwritten with new data')

        # Try catch to try to ensure program doesnt stop when download fails
        try:
            wt = self.fetch_wt(ticker)
        except FileNotFoundError:
            self.handler(ticker, stock, wt=False)
            return f'WorldTrade could not find {ticker}'
        except Exception as e:
            return f'WorldTrade: Error {e} was raised for {ticker}. {ticker} was ignored'

        try:
            zachs = ZachsApi(ticker, surpress_message=self.surpress_message)
        except FileNotFoundError:
            self.handler(ticker, stock, zachs=False)
            return f'Zachs could not find {ticker}'
        except Exception as e:
            return f'Zachs: Error {e} was raised for {ticker}. {ticker} was ignored'

        # Checks if downloaded stock passes threshold
        if calculate_threshold(wt) >= self.threshold:
            stock._load_from_wt(wt)
            stock._load_from_zachs(zachs)
            stock.to_json(self.path_builder(ticker))
            return f'{ticker} successfully saved'
        else:
            self.handler(ticker, stock)
            return f'{ticker} did not meet threshold'

    def handler(self, ticker: str, stock: Stock, wt=True, zachs=True):
        """Handler for incomplete/errors when downloading

        Notes:
            - a blacklist will always be returned with stocks that caused a problem
            - Ignore mode is dangerous/ inefficient. Will force update stocks even with
            incomplete stocks. Better alternative is to use tolerance = 1 to ignore the threshold.
            - Ignore mode also does not check both wt and zachs, wt > zachs in terms of importance.

        Parameters:
            :param ticker: ticker stock of interest (all caps)
            :param stock: stock object that is preloaded, mainly used for mode "ignore" for saving
            :param wt: used for mode "ignore" to determine if wt threw error when downloading
            :param zachs: used for mode "ignore" to determine if zachs threw error when downloading

        Modes:
            delete: Deletes the stock from the database
            raise_error: raises once handler is called
            move: moves the problem stock to a specified move_to location
            ignore: ignores the problem and TRIES to force update the stock
        """
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
        # Not both wt and zachs get checked so may fail alot
        # Api will do a double call when ignore, once in main download, once now
        elif self.incomplete_handler == 'ignore':
            try:
                stock.fetch(
                    self.api_key,
                    range_of_data=self.range_of_data,
                    zachs=zachs,
                    world_trade=wt
                )
                stock.to_json(self.path_builder(ticker))
            # Because force update, many errors could occur.
            except Exception as e:
                warnings.formatwarning = lambda msg, *args: f'{msg}\n'
                warnings.warn(f'Was not able to force update {ticker}. {e} was raised')









