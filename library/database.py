import datetime as dt
import os
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
import json
import pandas as pd
import pandas_market_calendars as mcal
from library.stock.fetch import ZachsApi, Intraday
from library.stock.stock import Stock

"""Database management module

Note:
    This module will deal EXCLUSIVELY with handling of the stock database, earnings and other will be 
    handled by their respective classes/ modules. For example, if a stock is blacklisted, earnings.py will
    deal with missing stocks/ blacklisted stocks appropriately. 

Class:
    JsonManager: Deals with the management of the local json database. 
        - Updater/ downloader: updates and filters out incomplete data
    Health: Health of indivudual stocks... i.e. corrupted data, incomplete files etc...
    Report: Reports health of a given stock list, multithreaded
"""


def calculate_threshold(wt: Intraday):
    """Threshold to determine quality of data"""

    wt.to_dataframe()
    return wt.df_dict['close'].count().sum()


class JsonManager:
    """Json database manager for interacting with the LOCAL json database

    Functions:
        update: using concurrent futures update the database with error/ complete data checking
        download_and_save: downloads and saves saves the passed stock.
        WARNING: download_and_save will overwrite existing stock if nothing passed in stock param
        download_list: downloads the given ticker list, WILL check for existing stocks
        exists: tests if ticker is in database
        delete: deletes the stock
        load_all: loads all tickers as stock objects in a list

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
            self.all_tickers = [ticker[:-5] for ticker in os.listdir(self.database_path)]
            validate = Stock(self.all_tickers[0])
            validate.read_json(self.path_builder(self.all_tickers[0]))
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
            self.threshold = self.threshold / len(self.SAMPLE_TICKERS)
        self.threshold = self.threshold * (1 - self.tolerance)

    def update(self):
        """Updates with error checking and logging messages"""

        self.get_sample_data()
        with self.parallel_mode(max_workers=self.max_workers) as executor:
            message = [
                executor.submit(
                    self.download_and_save,
                    ticker, Stock(ticker).read_json(self.path_builder(ticker))
                )
                for ticker in self.all_tickers
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
                print(f'{ticker} is new')
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
            if ticker in self.all_tickers:
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

    def handler(self, ticker: str, stock: Stock = None, wt=True, zachs=True):
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

    def exists(self, ticker):
        """Tests if stock is in database"""

        if ticker in self.all_tickers:
            return True
        return False

    def delete(self, ticker):
        """Deletes the stock"""

        if self.exists(ticker):
            os.remove(self.path_builder(ticker))
        else:
            raise FileNotFoundError(f'{ticker} not in database')

    def load_all(self):
        """Using multithreading load all stocks from disk

        :return list of loaded stocks (loaded_stocks)
        """
        loaded_stocks = [
            Stock(ticker).read_json(self.path_builder(ticker))
            for ticker in self.all_tickers
        ]
        return loaded_stocks

    def clean_database(self, report: dict):
        """All stocks in the report, will be passed into incomplete handler

        Note: cannot use mode raise_error and ignore for incomplete_handler
        as they will not do anything to the tickers in the report.

        :param report: dictionary file generated by class Report
        """
        if self.incomplete_handler == 'raise_error' or self.incomplete_handler == 'ignore':
            raise RuntimeError('Incomplete handler mode cannot clean database')
        for ticker in report:
            self.handler(ticker)

    def convert_to_legacy(self, legacy_path: os.path):
        """Converts the json database to legacy database"""

        # Tests if is currently a valid database
        try:
            pd.read_csv(os.path.join(legacy_path, 'database.csv'))
        except FileNotFoundError:
            #  Creates the database file if doesnt exist
            pd.DataFrame(columns=['market', 'first_date', 'last_date'])\
                .to_csv(os.path.join(legacy_path, 'database.csv'))

        for ticker in self.all_tickers:
            save_csv = Stock(ticker).read_json(self.path_builder(ticker))
            save_csv.to_legacy_csv(legacy_path)
            print(f'Successfully converted {ticker}')


def legacy_to_json(legacy_path: os.path, json_path: os.path, autopopulate=True, pop_list=None):
    """Convert legacy db to json db

    Parameters:
        :param legacy_path: the path to old database with database.csv
        :param json_path: new database location
        :param autopopulate: whether or not to autopopulate zachs data
        :param pop_list: any additional files to pop

    :return a list of errors that occured with ticker
    """
    # Checks if path given is a valid database path
    try:
        all_tickers = os.listdir(legacy_path)
        pd.read_csv(os.path.join(legacy_path, 'database.csv'))
        all_tickers.remove('database.csv')
    except FileNotFoundError:
        raise FileNotFoundError('Path is not a valid database')

    # For multithreading, mainly for populating zachs info
    def converter(ticker):
        try:
            save_json = Stock(ticker).read_legacy_csv(legacy_path, auto_populate=autopopulate)
            return save_json, ticker
        except Exception as e:
            return e, ticker

    # Remove any unwanted files
    if pop_list is not None:
        for to_pop in pop_list:
            all_tickers.remove(to_pop)

    problem_stocks = []
    with ThreadPoolExecutor() as executor:
        for result in executor.map(converter, all_tickers):
            if not isinstance(result[0], Exception):  # If error occurs ignore the stock
                result[0].to_json(os.path.join(json_path, f'{result[0].ticker}.json'))
            else:
                problem_stocks.append(result)
    return problem_stocks


class Health:
    """Used to evaluate the "health" of a stock object

    Functions:
        nan_checker: checks for the percentage of nan vs real values
        date_coherance: checks if volume, close, open, etc.. have the same dates
        outdated: aka last updated, last_date is last allowed date before criteria triggers
        date_checker: check for any missing dates compared to stock calendar
        missing_attrs: checks for missing attributes

    Attributes:
        stock: stock object to be checked
        report: report of the stock, if empty nothing wrong was found
    """
    MARKET_DICT = {
        '^IXIC': 'NASDAQ',
        '^NYA': 'NYSE',
        '^XAX': 'NYMEX'
    }

    def __init__(self, stock: Stock):
        self.stock = stock
        self.report = {}

    def nan_checker(self, allowed_nan: int):
        """Adds percentage of nan compared to actual values

        :param allowed_nan: percentage (0-1) of allowed percentage of nan
        """
        def nan_pct(df):
            return df.isna().sum().sum() / df.count().sum()

        for attr in self.stock.INTRADAY:
            df = getattr(self.stock, attr)
            if nan_pct(df) > allowed_nan:
                self.report[f'{attr}: nan percent'] = f'{round(nan_pct(df) * 100, 2)}%'

    def date_coherance(self):
        """Checks if all INTRADAY attrs have same date"""

        tail = len(set(getattr(self.stock, self.stock.INTRADAY[-1]).index))
        for attr in self.stock.INTRADAY:
            df = getattr(self.stock, attr)
            if tail != len(set(df.index)):
                self.report['date coherance'] = attr

    def date_checker(self, allowed_missing: int):
        """Checks for any missing dates vs holiday calendar

        :param allowed_missing: allowed percentage of missing dates (0-1)
        """
        calendar = mcal.get_calendar(self.MARKET_DICT[self.stock.market])
        for attr in self.stock.INTRADAY:
            df = getattr(self.stock, attr)
            self.stock_dates = len(set(df.index.to_list()))
            cal_dates = len(set([
                i.date()  # is a timestamp object that needs to be converted into datetime.date
                for i in calendar.schedule(start_date=df.index[0], end_date=df.index[-1]).index
            ]))
            mia_pct = (1-(self.stock_dates / cal_dates))

            # Sometimes stocks will have holidays as well
            # This is not a problem so those stocks are ignored using cal_dates > stock_dates
            if cal_dates > self.stock_dates and mia_pct > allowed_missing:
                self.report[f'{attr}: missing dates percent'] = f'{round(mia_pct*100, 2)}%'

    def outdated(self, last_date: dt.date):
        """Checks if stock is outdated vs the last_date

        :param last_date: the last_date that is considered to be updated
        """
        if self.stock.close.index[-1] < last_date:
            self.report.update({'outdated': True})

    def missing_attrs(self):
        """Checks for any missing/ non populated attributes"""

        missing = []
        for attr in self.stock.__dict__:
            # In order to check if attr/ dataframe is empty, two seprate checks need to happen
            # Because you cannot do .empty on a string
            if getattr(self.stock, attr) is None:
                missing.append(attr)
                continue
            try:
                if getattr(self.stock, attr).empty:
                    missing.append(attr)
            except AttributeError:
                continue
        if missing:
            self.report['missing data'] = missing


class Report:
    """Creates a report using Health class of list of given stock objects

    Parameters:
        stocks: list of stocks to be checked
        max_workers: max workers to be used in multiprocessing
    """

    def __init__(self, stocks: list, max_workers=None):
        self.stocks = stocks
        self.report = {}
        self.criterias = []
        self.max_workers = max_workers

    def add_critiera(self, name: str, *args):
        """Adds a checking criteria from Health class to the report

        Depending on the Health class function
        :param name: will be the name of the Health function i.e. nan_checker
        :param args: will be any additional parameters that the function needs

        Example call:
            add_critiera('nan_checker', 0.1)
        """
        self.criterias.append([name, *args])

    @staticmethod  # Static method for multiprocessing
    def _multiprocess(stock, criteria_list):
        """Used for multiprocessing"""

        print(f'Checking {stock.ticker} on process: {os.getpid()}')
        report = Health(stock)
        try:
            for criteria in criteria_list:
                # Needs to check if any additional args was used
                if len(criteria) == 1:
                    getattr(report, criteria[0])()
                else:
                    getattr(report, criteria[0])(criteria[1])
            return stock.ticker, report.report
        except Exception as e:
            return stock.ticker, {'error occured': e}

    def full_report(self):
        """Generates the report when called"""

        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            results = [
                executor.submit(self._multiprocess, stock, self.criterias)
                for stock in self.stocks
            ]
            for future in as_completed(results):
                ticker, report = future.result()
                # If the returned dict isnt empty, it will be added to report
                # End result will be a report filled with problem stocks only
                if report:
                    self.report[ticker] = report
        return self.report

    def save_report(self, path_or_buff):
        """Saves the report to a path or a buffer"""

        try:
            json.dump(self.report, path_or_buff)
            return
        except AttributeError:
            pass
        try:
            with open(path_or_buff, 'w') as save:
                json.dump(self.report, save, indent=4)
            return
        except PermissionError:
            raise FileNotFoundError('invalid file path')


if __name__ == '__main__':
    import pickle

    main_db = 'E:\\Libraries\\Documents\\Stock Assistant\\database'
    revb_path = 'E:\\Libraries\\Documents\\Stock Assistant\\database\\data'
    with open(os.path.join(main_db, 'cached_stocks.pkl'), 'rb') as read:
        stocks = pickle.load(read)

    print('done')
    stocks.insert(0, Stock('NVDA').read_legacy_csv(revb_path))

    health = Report(stocks, 0.2, dt.date(2020, 1, 10))
    health.full_report()
    print(health.report)
