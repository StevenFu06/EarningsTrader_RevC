from library.stock import fetch
import datetime as dt
import pandas as pd
import warnings
import json
from bson import ObjectId
import os

"""Main stock module 
Interacts with erryting basically, a life saver, one stop shop for all stock data
"""


class Stock:
    """stock api with shit loads of data

    Note:
        All df index and columns will be in datetime format if applicable.

        BIG Note. Using iloc with slicing is inclusive of start but not end for some reason.
        Using loc with slicing is fully inclusive

        When stock is mentioned it means stock obj when ticker is mentioned it means the str

    Read:
        All read functions (exceptions for legacy) will load all attributes with interval_of_data.
        Interval_of_data is not stored but is calculated on read call. All ready functions automatically parse
        columns and index to correct datetime format (cols: dt.time, index: dt.date()). All read functions
        will restore to both dataframes and series.

    Attributes:
        INTRADAY (list): Attributes for wt.intraday attributes. Data type of dataframe
        INFO (list): includes misc data for stocks/ single point entry that doesnt change based on date
        HISTORICAL (list): any attributes that change based on date that needs to be tracked
        MARKET_DICT (dict): convert market names to market tickers for consistency

    Parameter:
        :param ticker (str): ticker of interest
        :param interval_of_data (int): if data is loaded, returns the interval of intraday columns headers
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
        'NYSEAMERICAN': '^NYA',
        'NYSEARCA': '^NYA'
    }

    def __init__(self, ticker: str):
        self.ticker = ticker
        self.interval_of_data = None
        for info in self.INFO:
            setattr(self, info, None)
        for hist in self.HISTORICAL:
            setattr(self, hist, pd.Series(name=hist))
        for intra in self.INTRADAY:
            setattr(self, intra, pd.DataFrame())

    def fetch(self, api_key: str, interval_of_data: int = None, range_of_data=30, zachs=True, world_trade=True):
        """Fetch from api (Wt and zachs)

        If nothing prior loaded, it will populate all attributes, will update attributes if
        already loaded

        :param api_key: api key for world trade
        :param interval_of_data: how often the data is. Available every 1, 5, 15, or 60
        :param range_of_data: Default is 30 days, but 30 days is not available for 1 minute (7 days)
        :param zachs: if false will ignore fetch from zachs
        :param world_trade: if false will ignore fetch from world trade data
        """
        # Error and warning for improper interval_of_data
        if self.interval_of_data is not None:  # Test if nothing is loaded
            if interval_of_data is None:  # Default value
                interval_of_data = self.interval_of_data

            # If data is being updated, then check if intervals match
            elif interval_of_data is not None and interval_of_data != self.interval_of_data:
                warnings.formatwarning = lambda msg, *args: f'{msg}\n'
                warnings.warn('Given interval does not match loaded interval!')

        # Raise error if its a new stock and no interval is given
        elif interval_of_data is None:
            raise TypeError('"interval_of_data" must be specified if nothing is loaded')

        # Call fetch functions and load in correct parameters
        if world_trade:
            self._fetch_from_wt(api_key, interval_of_data, range_of_data)
        if zachs:
            self._fetch_from_zachs()
        return self

    def get_data_interval(self):
        """returns the time interval in minutes between column headers"""

        interval = (dt.datetime.combine(dt.date.today(), self.close.columns[1]) -
                    dt.datetime.combine(dt.date.today(), self.close.columns[0]))
        self.interval_of_data = int(interval.seconds/60)

    def _fetch_from_wt(self, api_key: str, interval_of_data: int, range_of_data=30):
        """Fetches data from world trade data, using fetch.py api"""

        wt = fetch.Intraday()
        wt.dl_intraday(self.ticker, api_key, interval_of_data, range_of_data)
        self._load_from_wt(wt)

    def _load_from_wt(self, worldtrade):
        """Converts WorlTrade.Intraday obj into stock (self) obj"""

        worldtrade.to_dataframe()  # wt.df_dict has dataframe already converted to datetime
        self.market = self.MARKET_DICT[worldtrade.raw_intra_data['stock_exchange_short']]
        for i in self.INTRADAY:
            # built in drop duplicates not working properly
            temp_df = pd.concat([getattr(self, i), worldtrade.df_dict[i]], axis=0)
            setattr(
                self, i,
                temp_df.loc[~temp_df.index.duplicated(keep='first')]
            )

    def _fetch_from_zachs(self):
        """Fetches data from zachs using fetch.ZachsApi

        mainly used for multithreading loading
        """
        zachs = fetch.ZachsApi(self.ticker)
        self._load_from_zachs(zachs)

    def _load_from_zachs(self, zachs):
        """Will set HISTORICAL parameters, sector and industry

        :param zachs: fetch.ZachsApi object
        """
        for i in self.HISTORICAL:
            # Note index is datetime format
            df_zachs = pd.Series(
                data=zachs.stock_activity[i],
                index=[dt.datetime.now().date()],
                name=i
            )
            # built in drop duplicates not working properly
            temp_df = pd.concat([getattr(self, i), df_zachs])
            setattr(
                self, i,
                temp_df.loc[~temp_df.index.duplicated(keep='first')]
            )
        self.sector = zachs.sector
        self.industry = zachs.industry

    def to_json(self, path_or_buff=None):
        """Instance to json

        Will return a dict with pandas object parsed into json with orient='split' if path or buff not given.
        If path_or_buff given, the dictionary will be saved automatically

        :param path_or_buff: will save to path or buffer if supplied.
        """
        df_attrs = self.HISTORICAL + self.INTRADAY
        stock_as_json = {key: getattr(self, key) for key in ['ticker'] + self.INFO}
        stock_as_json.update({
            key: getattr(self, key).to_json(
                orient='split',
                date_format='iso',
                date_unit='s',
            )
            for key in df_attrs
        })

        if path_or_buff is not None:
            try:
                json.dump(stock_as_json, path_or_buff)
                return
            except AttributeError:
                pass
            try:
                with open(path_or_buff, 'w') as save:
                    json.dump(stock_as_json, save)
                return
            except PermissionError:
                raise FileNotFoundError('invalid file path')
        return stock_as_json

    def read_json(self, path_or_buff: json):
        """Load a stock to_json serialized json file/ dictionary

        :param path_or_buff: takes dict*, json str, buffer, or path
        """
        try:  # Parse path or buff into usable dictionary
            serial_json = json.load(path_or_buff)
        except AttributeError:
            try:
                with open(path_or_buff, 'r') as read:
                    serial_json = json.load(read)
            except TypeError:
                try:
                    serial_json = json.loads(path_or_buff)
                except TypeError:
                    serial_json = path_or_buff

        for i in self.INTRADAY:
            temp_df = pd.read_json(
                serial_json[i],
                orient='split',
                convert_dates=False  # Prevents data from being converted into datetime
            )
            # convert to proper datetime format
            temp_df.columns = [col.time() for col in temp_df.columns]
            temp_df.index = [idx.date() for idx in temp_df.index]
            setattr(self, i, temp_df)
        self.get_data_interval()

        for i in self.HISTORICAL:
            temp_series = pd.read_json(
                serial_json[i],
                orient='split',
                typ='series',
                convert_dates=False  # Prevents data from being converted into datetime
            )
            # convert to proper datetime format
            temp_series.index = [idx.date() for idx in temp_series.index]
            setattr(self, i, temp_series)

        for i in self.INFO:
            setattr(self, i, serial_json[i])
        return self

    def to_legacy_csv(self, path: str):
        """Save to legacy csv, aka rev B data

        Creates a new folder/ appends to existing folder with folder name self.ticker at the specified path.
        Will also update the database.csv file

        :param path: path to main database folder. Think rev_B data folder.
        """
        # Create folder with self.ticker name if doesnt exist
        if not os.path.exists(f'{path}\\{self.ticker}'):
            os.makedirs(f'{path}\\{self.ticker}')
        for attr in self.INTRADAY:
            getattr(self, attr).to_csv(f'{path}\\{self.ticker}\\{attr}.csv')

        # Inside legacy path has a database.csv file which holds metadata
        metadata = pd.read_csv(f'{path}\\database.csv', index_col=0)
        metadata.loc[self.ticker, 'last_date'] = self.close.index[-1]
        metadata.loc[self.ticker, 'first_date'] = self.close.index[0]
        metadata.loc[self.ticker, 'market'] = self.market
        metadata.to_csv(f'{path}\\database.csv')

    def read_legacy_csv(self, path: str, auto_populate: bool = False):
        """Reads legacy database (rev B style database)

        :param path: path to database folder which contains the database.csv file
        :param auto_populate: because legacy, the database does not contain all attrs. when =True, will auto
        fetch remainder of attrs from zachs.
        """
        for attr in self.INTRADAY:
            temp_df = pd.read_csv(
                f'{path}\\{self.ticker}\\{attr}.csv',
                index_col=0,
                parse_dates=True
            )
            temp_df.columns = [dt.datetime.strptime(col, '%H:%M:%S').time() for col in temp_df.columns]
            temp_df.index = [idx.date() for idx in temp_df.index]  # ensure datetime properly parsed
            setattr(self, attr, temp_df)
        self.get_data_interval()
        # Because I was stupid and didn't save as tickers, conversion needs to be done
        self.market = self.MARKET_DICT[
            pd.read_csv(
                filepath_or_buffer=f'{path}\\database.csv',
                index_col=0
            ).loc[self.ticker, 'market']
        ]
        if auto_populate:
            self._fetch_from_zachs()
        return self

    def to_mongo(self, collection, object_id: str = None):
        """Saves to mongo collection specified

        Duplicates will be handled and correct errors will be raised

        Parameters:
            :param collection: pymongo database collection object
            :param object_id: string ID, will be parsed into ObjectID automatically
        """
        # Error handeling
        num_results = len(list(collection.find({'ticker': self.ticker})))
        if object_id is None and num_results > 1:
            raise FileExistsError('Repeat ticker found, ObjectID required')
        # serialize object
        elif object_id is not None:
            collection.replace_one({'_id': ObjectId(object_id)}, self.to_json(), upsert=True)
        else:
            collection.replace_one({'ticker': self.ticker}, self.to_json(), upsert=True)

    def read_mongo(self, collection, object_id: str = None):
        """Read from mongo collection

        Duplicates will be handled and correct errors will be raised

        Parameters:
            :param collection: pymongo database collection object
            :param object_id: string ID, will be parsed into ObjectID automatically
        """
        # Error handeling
        num_results = len(list(collection.find({'ticker': self.ticker})))
        if num_results == 0:
            raise FileNotFoundError('Stock ticker not found, make sure ticker is all cap')
        if object_id is None and num_results > 1:
            raise FileExistsError('Repeat ticker found, ObjectID required')
        # Load object
        elif object_id is not None:
            json_data = collection.find_one({'_id': ObjectId(object_id)})
        else:
            json_data = collection.find_one({'ticker': self.ticker})
        json_data.pop('_id')  # Because mongo has an extra _id tag
        self.read_json(json_data)
        return self


if __name__ == '__main__':
    nvda = Stock('NVDA')
