from library.stock.stock import Stock
from library.datetime_additions import MarketDatetime
from functools import lru_cache
import numpy as np


class Prepare:

    def __init__(self, effective_date, days_forward, days_backwards, end_time=None):
        self.effective_date = effective_date
        self.fdays = days_forward
        self.bdays = days_backwards
        self.end_time = end_time

    @lru_cache()
    def _get_datelist(self, market):
        date = MarketDatetime(self.effective_date, market)
        datelist = sorted(list(set(date.datelist(-self.bdays) + date.datelist(self.fdays))))
        return datelist

    def prepare_intraday(self, stock, normalize=None):
        dates = self._get_datelist(stock.market)
        num_cols = len(stock.close.columns)
        if_nan = np.zeros(num_cols) + np.nan
        stack_attr = []

        for attr in Stock.INTRADAY:
            flat_attr = []

            for date in dates:
                try:
                    flat_attr.extend(getattr(stock, attr).loc[date].to_list())
                except KeyError:
                    flat_attr.extend(if_nan)
            stack_attr.append(flat_attr)
        stack_attr = np.array(stack_attr)

        if self.end_time is not None:
            stack_attr = stack_attr[:, :len(stack_attr[0]) - stock.close.columns.get_loc(self.end_time)]
        if normalize is not None:
            stack_attr = getattr(Prepare, normalize)(stack_attr)
        return stack_attr

    @staticmethod
    def normalize_by_first_value(data: np.array):
        factor = 100 / data[:, 0]
        # Numpy.T wont work because a numpy reverses when transposing
        # Reverse of a 1d array is still a 1d array, so newaxis is called to reshape
        return data * factor[:, np.newaxis]


class PrepareStocks(Prepare):
    pass


class PrepareMarkets(Prepare):
    pass
