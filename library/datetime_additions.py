import datetime as dt
import pandas_market_calendars as mcal


def is_weekday(date):
    """Checks if is weekday"""

    return True if date.weekday() == 6 or date.weekday() == 5 else False


def add_days(date, num_days):
    """Adds number of days disregarding weekdays/ holidays"""

    return date + dt.timedelta(days=num_days)


class MarketDatetime:
    """Datetime addition for marketdates only

    Attributes:
        MARKET_DICT: to convert from stock market stored to pandas_market_calendars
        date: the date... duh
        market: the market of interest
        Buffer: Buffer is here to ensure that no matter how many days adding/ subtracting, it will account
                for the days that are not open. Default is 1.3*num_days
    """
    MARKET_DICT = {
        '^IXIC': 'NASDAQ',
        '^NYA': 'NYSE',
        '^XAX': 'NYMEX'
    }
    BUFFER = 1.5

    def __init__(self, date, market):
        self.date = date
        self.market = self.MARKET_DICT[market]
        self.calendar = mcal.get_calendar(self.market)

        if not self.is_open(date):
            raise RuntimeError(f'Market not open on {self.date}')

    def is_open(self, date):
        """Checks if market is open that day"""

        return not self.calendar.valid_days(date, date).empty

    def datelist(self, num_days):
        """Creates a datelist INCLUSIVE of the given date, and open dates only

        Note:
            - Inclusive of the first date
            - Will not modify own date, will create a new date
            - Sorted of course
            - Buffer is used to account for all holidays/ weekdays default set at 1.5

        :param num_days: number of days in the new datelist
        """
        if num_days >= 0:
            start, end = self.date, add_days(self.date, int(num_days*self.BUFFER))
            return [date.date()
                    for date in self.calendar.valid_days(start, end)[:num_days]]
        else:
            start, end = add_days(self.date, int(num_days*self.BUFFER)), self.date
            return [date.date()
                    for date in self.calendar.valid_days(start, end)[num_days:]]

    def add_days(self, num_days):
        """Add certain amounts number of days to self.date attribute

        :param num_days: number of days to add
        """
        increment, count, date = 1 if num_days >= 0 else -1, 0, self.date
        while abs(count) < abs(num_days):
            if self.is_open(self.date):
                count += 1
            self.date = add_days(self.date, increment)
        return self
