import requests
import time as py_time
from bs4 import BeautifulSoup
import datetime as dt
import pandas as pd
import json

"""
earnings.py for all your earning needs. Includes the fetch.py module which was originally
separated.

Class: 
    Earnings: meant to be the main api to interact with all the scraped data
    YahooEarningsCalendar: Yahoo earnings calendar api. Returns ticker+time only... yahoo sucks
    ZachsEarningsCalendar: Earnings calendar from zachs... Thank god for zachs
"""


class Earnings:

    def __init__(self, path):
        self.path = path
        with open(path, 'r') as read:
            self.data = json.load(read)

    def update(self):
        cal = ZachsEarningsCalendar(dt.datetime.now().date())
        cal.get_earnings()
        print(cal.earnings)


class YahooEarningsCalendar:
    """Earnings date information scraped from yahoo finance

    Attributes:
        DELAY (int): delay between traversing pages
        CALL_TIME_DICT (dict): translate yahoo lingo into shortened
        date (str): date of interest
        earnings (dict): the final output of the class. A dictionary of all earnings for that date
        url (str): url to yahoo
        offset (int): offset used in the url
        num_stocks (int): number of stocks on current day
        num_pages (int): not useful, mainly used for convenience, number of pages on yahoo
    """
    DELAY = 0.5  # delay in seconds between url calls
    CALL_TIME_DICT = {
        'Time Not Supplied': 'N/A',
        'Before Market Open': 'BMO',
        'After Market Close': 'AMC',
        'TAS': 'AMC'
    }

    def __init__(self, date: object):
        self.date = date.strftime('%Y-%m-%d')
        self.earnings = {'date': self.date, 'tickers': {}}
        self.url = None
        self._soup = None
        self.offset = 0
        self.num_stocks = 0
        self.num_pages = 0
        self.run()

    def getsoup(self):
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
            json.dump(self.earnings, save, indent=indent)


class ZachsEarningsCalendar:
    """Zachs Earnings Calendar

    Attributes:
        raw_data: raw data from zachs as a dict
        earnings: earnings data from zachs as a dataframe
        timestamp: timespamp of the input dated
        date: current date
        url: url to the data
    """
    HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                             'Chrome/79.0.3945.130 Safari/537.36'}

    def __init__(self, date: dt.date):
        self.raw_data = None
        self.date = date
        self.earnings = None
        # dt.time(1,0) is the time zachs wants for the url. NOTE **1AM IS EST TIME**
        self.timestamp = int(dt.datetime.combine(date, dt.time(1, 0)).timestamp())
        self.url = 'https://www.zacks.com/includes/classes/z2_class_calendarfunctions_data.php?calltype=' \
                   f'eventscal&date={self.timestamp}&type=1'
        self.get_earnings()

    def get_earnings(self):
        """Gets and returns the data from zachs earnings calendar"""

        def parse_eps(text):  # EPS may not exist so cant always be a float
            try:
                return float(text)
            except ValueError:
                return text

        # Get raw data as dict
        data = requests.get(self.url, headers=self.HEADERS)
        if data.status_code != 200:
            raise ConnectionError(f'code {data.status_code}')
        soup = BeautifulSoup(data.content, 'html.parser')
        self.raw_data = json.loads(soup.text)['data']

        # To dataframe
        to_frame = {i[0]: {
            'time': i[3],
            'estimate': parse_eps(i[4]),
            'reported': parse_eps(i[5])
        } for i in self.raw_data}
        self.earnings = pd.DataFrame.from_dict(to_frame, orient='index')


if __name__ == '__main__':
    zachs = ZachsEarningsCalendar(dt.date(2020, 2, 4))
    zachs.get_earnings()
    print(zachs.earnings)
