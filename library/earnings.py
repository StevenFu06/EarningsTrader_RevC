import requests
import time as py_time
from bs4 import BeautifulSoup


"""
earnings.py for all your earning needs. Includes the fetch.py module which was originally
separated.

Class: 
    EarningsCalendar: Yahoo earnings calendar api
    Earnings: meant to be the main api to interact with all the scraped data
"""


class EarningsCalendar:
    """Earnings date information scraped from yahoo finance

    Args:
        :arg DELAY (int): delay between traversing pages
        :arg CALL_TIME_DICT (dict): translate yahoo lingo into shortened
        :arg date (str): date of interest
        :arg earnings (dict): the final output of the class. A dictionary of all earnings for that date
        :arg url (str): url to yahoo
        :arg offset (int): offset used in the url
        :arg num_stocks (int): number of stocks on current day
        :arg num_pages (int): not useful, mainly used for convenience, number of pages on yahoo
    """
    DELAY = 0.5  # delay in seconds between url calls
    CALL_TIME_DICT = {
        'Time Not Supplied': 'N/A',
        'Before Market Open': 'BMO',
        'After Market Close': 'AMC',
        'TAS': 'N/A'
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
            save.dump(self.earnings, save, indent=indent)