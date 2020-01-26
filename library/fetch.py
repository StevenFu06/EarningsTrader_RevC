import requests


"""
fetch.py is a module for getting/ connecting to internet and grabbing data.
Any and all programs/methods that involve connecting to the internet/ another 3rd party
provider will be done through fetch.py.
"""


class WorldTrade:
    """
    WorldTrade is meant to deal with all things involving World Trading Data's api. Meant to be a header/ title to make
    grouping of various World Trade downloading easier.
    """
    def __init__(self):
        pass

    class Intraday:
        def __init__(self, api_key):
            self.raw_intra_data = None
            self.api_key = api_key

        def dl_intraday(self, ticker, interval_of_data, range_of_data):
            intra_url = f'https://intraday.worldtradingdata.com/api/v1/intraday?symbol={ticker}\
            &range={str(range_of_data)}&interval={str(interval_of_data)}&api_token={self.api_key}'
            print(intra_url)
            data = requests.get(intra_url)
            if data.status_code != 200:
                raise ConnectionError(f'code {data.status_code}')
            self.raw_intra_data = data.json()
            return self.raw_intra_data

        def to_dataframe(self):


WorldTrade.Intraday("apikey").dl_intraday('^IXIC', 123, 123).to_dataframe()