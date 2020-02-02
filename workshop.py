import os
import datetime as dt
from library.stock.stock import Stock
from library.earnings import  EarningsCalendar
from concurrent.futures import ThreadPoolExecutor

earnings = EarningsCalendar(dt.date(2020, 1, 29))
print(earnings.earnings)
print(earnings.num_stocks)