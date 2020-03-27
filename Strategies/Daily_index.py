import tushare as ts
import pandas as pd
import datetime

token = '9585083c3cb3cc48af1ac4401d6be82cf738b23f1eeb31ce284011e5'
ts.set_token(token)
pro = ts.pro_api()

days = 30


def trade_date_list():
    start_date = datetime.date.today() + datetime.timedelta(-500)
    start_date = start_date.strftime("%Y%m%d")
    trade_dates = pro.daily(ts_code='000001.SZ', start_date=start_date, fields='trade_date')
    trade_dates = trade_dates['trade_date']
    return trade_dates


trade_date = trade_date_list()
stock = pro.daily_basic(ts_code='', begin_date=trade_date[days],
                        fields='ts_code, trade_date, close, free_share')
