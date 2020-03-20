#  计算指定天数的欧奈尔相对强度
import tushare as ts
import datetime
import pandas as pd
import numpy as np
import pymysql
from sqlalchemy import create_engine


timestemp = 0  # 返回数据的交易日距今天的日期
delta = 20     # 相对于delta个交易日的数据得到relative strength
rows = 20      # 按relative strength排序并保存前rows的数据


token = '9585083c3cb3cc48af1ac4401d6be82cf738b23f1eeb31ce284011e5'
ts.set_token(token)
pro = ts.pro_api()


def trade_date_list():
    start_date = datetime.date.today() + datetime.timedelta(-500)
    start_date = start_date.strftime("%Y%m%d")
    trade_dates = pro.daily(ts_code='000001.SZ', start_date=start_date, fields='trade_date')
    trade_dates = trade_dates['trade_date']
    return trade_dates


def stock_list():
    stocks = pro.stock_basic(exchange='', list_status='L',
                             fields='ts_code,name,market,area,industry,list_date')
    stocks = stocks[~stocks['market'].isin(['科创板'])]  # 排除创业板的企业
    stocks = stocks[~stocks['market'].isin(['创业板'])]  # 排除创业板的企业

    return stocks


def relative_strength(timestemp=0, delta=20, rows=20):
    # ******************************** #
    #     获取当前交易日的股票信息
    stock0 = stock_list()
    trade_dates = trade_date_list()

    stock1 = pro.daily_basic(ts_code='', trade_date=trade_dates[timestemp],
                             fields='ts_code, trade_date, close, total_mv')

    columns1 = ['ts_code', 'trade_date', 'close0', 'total_mv0']
    stock1.columns = columns1
    stock1['total_mv0'] = stock1['total_mv0'] / 10000  # 总市值，单位亿元
    stock = pd.merge(stock0, stock1, how='left')

    # ******************************** #
    #     获取timestemp+delta个交易日前的股票信息
    stock2 = pro.daily_basic(ts_code='', trade_date=trade_dates[timestemp + delta - 1],
                             fields='ts_code, trade_date, close, total_mv')
    columns2 = ['ts_code', 'date1', 'close1', 'total_mv1']
    stock2.columns = columns2
    stock2['total_mv1'] = stock2['total_mv1'] / 10000  # 总市值，单位亿元

    stock = pd.merge(stock, stock2, how='left')
    stock = stock.dropna(axis=0, how='any')  # 删除含有NaN的行

    # ******************************** #
    #     计算欧奈尔相对价格强度
    stock['growth'] = stock['close0'] / stock['close1'] - 1
    stock = stock.drop(stock['growth'].idxmin())  # 去掉最小值

    stock['relative_strength'] = 100 * (stock['growth'] - min(stock['growth'])) / (
                max(stock['growth']) - min(stock['growth']))
    stock.sort_values(by="growth", ascending=False, inplace=True)

    stock = stock[0:rows]
    a = np.arange(stock.shape[0])
    a[0:len(a)] = delta
    stock['delta'] = a

    # ******************************** #
    #     调整stock列的顺序之后返回stock信息
    order = ['delta', 'name', 'growth', 'relative_strength', 'close0',
             'close1', 'trade_date', 'date1', 'total_mv0', 'total_mv1', 'ts_code']
    return stock[order]


def stock_info(ts_code, timestemp=0):
    stock_info1 = pro.daily_basic(ts_code=ts_code, trade_date=trade_date_list()[timestemp],
                                  fields='ts_code, trade_date, turnover_rate_f,'
                                         'volume_ratio, pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_share,'
                                         'float_share,circ_mv')
    stock_info2 = pro.stock_basic(exchange='', list_status='L',
                                  fields='ts_code,area,industry,list_date')
    stock_info3 = pro.daily(ts_code=ts_code, trade_date=trade_date_list()[timestemp],
                            fields='ts_code,vol,amount')

    stock = pd.merge(stock_info1, stock_info2, how='left')
    stock = pd.merge(stock, stock_info3, how='left')

    stock['total_share'] = stock['total_share'] / 10000  # 亿股
    stock['float_share'] = stock['float_share'] / 10000  # 亿股
    stock['circ_mv'] = stock['circ_mv'] / 10000  # 亿元
    stock['vol'] = stock['vol'] / 10000  # 万手
    stock['amount'] = stock['amount'] / 100000  # 亿元

    columns = ['ts_code', 'trade_date', '换手率', '量比', '市盈率',
               'TTM市盈率', '市净率', '市销率', 'TTM市销率', '股息率',
               'TTM股息率', '总股本/亿', '流通股本/亿', '流通市值/亿',
               '区域', '行业', '上市时间', '成交量/万手', '成交额/亿']
    stock.columns = columns
    order = ['ts_code', 'trade_date', '市盈率', 'TTM市盈率', '市净率',
             '市销率', 'TTM市销率', '股息率', 'TTM股息率', '量比',
             '换手率', '总股本/亿', '流通股本/亿', '流通市值/亿', '成交量/万手',
             '成交额/亿', '区域', '行业', '上市时间']
    return stock[order]


def create_table():
    # ******************************** #
    #     创建表
    conn = pymysql.connect('localhost', 'root', 'password', 'stock')
    cursor = conn.cursor()
    sql_create = """
    CREATE TABLE IF NOT EXISTS `stock`.`relative_strength` (
    `delta` FLOAT NOT NULL,
    `name` VARCHAR(45),
    `growth` FLOAT,
    `relative_strength` FLOAT,
    `close0` FLOAT,
    `close1` FLOAT,
    `trade_date` VARCHAR(45) NOT NULL,
    `date1` VARCHAR(45),
    `total_mv0` FLOAT,
    `total_mv1` FLOAT,
    `ts_code` VARCHAR(45) NOT NULL COMMENT 'stock code',
    `市盈率` FLOAT,
    `TTM市盈率` FLOAT,
    `市净率` FLOAT,
    `市销率` FLOAT,
    `TTM市销率` FLOAT,
    `股息率` FLOAT,
    `TTM股息率` FLOAT,
    `量比` FLOAT,
    `换手率` FLOAT,
    `总股本/亿` FLOAT,
    `流通股本/亿` FLOAT,
    `流通市值/亿` FLOAT,
    `成交量/万手` FLOAT,
    `成交额/亿` FLOAT,
    `区域` VARCHAR(45),
    `行业` VARCHAR(45),
    `上市时间` VARCHAR(45),
    PRIMARY KEY (`delta`, `trade_date`, `ts_code`));"""

    cursor.execute(sql_create)

    conn.commit()
    cursor.close()
    conn.close()


def write_data(data):
    engine = create_engine("mysql+pymysql://root:password@localhost/stock")
    pd.io.sql.to_sql(data, 'relative_strength', engine, if_exists='append',
                     chunksize=10000, index=False)


strength = relative_strength(timestemp=timestemp, delta=delta, rows=rows)

info = pd.DataFrame()
for j in range(rows):
    info = info.append(stock_info(ts_code=strength['ts_code'].values[j]))

data = pd.merge(strength, info, how='left')

create_table()
write_data(data=data)