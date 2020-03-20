# 输入一个股票名称，获取其所在行业，及所在行业所有的股票名称

import tushare as ts
import pandas as pd
import datetime
import pymysql
from sqlalchemy import create_engine
import numpy as np

token = '9585083c3cb3cc48af1ac4401d6be82cf738b23f1eeb31ce284011e5'
ts.set_token(token)
pro = ts.pro_api()

timestemp = 0  # 返回数据的交易日距最近交易日的日期
# write = 1
write = 0
delta = [20, 120, 250]     # 相对于delta个交易日的数据得到relative strength
industry_list = ['黄金',         '半导体']
index = 2
industry2 = industry_list[index-1]
index2 = pro.index_classify(level='L2', src='SW')


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


def relative_strength(delta, timestemp=0):
    # ******************************** #
    #     获取当前交易日的股票信息
    stock = stock_list()
    trade_dates = trade_date_list()
    delta1 = delta[0]
    delta2 = delta[1]
    delta3 = delta[2]

    stock0 = pro.daily_basic(ts_code='', trade_date=trade_dates[timestemp],
                             fields='ts_code, trade_date, close, total_mv')
    columns0 = ['ts_code', 'trade_date', 'close0', 'total_mv0']
    stock0.columns = columns0
    stock0['total_mv0'] = stock0['total_mv0'] / 10000  # 总市值，单位亿元

    stock1 = pro.daily_basic(ts_code='', trade_date=trade_dates[timestemp + delta1 - 1],
                             fields='ts_code, trade_date, close, total_mv')
    columns1 = ['ts_code', 'date1', 'close1', 'total_mv1']
    stock1.columns = columns1
    stock1['total_mv1'] = stock1['total_mv1'] / 10000  # 总市值，单位亿元

    stock2 = pro.daily_basic(ts_code='', trade_date=trade_dates[timestemp + delta2 - 1],
                             fields='ts_code, trade_date, close, total_mv')
    columns2 = ['ts_code', 'date2', 'close2', 'total_mv2']
    stock2.columns = columns2
    stock2['total_mv2'] = stock2['total_mv2'] / 10000  # 总市值，单位亿元

    stock3 = pro.daily_basic(ts_code='', trade_date=trade_dates[timestemp + delta3 - 1],
                             fields='ts_code, trade_date, close, total_mv')
    columns3 = ['ts_code', 'date3', 'close3', 'total_mv3']
    stock3.columns = columns3
    stock3['total_mv3'] = stock3['total_mv3'] / 10000  # 总市值，单位亿元

    stock = pd.merge(stock, stock0, how='left')
    stock = pd.merge(stock, stock1, how='left')
    stock = pd.merge(stock, stock2, how='left')
    stock = pd.merge(stock, stock3, how='left')
    stock = stock.dropna(axis=0, how='any')  # 删除含有NaN的行

    stock['growth%s' % delta1] = stock['close0'] / stock['close1'] - 1
    stock['strength%s' % delta1] = 100 * (stock['growth%s' % delta1] - min(stock['growth%s' % delta1])) / (
                max(stock['growth%s' % delta1]) - min(stock['growth%s' % delta1]))

    stock['growth%s' % delta2] = stock['close0'] / stock['close2'] - 1
    stock['strength%s' % delta2] = 100 * (stock['growth%s' % delta2] - min(stock['growth%s' % delta2])) / (
            max(stock['growth%s' % delta2]) - min(stock['growth%s' % delta2]))

    stock['growth%s' % delta3] = stock['close0'] / stock['close3'] - 1
    stock['strength%s' % delta3] = 100 * (stock['growth%s' % delta3] - min(stock['growth%s' % delta3])) / (
            max(stock['growth%s' % delta3]) - min(stock['growth%s' % delta3]))

    stock.sort_values(by='strength%s' % delta3, ascending=False, inplace=True)

    order = ['ts_code', 'growth%s' % delta1, 'strength%s' % delta1, 'growth%s' % delta2,
             'strength%s' % delta2, 'growth%s' % delta3, 'strength%s' % delta3]
    return stock[order]


def stock_info(ts_code, timestemp=0):

    stock_info1 = pro.daily(ts_code=ts_code, trade_date=trade_date_list()[timestemp],
                            fields='ts_code,open,high,low,close,pre_close,vol,amount')
    stock_info1['change(%)'] = stock_info1['close']/stock_info1['pre_close']*100-100

    stock_info2 = pro.stock_basic(exchange='', list_status='L',
                                  fields='ts_code,name,area,market,list_date')
    stock_info3 = pro.daily_basic(ts_code=ts_code, trade_date=trade_date_list()[timestemp],
                                  fields='ts_code, trade_date, turnover_rate_f,'
                                         'volume_ratio, pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_share,'
                                         'float_share,total_mv,circ_mv')

    stock = pd.merge(stock_info1, stock_info2, how='left')
    stock = pd.merge(stock, stock_info3, how='left')

    stock['vol'] = stock['vol'] / 10000  # 万手
    stock['amount'] = stock['amount'] / 100000  # 万元
    stock['total_share'] = stock['total_share'] / 10000  # 亿股
    stock['float_share'] = stock['float_share'] / 10000  # 亿股
    stock['total_mv'] = stock['total_mv'] / 10000  # 亿元
    stock['circ_mv'] = stock['circ_mv'] / 10000  # 亿元

    columns = ['ts_code', 'open', 'high', 'low', 'close', 'pre_close',
               '成交量/万手', '成交额/亿', '涨跌幅','name', 'area', 'market', 'list_date',
               'trade_date', '换手率', '量比', 'pe',
               'pe_ttm', 'pb', 'ps', 'ps_ttm', '股息率',
               '股息率_ttm', 'total_share', 'float_share', 'total_mv', 'circ_mv'
               ]
    stock.columns = columns
    order = ['ts_code', 'name', 'area', 'total_mv', 'trade_date',
             '涨跌幅', 'close', 'pre_close', 'open', 'high', 'low', 'pe',
             'pe_ttm', 'pb', 'ps', 'ps_ttm', '股息率',
             '股息率_ttm', '成交量/万手', '成交额/亿', 'circ_mv',
             '换手率', '量比', 'total_share', 'float_share', 'market', 'list_date'
             ]

    return stock[order]


def index_member(industry2, timestemp=0):
    index3 = pro.index_classify(level='L2', src='SW')
    index_code = index3[index3['industry_name'] == industry2]['index_code'].values[0]
    index3_member = pro.index_member(index_code=index_code, fields='index_code,con_code')
    index3_member.columns = ['index_code', 'ts_code']

    name = []
    for i in range(index3_member.shape[0]):
        name.append(industry2)
    col_name = index3_member.columns.tolist()
    col_name.insert(1, 'industry2')
    index3_member = index3_member.reindex(columns=col_name)
    index3_member['industry2'] = name

    info = pd.DataFrame()
    for m in range(index3_member.shape[0]):
        ts_code = index3_member['ts_code'].values[m]
        temp = stock_info(ts_code=ts_code, timestemp=timestemp)
        info = info.append(temp)

    member = pd.merge(index3_member, info, how='left')
    member= member.dropna(subset=["trade_date"])
    member = member[~member['market'].isin(['创业板', '科创板'])]
    return member


def create_table(member, delta):
    # ******************************** #
    #     创建表
    conn = pymysql.connect('localhost', 'root', 'password', 'stock')
    cursor = conn.cursor()
    sql_create = """
    CREATE TABLE IF NOT EXISTS `stock`.`%s` (
    `index_code` VARCHAR(45) NOT NULL, `industry2` VARCHAR(45),
    `ts_code` VARCHAR(45) NOT NULL, `name` VARCHAR(45), `market` VARCHAR(45), `area` VARCHAR(45), 
    `total_mv` FLOAT, `circ_mv` FLOAT,`成交量/万手` FLOAT, 
    `成交额/亿` FLOAT, `trade_date` VARCHAR(45) NOT NULL, `涨跌幅` FLOAT,
    `growth%s` FLOAT, `strength%s` FLOAT,
    `growth%s` FLOAT, `strength%s` FLOAT,
    `growth%s` FLOAT, `strength%s` FLOAT,
    `close` FLOAT, `pre_close` FLOAT, `open` FLOAT,
     `high` FLOAT, `low` FLOAT, `pe` FLOAT,
     `pe_ttm` FLOAT, `pb` FLOAT, `ps` FLOAT,
     `ps_ttm` FLOAT, `股息率` FLOAT, `股息率_ttm` FLOAT, 
    `换手率` FLOAT, `量比` FLOAT, `total_share` FLOAT,
    `float_share` FLOAT, `list_date` VARCHAR(45),    
     PRIMARY KEY (`index_code`, `ts_code`, `trade_date`));""" % (member['industry2'].values[0],
                                                                 delta[0], delta[0], delta[1],
                                                                 delta[1], delta[2], delta[2])

    cursor.execute(sql_create)

    conn.commit()
    cursor.close()
    conn.close()


def write_data(member):
    engine = create_engine("mysql+pymysql://root:password@localhost/stock")
    pd.io.sql.to_sql(member, '%s' % (member['industry2'].values[0]), engine, if_exists='append',
                     chunksize=10000, index=False)


member = index_member(industry2=industry2, timestemp=timestemp)
strength = relative_strength(delta=delta, timestemp=timestemp)
stock = pd.merge(member, strength, how='left')
if write != 0:
    create_table(member=stock, delta=delta)
    write_data(member=stock)
