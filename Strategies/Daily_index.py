import tushare as ts
import pandas as pd
import datetime
import time
import pymysql
from sqlalchemy import create_engine

token = '9585083c3cb3cc48af1ac4401d6be82cf738b23f1eeb31ce284011e5'
ts.set_token(token)
pro = ts.pro_api()

day = 90
write = 1
index2 = pro.index_classify(level='L2', src='SW')
industry_list = ['黄金', '半导体', '银行', '保险']
index2 = index2[index2['industry_name'].isin(industry_list)]


def trade_date_list():
    start_date = datetime.date.today() + datetime.timedelta(-500)
    start_date = start_date.strftime("%Y%m%d")
    trade_dates = pro.daily(ts_code='000001.SZ', start_date=start_date, fields='trade_date')
    trade_dates = trade_dates['trade_date']
    return trade_dates


def index_info(index2):
    result = pd.DataFrame()
    for i in range(index2.shape[0]):
        temp = pro.index_member(index_code=index2['index_code'].values[i],
                                fields='index_code,con_code')
        temp.columns = ['index_code', 'ts_code']
        result = result.append(temp)
    return pd.merge(result, index2, how='left')


def daily_stocks(day, member, trade_date):
    result = pd.DataFrame()
    for i in range(member.shape[0]):
        temp = pro.daily_basic(ts_code=member['ts_code'].values[i],
                               start_date=trade_date[day],
                               fields='ts_code, trade_date, close, free_share')
        result = result.append(temp)
    return pd.merge(result, member, how='left')


def point_calculate(df):
    df['market'] = df['close'] * df['free_share']
    result = pd.DataFrame()
    index_code = df.drop_duplicates('index_code')
    dates = df.drop_duplicates('trade_date')
    for i in range(index_code.shape[0]):
        for j in range(dates.shape[0]):
            temp = df[df['index_code'] == index_code['index_code'].values[i]]
            temp = temp[temp['trade_date'] == dates['trade_date'].values[j]]
            sum_market = temp['market'].sum()
            sum_free_share = temp['free_share'].sum()
            temp = temp[['index_code', 'industry_name', 'trade_date']][0:1]
            temp['point'] = sum_market/sum_free_share
            result = result.append(temp)
    return result


def create_table():
    # ******************************** #
    #     创建表
    conn = pymysql.connect('localhost', 'root', 'password', 'stock')
    cursor = conn.cursor()
    sql_create = """
    CREATE TABLE IF NOT EXISTS `stock`.`index_point` (
    `index_code` VARCHAR(45) NOT NULL, 
    `industry_name` VARCHAR(45),
     `trade_date` VARCHAR(45)  NOT NULL,
    `point` FLOAT,    
     PRIMARY KEY (`index_code`, `trade_date`));"""
    cursor.execute(sql_create)
    conn.commit()
    cursor.close()
    conn.close()


def write_data(data):
    engine = create_engine("mysql+pymysql://root:password@localhost/stock")
    pd.io.sql.to_sql(data, 'index_point', engine, if_exists='append',
                     chunksize=10000, index=False)


# here starts the main loop
trade_dates = trade_date_list()
member = index_info(index2=index2)
df = daily_stocks(day=day, member=member, trade_date=trade_dates)
data = point_calculate(df=df)

if write != 0:
    create_table()
    write_data(data=data)

