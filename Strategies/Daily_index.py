import tushare as ts
import pandas as pd
import datetime

token = '9585083c3cb3cc48af1ac4401d6be82cf738b23f1eeb31ce284011e5'
ts.set_token(token)
pro = ts.pro_api()

day = 90
index2 = pro.index_classify(level='L2', src='SW')
# index2 = index2[~index2['industry_name'].isin(['动物保健', '园区开发', '石油开采', '水务', '渔业'])]
index2 = index2[index2['industry_name'] == '银行']


def trade_date_list():
    start_date = datetime.date.today() + datetime.timedelta(-500)
    start_date = start_date.strftime("%Y%m%d")
    trade_dates = pro.daily(ts_code='000001.SZ', start_date=start_date, fields='trade_date')
    trade_dates = trade_dates['trade_date']
    return trade_dates


def daily_index(index2, day=0):
    trade_date = trade_date_list()
    stock0 = pro.daily_basic(ts_code='', trade_date=trade_date[day],
                             fields='ts_code, trade_date, close, free_share')

    index_info = pd.DataFrame()
    for i in range(index2.shape[0]):
        member = pro.index_member(index_code=index2['index_code'].values[i],
                                  fields='index_code,con_code')
        member.columns = ['index_code', 'ts_code']
        index_info = index_info.append(member)

    stock1 = pd.merge(stock0, index_info, how='left')
    stock1 = pd.merge(stock1, index2[['index_code', 'industry_name']], how='left')
    stock1 = stock1.dropna(axis=0, how='any')  # 删除含有NaN的行

    result = pd.DataFrame()
    for i in range(index2.shape[0]):
        temp = stock1[stock1['index_code'] == index2['index_code'].values[i]]
        sum_market = sum(temp['close']*temp['free_share'])
        sum_free_share = temp['free_share'].sum()
        temp = temp[['index_code', 'industry_name', 'trade_date']][0:1]
        temp['point'] = sum_market/sum_free_share
        result = result.append(temp)
    return result


index_point = pd.DataFrame()
for j in range(day):
    index_point = index_point.append(daily_index(index2=index2, day=j))

index_point.to_excel(excel_writer="Daily_index.xlsx", sheet_name=index2['industry_name'].values[0], index=False)
