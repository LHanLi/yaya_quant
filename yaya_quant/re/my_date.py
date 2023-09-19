import pandas as pd
import datetime


# tradedays 所有交易日  list(market.index.levels[0].unique())
def tradeday(date, tradedays):
    while date not in tradedays:
        date = date + datetime.timedelta(1)
    return date


# 从start（‘2023-1-1’）开始，向后n个，间隔n_month月
def daterange(start, n, n_month=1):
    datelist = []
    date = pd.to_datetime(start)
    for i in range(n):
        datelist.append(date)
        year = date.year
        # 一年12个月
        month = date.month + n_month
        if month > 12:
            year += 1
            month = month - 12
        day = date.day
        date = pd.to_datetime('%s-%s-%s'%(year, month, day))
    return datelist








