import pandas as pd
import backtrader as bt
import datetime


# df_price  date, thscode, open, high, close, low, vol, ...
# index: datetime(e.g. pd.to_datetime(20140121))  
# 默认数据： datetime, open, high, low, close, vol, oi  自动查询列字段添加
# 个性数据使用列表添加
def df_Data(df_price, extra=None, start='adpative', end='adpative'):
    if(start == 'adpative'):
        start = df_price.date.min()
    if(end=='adpative'):
        end = df_price.date.max()
    # 获取默认数据的列号,没有则返回-1
    def find(col):
        try:
            return df_price.columns.get_loc(col)
        except Exception as e:
            return -1
    if(isinstance(extra, list)):
        # 添加默认数据之外的列
        data_col = [find(i) for i in extra]
        class pdData(bt.feeds.PandasData):
            lines = tuple(extra)
            params = tuple(zip(extra,data_col))
        print(data_col)
        data_col = [find(i) for i in ['date','open','high','low','close','vol','oi']]
        print(data_col)
        data = pdData(dataname=df_price,
                                datetime = data_col[0],
                                open = data_col[1],
                                high = data_col[2],
                                low = data_col[3],
                                close = data_col[4],
                                volume = data_col[5],
                                openinterest = data_col[6],
                                fromdate = start,
                                todate = end)
    else:
        data_col = [find(i) for i in ['date','open','high','low','close','vol','oi']]
        print(data_col)
        data = bt.feeds.PandasData(dataname=df_price,
                                datetime = data_col[0],
                                open = data_col[1],
                                high = data_col[2],
                                low = data_col[3],
                                close = data_col[4],
                                volume = data_col[5],
                                openinterest = data_col[6],
                                fromdate = start,
                                todate = end)
    return data



