from sqlalchemy import create_engine
from numpy_ext import rolling_apply
import datetime
import pandas as pd

enddate = str(datetime.datetime.today().date())

# 提取股票数据，开始到结束日期  ‘2020-1-1’  ‘2021-1-1’
def stocks(start, end=enddate):
    # timestamp
    start = pd.to_datetime(start)
    end = pd.to_datetime(end)
    # 正股数据
    # 从DB读取数据
    engine = create_engine("mysql+pymysql://root:a23187@localhost:3306/stock")
    sql = "SELECT date,code,close,high,open,low,volume,total,free_circulation \
            FROM daily WHERE `date` >= '%s' AND `date` <= '%s'"%(str(start), str(end))
    daily = pd.read_sql(sql, engine)
    sql = "SELECT date,code,industry_code,special_type \
            FROM tradecode WHERE `date` >= '%s' AND `date` <= '%s'"%(str(start), str(end))
    tradecode = pd.read_sql(sql, engine)
    sql = "SELECT date,code,ex_factor \
            FROM exfactor WHERE `date` >= '%s' AND `date` <= '%s'"%(str(start), str(end))
    exfactor = pd.read_sql(sql, engine)
    # sql提取完毕
    print('sql success')
    # 行情数据添加 行业分类 特殊状态
    df_stocks = daily.merge(tradecode, on=['date','code'])
    # 计算累乘复权因子
    exfactor = exfactor.sort_values(by=['code','date'], ascending=False)
    exfactor['ex_factor'] = exfactor.groupby('code')['ex_factor'].cumprod()
    # 填充到stock  没有补nan
    df_stocks = df_stocks.merge(exfactor, on=['date','code'], how='outer')
    df_stocks = df_stocks.sort_values(by='date')
    df_stocks = df_stocks.set_index(['date','code'])
    # 用靠后日期复权因子填充前面的  为空则补1
    # 当天的复权因子只影响之前的复权价格，不影响当天
    df_stocks['ex_factor'] = df_stocks.groupby('code')['ex_factor'].shift(-1)
    df_stocks['ex_factor'] = df_stocks.groupby('code')['ex_factor'].fillna(method='backfill')
    df_stocks['ex_factor'] = df_stocks['ex_factor'].fillna(1)
    # 添加前复权收盘价
    df_stocks['ex_close'] = df_stocks['close']/df_stocks['ex_factor']
    df_stocks = df_stocks.drop('ex_factor', axis=1)
    # volume - vol
    df_stocks = df_stocks.rename(columns={'volume':'vol'})
    return df_stocks

# 北向资金
def connect_holding(start, end=enddate):
    # timestamp
    start = pd.to_datetime(start)
    end = pd.to_datetime(end)
    # 数据库中日期为 字符串格式 '2023-05-22'
    engine = create_engine("mysql+pymysql://root:a23187@localhost:3306/stock")
    sql = "SELECT date,code,connect_holding \
        FROM connect_holding WHERE `date` >= '%s' AND `date` <= '%s'"%(str(start.date()), str(end.date()))
    df_connect_holding = pd.read_sql(sql, engine)



# start '2023-1-1'  end  '2023-5-22'
def convertible(start, end=enddate):
    # timestamp
    start = pd.to_datetime(start)
    end = pd.to_datetime(end)
# 数据库中日期为 字符串格式 '2023-05-22'
    engine = create_engine("mysql+pymysql://root:a23187@localhost:3306/convertible")
    sql = "SELECT date,code,close,high,open,low,vol,conversion, pure_bond, balance, ytm, dur, \
    convexity, credit, init_credit, fund_ratio, fund_num, concentration, stock_code \
        FROM daily WHERE `date` >= '%s' AND `date` <= '%s'"%(str(start.date()), str(end.date()))
    df_convertible = pd.read_sql(sql, engine)
# 日期从字符转化为timestamp，方便检索
    df_convertible['date'] = df_convertible['date'].apply(lambda x: pd.to_datetime(x))
    df_convertible = df_convertible.sort_values(by='date')
    df_convertible = df_convertible.set_index(['date','code'])
    return df_convertible


# start '2023-1-1'   end   '2023-5-22 00:08:00'
def convertible_announce(start, end=enddate):
    # timestamp
    start = pd.to_datetime(start)
    end = pd.to_datetime(end)
# 数据库
    engine = create_engine("mysql+pymysql://root:a23187@localhost:3306/convertible")
# 公告
    sql = "SELECT time,code,abstract FROM announcements \
        WHERE `time` >= '%s' AND `time` <= '%s'"%(str(start),str(end))

    df_announcements = pd.read_sql(sql, engine)
    return df_announcements



# start '2023-1-1'  end  '2023-5-22'
def index(start, end=enddate):
    # timestamp
    start = pd.to_datetime(start)
    end = pd.to_datetime(end)
# 数据库中日期为 字符串格式 '2023-05-22'
    engine = create_engine("mysql+pymysql://root:a23187@localhost:3306/mindex")
    sql = "SELECT date,code,close,high,open,low,amount, totalCapital, floatCapital \
        FROM broadindex WHERE `date` >= '%s' AND `date` <= '%s'"%(str(start.date()), str(end.date()))
    index = pd.read_sql(sql, engine)
    # 板块指数
    #index1 = pd.read_sql_table('blockindex', engine, columns=['date','code','close','high','open','low','amount', 'totalCapital', 'floatCapital'])
    sql = "SELECT date,code,close,high,open,low,amount, totalCapital, floatCapital \
        FROM blockindex WHERE `date` >= '%s' AND `date` <= '%s'"%(str(start.date()), str(end.date()))
    index1 = pd.read_sql(sql, engine)
    # 全部指数
    index = pd.concat([index, index1])
# 日期从字符转化为timestamp，方便检索
    index['date'] = index['date'].apply(lambda x: pd.to_datetime(x))
    index = index.sort_values(by='date')
    index = index.set_index(['date','code'])
    return index

