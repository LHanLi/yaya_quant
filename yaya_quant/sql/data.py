from iFinDPy import *
import datetime
import pandas as pd
import rqdatac as rq
from yaya_quant.re import my_io


# 同花顺数据
#THS_iFinDLogin('zsqhx005','b1580f')
#THS_iFinDLogin('zsqh517','zhiyuan91242')
#THS_iFinDLogin('zsqh588','282092')
# 正式账号
#THS_iFinDLogin('zsqh256', '357411')



# 是否为交易日
def is_tradeday(today):
    temp = THS_Date_Query('212001','mode:1,dateType:0,period:D,dateFormat:0',str(today),str(today))
# 非交易日
    if temp.data == '':
        return False
    else:
        return True

# 获取转债数据     today  首先保证为交易日               
def query_convert(today):
# 可交易转债
    query = today + ';031026_640007'
    temp = THS_DP('block',query,'thscode:Y,security_name:Y')
    if type(temp.data) != type(None):
        codes0 = temp.data['THSCODE']
    # 只取沪深转债
        codes0 = codes0[codes0.apply(lambda x: ('SH' in x)|('SZ' in x))]
    else:
        codes0 = ()
# 退市转债
    query = today + ';031014002004'
    temp = THS_DP('block',query,'thscode:Y,security_name:Y')
    if type(temp.data) != type(None):
        codes1 = temp.data['THSCODE']
        codes1 = codes1[codes1.apply(lambda x: ('SH' in x)|('SZ' in x))]
    else:
        codes1 = ()
    codes = list(set(codes0) | set(codes1))
# 同花顺数据提取code标准格式
    codequery = ''
    for i in codes:
        codequery += i+','
    # 去掉最后一个,
    codequery = codequery[:-1]
# 行情信息 开高低收 vol
    HQ = THS_HQ(codequery,'open,high,low,close,volume','PriceType:1',today,today).data
# 基本信息  转股价 纯债价值 债券余额 对应正股代码
    BD = THS_BD(codequery,'ths_conversion_price_cbond;ths_pure_bond_value_cbond;ths_bond_balance_bond;ths_stock_code_cbond',
       today+';'+today+';'+today+';').data
# 最终结果df
    df = HQ.merge(BD, on='thscode')
    df = df.rename(columns={'time':'date','thscode':'code','volume':'vol',
                       'ths_conversion_price_cbond':'conversion','ths_pure_bond_value_cbond':'pure_bond',
                      'ths_bond_balance_bond':'balance','ths_stock_code_cbond':'stock_code'})
# to_sql方法无法处理mutiindex
#    df = df.set_index(['date','code'])
    return df


# 米筐数据
#rq.init('13645719609', '123456')

# 修改code命名方式为通用方式
def normRQcode(code):
    code = code.replace('XSHE','SZ')
    code = code.replace('XSHG','SH')
    return code

# 获取当日股票数据  复权信息， 行业、状态， 行情
def query_stock(today):
# 获得在交易A股所有股票代码，行业分类，特殊状态（ST）
    tradecode = rq.all_instruments(type='CS', market='cn', date=today)[['order_book_id','industry_code','special_type']]
    tradecode = tradecode.rename(columns={'order_book_id':'code'})
    tradecode['date'] = today
# 将tradecode中code转化为通用命名方式
    secu = list(tradecode['code'])
# 行情数据 高开低手 成交量  流通股本 总股本
    tradecode['code'] = tradecode['code'].apply(normRQcode)
    # 结束是tomorrow才能获得当日数据
#    tomorrow = str((pd.to_datetime(today) + datetime.timedelta(1)).date())
    price = rq.get_price(secu, start_date=today, end_date=today,
                      fields=['close', 'high', 'open', 'low', 'volume'],
                      adjust_type='none')
    shares = rq.get_shares(secu, start_date=today, end_date=today,
                      fields=['total', 'free_circulation']) 
    stock = price.merge(shares, on=['date','order_book_id'])
    stock = stock.reset_index().rename(columns = {'order_book_id':'code'})
    stock['code'] = stock['code'].apply(normRQcode)
    # 复权因子 (对应每只股票的每个除权除息日)
    exfactor = rq.get_ex_factor(secu, start_date=today, end_date=today)
    if type(exfactor) != type(None):
        exfactor = exfactor.reset_index()
        exfactor['code'] = exfactor['order_book_id'].apply(normRQcode)
        exfactor = exfactor.rename(columns = {'ex_date':'date'})
        exfactor = exfactor[['date','code','ex_factor']]
    return tradecode, stock, exfactor



