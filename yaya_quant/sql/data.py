from iFinDPy import *
# datetime 需要在 import iFinDPy 后
import datetime
import pandas as pd
import rqdatac as rq
from yaya_quant.re import my_io


# 同花顺数据
#THS_iFinDLogin('zsqhx005','b1580f')
#THS_iFinDLogin('zsqh517','zhiyuan91242')
#THS_iFinDLogin('zsqh588','282092')


# 是否为交易日   格式为  '2023-05-10'
def is_tradeday(today):
    temp = THS_Date_Query('212001','mode:1,dateType:0,period:D,dateFormat:0',str(today),str(today))
# 非交易日
    if temp.data == '':
        return False
    else:
        return True

# 获取某日市场全部转债
def query_CBlist(today, trade=0):
# 数据查询时的全部可交易转债
    query = today + ';031026_640007'
    temp = THS_DP('block',query,'thscode:Y,security_name:Y')
    if type(temp.data) != type(None):
        codes0 = temp.data['THSCODE']
    # 只取沪深转债
        codes0 = codes0[codes0.apply(lambda x: ('SH' in x)|('SZ' in x))]
    else:
        codes0 = ()
# 数据查询时的全部可交换债
    query = today + ';031026_640021001'
    temp = THS_DP('block',query,'thscode:Y,security_name:Y')
    if type(temp.data) != type(None):
        codes01 = temp.data['THSCODE']
    # 只取沪深转债
        codes01 = codes01[codes01.apply(lambda x: ('SH' in x)|('SZ' in x))]
    else:
        codes01 = ()
# 退市转债
    query = today + ';031014002004'
    temp = THS_DP('block',query,'thscode:Y,security_name:Y')
    if type(temp.data) != type(None):
        codes1 = temp.data['THSCODE']
        codes1 = codes1[codes1.apply(lambda x: ('SH' in x)|('SZ' in x))]
    else:
        codes1 = ()
# 退市可交换债
    query = today + ';031014002015'
    temp = THS_DP('block',query,'thscode:Y,security_name:Y')
    if type(temp.data) != type(None):
        codes11 = temp.data['THSCODE']
        codes11 = codes11[codes11.apply(lambda x: ('SH' in x)|('SZ' in x))]
    else:
        codes11 = ()
    # 包含全部 today 时可交易转债（还包含today时已退市转债）
    codes = list(set(codes0) | set(codes01) | set(codes1) | set(codes11))
    if trade:
        return codes
    # 当日可交易
    else:
        # 同花顺数据提取code标准格式
        codequery = ''
        for i in codes:
            codequery += i+','
            # 去掉最后一个,
            codequery = codequery[:-1]
        #  自动略过当时未上市和退市转债
        HQ = THS_HQ(codequery,'open','PriceType:1',today,today).data
        if HQ==None:
            return []
        codes = list(HQ.thscode.values)
        return codes

# 获取转债与可交换债数据     today  首先保证为交易日               
def query_convert(today, enddate=None, codes=None):
    if codes==None:
    # 数据查询时的全部可交易转债
        query = today + ';031026_640007'
        temp = THS_DP('block',query,'thscode:Y,security_name:Y')
        if type(temp.data) != type(None):
            codes0 = temp.data['THSCODE']
        # 只取沪深转债
            codes0 = codes0[codes0.apply(lambda x: ('SH' in x)|('SZ' in x))]
        else:
            codes0 = ()
    # 数据查询时的全部可交换债
        query = today + ';031026_640021001'
        temp = THS_DP('block',query,'thscode:Y,security_name:Y')
        if type(temp.data) != type(None):
            codes01 = temp.data['THSCODE']
        # 只取沪深转债
            codes01 = codes01[codes01.apply(lambda x: ('SH' in x)|('SZ' in x))]
        else:
            codes01 = ()
    # 退市转债
        query = today + ';031014002004'
        temp = THS_DP('block',query,'thscode:Y,security_name:Y')
        if type(temp.data) != type(None):
            codes1 = temp.data['THSCODE']
            codes1 = codes1[codes1.apply(lambda x: ('SH' in x)|('SZ' in x))]
        else:
            codes1 = ()
    # 退市可交换债
        query = today + ';031014002015'
        temp = THS_DP('block',query,'thscode:Y,security_name:Y')
        if type(temp.data) != type(None):
            codes11 = temp.data['THSCODE']
            codes11 = codes11[codes11.apply(lambda x: ('SH' in x)|('SZ' in x))]
        else:
            codes11 = ()
    # 包含全部 today 时可交易转债（还包含today时已退市转债）
        codes = list(set(codes0) | set(codes01) | set(codes1) | set(codes11))
# 同花顺数据提取code标准格式
    codequery = ''
    for i in codes:
        codequery += i+','
    # 去掉最后一个,
    codequery = codequery[:-1]
    if enddate==None:
        #print(codequery)
# 行情信息 开高低收 vol   自动略过退市转债
        HQ = THS_HQ(codequery,'open,high,low,close,volume,amount,yieldMaturity,remainingTerm,maxwellDuration,modifiedDuration,convexity',\
                'PriceType:1',today,today).data
    else:
        HQ = THS_HQ(codequery,'open,high,low,close,volume,amount,yieldMaturity,remainingTerm,maxwellDuration,modifiedDuration,convexity',\
                'PriceType:1',today, enddate).data
    #print(HQ.thscode.values)
    # codequery2不含退市转债，节约数据量
    codequery2 = ''
    i = 0
    while i+9 <= len(codequery):
        if codequery[i:i+9] in HQ.thscode.values:
            codequery2 += codequery[i:i+9] + ','
        i += 10
    codequery2 = codequery2[:-1]
    # 行情获取不到的代码表示当时该转债不存在，则不进行继续（！！！非常重要，如果继续查询计算数据量！！！）
# 基本信息  
    if enddate==None:
        #print(codequery2)
        BD = THS_BD(codequery2,
    'ths_conversion_price_cbond;ths_pure_bond_value_cbond;ths_bond_balance_bond;ths_specified_date_bond_rating_bond;ths_holder_held_ratio_cbond',\
        today+';'+today+';'+today+';'+today+',100;'+today+',1').data
        df = HQ.merge(BD, on='thscode')
    else:
        BD = pd.DataFrame()
        # 每一个交易日
        for today in HQ['time'].unique():
            BD_ = THS_BD(codequery2,
    'ths_conversion_price_cbond;ths_pure_bond_value_cbond;ths_bond_balance_bond;ths_specified_date_bond_rating_bond;ths_holder_held_ratio_cbond',\
        today+';'+today+';'+today+';'+today+',100;'+today+',1').data
            BD_['time'] = today
            BD = pd.concat([BD, BD_])
        df = HQ.merge(BD, on=['time', 'thscode'])
# 最终结果df
    df = df.rename(columns={'time':'date','thscode':'code','volume':'vol', 'yieldMaturity':'ytm', \
                            'remainingTerm':'Dur', 'maxwellDuration':'mwDur', 'modifiedDuration':'mDur',\
                       'ths_conversion_price_cbond':'conversion','ths_pure_bond_value_cbond':'pure_bond',\
                      'ths_bond_balance_bond':'balance', 'ths_specified_date_bond_rating_bond':'credit',\
                      'ths_holder_held_ratio_cbond':'concentration'})
# to_sql方法无法处理mutiindex
#    df = df.set_index(['date','code'])
    return df

# 获取转债分钟线
def query_CB_min(query_date, codes):
    # 单次查询数据量限制在200w以内，需要分批次查询
    split_codes = [codes[i:i+300] for i in range(0, len(codes), 300)]
    #len(sum(split_codes, []))
    query_codes_list = ["".join([i+',' for i in codes])[:-1] for codes in split_codes]
    result = [THS_HF(query_codes,'open;high;low;close;volume;amount,sellVolume;buyVolume','Fill:Original',\
                '%s 09:15:00'%query_date,'%s 15:15:00'%query_date).data.rename(\
                    columns={'time':'date', 'thscode':'code', 'volume':'vol'}) for query_codes in query_codes_list]
    #result = [THS_HF(query_codes,'open;high;low;close;volume;amount','Fill:Original',\
    #            '%s 09:15:00'%query_date,'%s 15:15:00'%query_date).data.rename(\
    #                columns={'time':'date', 'thscode':'code', 'volume':'vol'}) for query_codes in query_codes_list]
    # 深市单位是张，沪市单位是手，全部统一为张
    result = pd.concat(result)
    result['vol'] = result.apply(lambda x: x['vol'] if x['code'][-2:]=='SZ' else 10*x['vol'], axis=1)
    return result

## 查询日期（如果存在截止日期则为开始日期）， 查询代码
## 获取转债与可交换债数据     today  首先保证为交易日               
#def get_CB(today, enddate=None, codes=None):
#    # 如果codes为None则获取today时可交易的全部转债
#    if codes==None:
#    # 数据查询时的全部可交易转债
#        query = today + ';031026_640007'
#        temp = THS_DP('block',query,'thscode:Y,security_name:Y')
#        if type(temp.data) != type(None):
#            codes0 = temp.data['THSCODE']
#        # 只取沪深转债
#            codes0 = codes0[codes0.apply(lambda x: ('SH' in x)|('SZ' in x))]
#        else:
#            codes0 = ()
#    # 数据查询时的全部可交换债
#        query = today + ';031026_640021001'
#        temp = THS_DP('block',query,'thscode:Y,security_name:Y')
#        if type(temp.data) != type(None):
#            codes01 = temp.data['THSCODE']
#        # 只取沪深转债
#            codes01 = codes01[codes01.apply(lambda x: ('SH' in x)|('SZ' in x))]
#        else:
#            codes01 = ()
#    # 退市转债
#        query = today + ';031014002004'
#        temp = THS_DP('block',query,'thscode:Y,security_name:Y')
#        if type(temp.data) != type(None):
#            codes1 = temp.data['THSCODE']
#            codes1 = codes1[codes1.apply(lambda x: ('SH' in x)|('SZ' in x))]
#        else:
#            codes1 = ()
#    # 退市可交换债
#        query = today + ';031014002015'
#        temp = THS_DP('block',query,'thscode:Y,security_name:Y')
#        if type(temp.data) != type(None):
#            codes11 = temp.data['THSCODE']
#            codes11 = codes11[codes11.apply(lambda x: ('SH' in x)|('SZ' in x))]
#        else:
#            codes11 = ()
#    # 包含全部 today 时可交易转债（还包含today时已退市转债）
#        codes = list(set(codes0) | set(codes01) | set(codes1) | set(codes11))
## 同花顺数据提取code标准格式
#    codequery = ''
#    for i in codes:
#        codequery += i+','
#    # 去掉最后一个,
#    codequery = codequery[:-1]
#    
## 行情信息 开高低收 vol   自动略过退市转债
#    if enddate==None:
#        HQ = THS_HQ(codequery,'open,high,low,close,volume','PriceType:1',today,today).data
#    else:
#        HQ = THS_HQ(codequery,'open,high,low,close,volume','PriceType:1',today,enddate).data
#    # 更新codequery使其只含有当时可交易转债，节约数据量
#    if codes==None:
#        codequery2 = ''
#        i = 0
#        while i+9 < len(codequery):
#            if codequery[i:i+9] in HQ.thscode.values:
#                codequery2 += codequery[i:i+9] + ','
#            i += 10
#        codequery = codequery2[:-1]
#    # 行情获取不到的代码表示当时该转债不存在，则不进行继续（！！！非常重要，如果继续查询计算数据量！！！）
## 基本信息  转股价 纯债价值 债券余额 对应正股代码
##    BD = THS_BD(codequery,'ths_conversion_price_cbond;ths_pure_bond_value_cbond;ths_bond_balance_bond;ths_stock_code_cbond',
##       today+';'+today+';'+today+';').data
## 基本信息  
## 转股价 纯债价值 债券余额 对应正股代码
## 到期收益率  久期   凸性  信用评级  发行信用评级
## 基金持有占比  基金家数  前十大持有人占比 
##    BD = THS_BD(codequery,'ths_conversion_price_cbond;ths_pure_bond_value_cbond;ths_bond_balance_bond;ths_stock_code_cbond;ths_ytm_bond;ths_dur_bond;ths_convexity_bond;ths_specified_date_bond_rating_bond;ths_issue_credit_rating_bond;ths_fundholdratio_of_positionamt_bond;ths_held_fund_corp_num_bond;ths_holder_held_ratio_cbond',
##       today+';'+today+';'+today+';').data
##    BD = THS_BD(codequery,
##'ths_conversion_price_cbond;ths_pure_bond_value_cbond;ths_bond_balance_bond;ths_stock_code_cbond;ths_ytm_bond;ths_dur_bond;ths_convexity_bond;ths_specified_date_bond_rating_bond;ths_issue_credit_rating_bond;ths_fundholdratio_of_positionamt_bond;ths_held_fund_corp_num_bond;ths_holder_held_ratio_cbond',
##   today+';'+today+';'+today+';'+today+',100;'+today+';'+today+';'+today+',100;;104;104;'+today+';'+today+',1').data
#    if enddate==None:
#        BD = THS_BD(codequery,
#'ths_conversion_price_cbond;ths_pure_bond_value_cbond;ths_bond_balance_bond;ths_stock_code_cbond;ths_pure_bond_ytm_cbond;ths_dur_bond;ths_convexity_bond;ths_specified_date_bond_rating_bond;ths_issue_credit_rating_bond;ths_fundholdratio_of_positionamt_bond;ths_held_fund_corp_num_bond;ths_holder_held_ratio_cbond',
#   today+';'+today+';'+today+';;'+today+';'+today+';'+today+';'+today+',100;;104;104;'+today+',1').data
#    # 最终结果df
#        df = HQ.merge(BD, on='thscode')
#    else:
#        BD = pd.DataFrame()
#        # 每一个交易日
#        for today in HQ['time'].unique():
#            BD_ = THS_BD(codequery,
#    'ths_conversion_price_cbond;ths_pure_bond_value_cbond;ths_bond_balance_bond;ths_stock_code_cbond;ths_pure_bond_ytm_cbond;ths_dur_bond;ths_convexity_bond;ths_specified_date_bond_rating_bond;ths_issue_credit_rating_bond;ths_fundholdratio_of_positionamt_bond;ths_held_fund_corp_num_bond;ths_holder_held_ratio_cbond',
#    today+';'+today+';'+today+';;'+today+';'+today+';'+today+';'+today+',100;;104;104;'+today+',1').data
#            BD_['time'] = today
#            BD = pd.concat([BD, BD_])
#        # 最终结果df
#        df = HQ.merge(BD, on=['time', 'thscode'])
#
##    df = df.rename(columns={'time':'date','thscode':'code','volume':'vol',
##                       'ths_conversion_price_cbond':'conversion','ths_pure_bond_value_cbond':'pure_bond',
##                      'ths_bond_balance_bond':'balance','ths_stock_code_cbond':'stock_code'})
#    df = df.rename(columns={'time':'date','thscode':'code','volume':'vol',
#                       'ths_conversion_price_cbond':'conversion','ths_pure_bond_value_cbond':'pure_bond',
#                      'ths_bond_balance_bond':'balance','ths_stock_code_cbond':'stock_code',
#                      'ths_pure_bond_ytm_cbond':'ytm', 'ths_dur_bond':'dur', 'ths_convexity_bond':'convexity',
#                      'ths_specified_date_bond_rating_bond':'credit','ths_issue_credit_rating_bond':'init_credit',
#                      'ths_fundholdratio_of_positionamt_bond':'fund_ratio','ths_held_fund_corp_num_bond':'fund_num',
#                      'ths_holder_held_ratio_cbond':'concentration'})
## to_sql方法无法处理mutiindex
##    df = df.set_index(['date','code'])
#    return df


# 获取etf数据
def query_ETFlist(today, trade=True):
    # 全部ETF
    query = today+ ';051001006'
    # code_list 
    temp = THS_DP('block',query,'date:Y,thscode:Y,security_name:Y')
    codes = list(temp.data['THSCODE'])
    return codes

# 宽基ETF，行业ETF
def query_etf(today):
    # 全部ETF
    query = today+ ';051001006'
    # code_list 
    temp = THS_DP('block',query,'date:Y,thscode:Y,security_name:Y')
    code_list = list(temp.data['THSCODE'])
    codequery = ''
    for i in code_list:
        codequery += i+','
    # 去掉最后一个,
    codequery = codequery[:-1]
    temp = THS_HQ(codequery,\
        'open,high,low,close,volume,amount,turnoverRatio,adjustmentFactorBackward1,netAssetValue',\
            '',today, today)
    df = temp.data
    return df.rename(columns = {'time':'date', 'thscode':'code',\
                                'adjustmentFactorBackward1':'exfactor', 'netAssetValue':'net'}) 
    ## 宽基etf
    #if kind == 'broad':
    #    # 查询全部合约名称
    #    query = today + ';051001006006001001'
    ## 行业etf
    #elif kind == 'industry':
    #    query = today + ';051001006006001002'
    ## 策略etf
    #elif kind == 'strat':
    #    query = today + ';051001006006001003'
    ## code_list 
    #temp = THS_DP('block',query,'date:Y,thscode:Y,security_name:Y')
    #code_list = list(temp.data['THSCODE'])
    #codequery = ''
    #for i in code_list:
    #    codequery += i+','
    ## 去掉最后一个,
    #codequery = codequery[:-1]
    #temp = THS_HQ(codequery,'open,high,low,close,volume,adjustmentFactorBackward1','',today, today)
    #df = temp.data
    #return df.rename(columns = {'time':'date', 'thscode':'code','adjustmentFactorBackward1':'exfactor'})

def query_STOCKlist(query_date):
    # 获取股票池
    xianzai = THS_DP('block','%s;001005010'%query_date,'date:Y,thscode:Y')
    tuishi = THS_DP('block','最新;001005334011','date:Y,thscode:Y')
    codes = list(xianzai.data['THSCODE']) + list(tuishi.data['THSCODE']) 
    return codes

def query_stock(query_date, codes=None):
    if codes==None:
        # 获取股票池
        temp = THS_DP('block','%s;001005010'%query_date,'date:Y,thscode:Y')
        tuishi = THS_DP('block','最新;001005334011','date:Y,thscode:Y')
        query_codes = "".join([i+',' for i in temp.data['THSCODE']])
        tuishi_codes = "".join([i+',' for i in tuishi.data['THSCODE']])[:-1]
        query_codes = query_codes + tuishi_codes
    else:
        query_codes = "".join([i+',' for i in codes])[:-1]
    # 获取行情数据
    HQ = THS_HQ(query_codes,'open,high,low,close,volume,amount,transactionAmount,totalShares,floatSharesOfAShares',\
                '',query_date,query_date).data.rename(columns={'time':'date',\
            'thscode':'code', 'totalShares':'total_shares', 'volume':'vol', 'transactionAmount':'deal_times',\
                 'floatSharesOfAShares':'float_shares'})
    # 存在的股票
    query_codes2 = "".join([i + ',' for i in HQ['code']])[:-1]
    # 获取基础数据
    query_str = 'ths_stock_short_name_quote_client_stock;ths_af_stock;ths_free_float_shares_stock'
    query_strdate = '%s;%s;%s'%(query_date,query_date,query_date)
    temp = THS_BD(query_codes2, query_str, query_strdate)
    BD = temp.data.rename(columns={'thscode':'code', 'ths_stock_short_name_quote_client_stock':'name',\
                        'ths_af_stock':'ex_factor',\
                'ths_free_float_shares_stock':'free_float_shares'})
    df = HQ.merge(BD, on='code')
    return df

def query_stock_min(query_date, codes):
    # 单次查询数据量限制在200w以内，需要分批次查询,每次300只股票
    split_codes = [codes[i:i+300] for i in range(0, len(codes), 300)]
    #len(sum(split_codes, []))
    query_codes_list = ["".join([i+',' for i in codes])[:-1] for codes in split_codes]
    result = [THS_HF(query_codes,'open;high;low;close;volume;amount,sellVolume;buyVolume','Fill:Original',\
                '%s 09:15:00'%query_date,'%s 15:15:00'%query_date).data.rename(\
                    columns={'time':'date', 'thscode':'code', 'volume':'vol'}) for query_codes in query_codes_list]
    return pd.concat(result)








##############################################################################
##################################### 季度数据 ###############################
################################################################################


def query_balance(year, quater, codes, tradedate):
    # code 代码， currency 货币资金， receivable 应收账款
    # prepays 预付账款， inventory 存货， current_asset 流动资产
    # fixed 固定资产， building 在建工程， intangible 无形资产
    # goodwill 商誉， deferred 长期待摊费用， asset 总资产，
    # st_borrow 短期负债， receivable_borrow 应付账款， prepays_borrow 预收账款，
    # payroll 应付员工工资， 1y_borrow 一年内到期非流动债务 current_borrow 流动负债， 
    # lt_payable 长期应付款， borrow 总负债， capital_reverve 资本公积
    # earn_reverve 盈余公积， noassigned_profit 未分配利润， equity 归母权益
    # date 发布日期
    #query_codes = "".join([i+',' for i in stocks.loc[look_date].index])[:-1]
    #query_codes = "".join([i+',' for i in \
    #    list(stocks.loc[look_date-datetime.timedelta(days=720):look_date].index.get_level_values(1).unique())])[:-1]
    query_codes = "".join([i+',' for i in codes])[:-1]
    # 资产负债表，不需要区分一季报、半年报、三季报、年报
    # 货币资金、 应收票据及应收账款
    # 预付款项、 存货
    # 流动资产合计
    # 固定资产、 在建工程
    # 无形资产、商誉、长期待摊费用
    # 资产总计
    # 短期借款、应付票据及应付账款、预收款项
    # 应付职工薪酬、一年内到期非流动负债
    # 流动负债合计
    # 长期应付款、负债合计
    # 资本公积、盈余公积、未分配利润
    # 归属母公司所有者权益
    # 公告日期
    query_str = 'ths_currency_fund_pit_stock;ths_bill_and_account_receivable_pit_stock;\
        ths_prepays_pit_stock;ths_inventory_pit_stock;\
            ths_total_current_assets_pit_stock;\
        ths_fixed_asset_sum_pit_stock;ths_construction_in_process_sum_pit_stock;\
                ths_intangible_assets_pit_stock;ths_goodwill_pit_stock;ths_lt_deferred_expense_pit_stock;\
                    ths_total_assets_pit_stock;\
        ths_st_borrow_pit_stock;ths_bill_and_account_payable_pit_stock;ths_advance_payment_pit_stock;\
            ths_payroll_payable_pit_stock;ths_noncurrent_liab_due_in1y_pit_stock;\
                ths_total_current_liab_pit_stock;\
            ths_lt_payable_sum_pit_stock;ths_total_liab_pit_stock;\
        ths_capital_reserve_pit_stock;ths_earned_surplus_pit_stock;ths_undstrbtd_profit_pit_stock;\
            ths_total_equity_atoopc_pit_stock;\
                ths_report_changedate_pit_stock'.replace(' ','')
    # 0331 一季报； 0630 二季报； 0930 三季报； 1231 年报
    yearquater = year + quater
    query_para = '2050-11-30,%s,1;2050-11-30,%s,1;\
        2050-11-30,%s,1;2050-11-30,%s,1;\
            2050-11-30,%s,1;\
                2050-11-30,%s,1;2050-11-30,%s,1;\
                    2050-11-30,%s,1;2050-11-30,%s,1;2050-11-30,%s,1;\
                        2050-11-30,%s,1;\
            2050-11-30,%s,1;2050-11-30,%s,1;2050-11-30,%s,1;\
                2050-11-30,%s,1;2050-11-30,%s,1;\
                    2050-11-30,%s,1;\
            2050-11-30,%s,1;2050-11-30,%s,1;\
                2050-11-30,%s,1;2050-11-30,%s,1;2050-11-30,%s,1;\
                    2050-11-30,%s,1;\
                    2000-11-28,2050-11-19,604,1,%s'%(yearquater, yearquater, yearquater, yearquater, yearquater, \
                                                    yearquater, yearquater, yearquater, yearquater, yearquater,\
                                                    yearquater, yearquater, yearquater, yearquater, yearquater,\
                                                    yearquater, yearquater, yearquater, yearquater, yearquater,\
                                                    yearquater, yearquater, yearquater, yearquater)
    query_para = query_para.replace(' ','')
    global temp
    temp = THS_BD(query_codes,query_str,query_para)
    df = temp.data.rename(columns={'thscode':'code', 'ths_currency_fund_pit_stock':'currency',\
        'ths_bill_and_account_receivable_pit_stock':'receivable', 'ths_prepays_pit_stock':'prepays',\
        'ths_inventory_pit_stock':'inventory', 'ths_total_current_assets_pit_stock':'current_asset',\
        'ths_fixed_asset_sum_pit_stock':'fixed', 'ths_construction_in_process_sum_pit_stock':'building',\
            'ths_intangible_assets_pit_stock':'intangible', 'ths_goodwill_pit_stock':'goodwill',\
                'ths_lt_deferred_expense_pit_stock':'deferred', 'ths_total_assets_pit_stock':'asset',\
                        'ths_st_borrow_pit_stock':'st_borrow', 'ths_bill_and_account_payable_pit_stock':'receivable_borrow',\
            'ths_advance_payment_pit_stock':'prepays_borrow',  'ths_noncurrent_liab_due_in1y_pit_stock':'1y_borrow', 'ths_total_current_liab_pit_stock':'current_borrow',\
            'ths_payroll_payable_pit_stock':'payroll', 'ths_lt_payable_sum_pit_stock':'lt_payable', 'ths_total_liab_pit_stock':'borrow',\
                    'ths_capital_reserve_pit_stock':'capital_reverve', 'ths_earned_surplus_pit_stock':'earn_reverve',\
                        'ths_undstrbtd_profit_pit_stock':'noassigned_profit', 'ths_total_equity_atoopc_pit_stock':'equity',\
                        'ths_report_changedate_pit_stock':'changedate'})
    # 财报初始发布日期
    df['date' ] = df['changedate'].map(lambda x: pd.to_datetime(x.split(',')[0]))
    # 去除退市股票
    df = df.dropna(subset=['date']).copy()
    # 标注日期为财报日期向前推最早一个交易日
    df['date'] = df['date'].map(lambda x: tradedate[tradedate<x][-1])
    df = df.set_index(['date', 'code'])
    return df


def query_income(year, quater, codes, tradedate):
    #query_codes = "".join([i+',' for i in \
    #    list(stocks.loc[look_date-datetime.timedelta(days=365):look_date].index.get_level_values(1).unique())])[:-1]
    query_codes = "".join([i+',' for i in codes])[:-1]
    # 利润表，需要区分一季报、半年报、三季报、年报
    # 营业收入，营业成本
    # 销售费用，管理费用，财务费用
    # 营业利润，归母净利润
    # 财报变更日期
    query_str = 'ths_revenue_pit_stock;ths_operating_cost_pit_stock;\
        ths_sales_fee_pit_stock;ths_manage_fee_pit_stock;ths_financing_expenses_pit_stock;\
            ths_op_pit_stock;ths_np_atoopc_pit_stock;\
                ths_report_changedate_pit_stock'.replace(' ','')
    # 0331 一季报； 0630 二季报； 0930 三季报； 1231 年报
    yearquater = year + quater
    query_para = '2050-11-30,%s,1;2050-11-30,%s,1;\
                    2050-11-30,%s,1;2050-11-30,%s,1;2050-11-30,%s,1;\
                2050-11-30,%s,1;2050-11-30,%s,1;\
                2000-11-28,2050-11-19,604,1,%s'%(yearquater, yearquater, yearquater, yearquater, yearquater, \
                                                    yearquater, yearquater, yearquater)
    query_para = query_para.replace(' ','')
    global temp
    temp = THS_BD(query_codes,query_str,query_para)
    df = temp.data.rename(columns={'thscode':'code', 'ths_revenue_pit_stock':'revenue%s'%quater, 'ths_manage_fee_pit_stock':'manage_fee%s'%quater,\
        'ths_operating_cost_pit_stock':'operating_cost%s'%quater, 'ths_sales_fee_pit_stock':'sales_fee%s'%quater,\
        'ths_financing_expenses_pit_stock':'financing_fee%s'%quater, 'ths_op_pit_stock':'operating_profit%s'%quater, \
            'ths_np_atoopc_pit_stock':'profit%s'%quater, 'ths_report_changedate_pit_stock':'changedate%s'%quater})
    # 财报初始发布日期
    df['date' ] = df['changedate%s'%quater].map(lambda x: pd.to_datetime(x.split(',')[0]))
    # 去除退市股票
    df = df.dropna(subset=['date']).copy()
    # 标注日期为财报日期向前推最早一个交易日
    df['date'] = df['date'].map(lambda x: tradedate[tradedate<x][-1])
    df = df.set_index(['date', 'code'])
    return df


def query_cashflow(year, quater, codes, tradedate):
    #基础数据-股票-经营活动现金流入小计;经营活动现金流出小计;投资活动现金流入小计;投资活动现金流出小计;筹资活动现金流入小计;分配股利、利润或偿付利息支付的现金等-iFinD数据接口
    query_codes = "".join([i+',' for i in codes])[:-1]
    # 经营现金流入、流出
    # 投资活动流入、流出
    # 筹资活动流入、流出，
    # 分配股利、债息
    # 财报变更日期
    query_str = 'ths_sub_total_of_ci_from_oa_pit_stock;ths_sub_total_of_cos_from_oa_pit_stock;\
        ths_sub_total_of_ci_from_ia_pit_stock;ths_sub_total_of_cos_from_ia_pit_stock;\
            ths_sub_total_of_ci_from_fa_pit_stock;ths_sub_total_of_cos_from_fa_pit_stock;\
       ths_cash_paid_of_distribution_pit_stock;\
        ths_report_changedate_pit_stock'.replace(' ','')
    # 0331 一季报； 0630 二季报； 0930 三季报； 1231 年报
    yearquater = year + quater
    query_para = '%s,1,2050-11-30;1,%s,2050-11-30;\
        %s,1,2050-11-30;%s,1,2050-11-30;%s,1,2050-11-30;\
            %s,1,2050-11-30;%s,1,2050-11-30;\
                2000-11-28,2050-11-19,604,1,%s'%(yearquater, yearquater, yearquater, yearquater, yearquater, \
                                                    yearquater, yearquater, yearquater)
    query_para = query_para.replace(' ','')
    global temp
    temp = THS_BD(query_codes,query_str,query_para)
    df = temp.data.rename(columns={'thscode':'code', 'ths_sub_total_of_ci_from_oa_pit_stock':'OAcashin%s'%quater,\
             'ths_sub_total_of_cos_from_oa_pit_stock':'OAcashout%s'%quater,\
        'ths_sub_total_of_ci_from_ia_pit_stock':'IAcashin%s'%quater, \
            'ths_sub_total_of_cos_from_ia_pit_stock':'IAcashout%s'%quater,\
        'ths_sub_total_of_ci_from_fa_pit_stock':'FAcashin%s'%quater,\
             'ths_sub_total_of_cos_from_fa_pit_stock':'FAcashout%s'%quater, \
            'ths_cash_paid_of_distribution_pit_stock':'distribution%s'%quater,\
                  'ths_report_changedate_pit_stock':'changedate%s'%quater})
    # 财报初始发布日期
    df['date' ] = df['changedate%s'%quater].map(lambda x: pd.to_datetime(x.split(',')[0]))
    # 去除退市股票
    df = df.dropna(subset=['date']).copy()
    # 标注日期为财报日期向前推最早一个交易日
    df['date'] = df['date'].map(lambda x: tradedate[tradedate<x][-1])
    df = df.set_index(['date', 'code'])
    return df





















