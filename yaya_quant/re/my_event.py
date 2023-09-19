import pandas as pd
import numpy as np
import datetime
import math
from yaya_quant.re import my_str

# 针对事件分析的函数，时间点事件通常用标的，日期的dict表示


# 统计合约开始时间，结束时间，存在天数
def count_dur(df, code = 'thscode'):
    df_result = pd.DataFrame(columns = [code, 'start_date', 'end_date', 'count'])
    df = df.sort_values(by = 'date')
    for i in df[code].unique():
        # 默认按时间第一行
        df_i = df[df[code]==i]
        df_ = pd.DataFrame(columns = [code, 'start_date', 'end_date', 'count'], index=[0])
        df_[code] = i
        df_['start_date'] = df_i.iloc[0]['date']
        df_['end_date'] = df_i.iloc[-1]['date']
        df_['count'] = len(df_i)
        df_result = pd.concat([df_result, df_], ignore_index = True) 
    return df_result


# 转债在事件发生后，持有n日后，对数收益率
#dict_date:  key thscode, value list of date(事件后可以买入的日期)
# hold_days = list(range(61))
#df_price:   date thscode  open close
def event_return(dict_date, df_price, hold_days):
    # 建立dataframe 前两列为合约与日期
    columns = ['thscode', 'date']
    # return_1 代表持有1天，即当天(0)的收盘价与开盘价之比
    for i in hold_days:
        columns.append('return_%d'%(i+1))
    df_return = pd.DataFrame(columns = columns)
    # 每个合约
    for i in list(dict_date.keys()):
        # 事件日期列表
        list_date = dict_date[i]
        # 如果在行情数据中没有,输出，跳过
        if(i not in df_price.thscode.unique()):
            print('not found',i)
            continue
        # 筛选出此合约行情
        df_ = df_price[df_price.thscode == i]
        # 按日期排序
        df_ = df_.sort_values(by = 'date')
        df_ = df_.reset_index(drop=True)
        # 每一次事件
        for start_date in list_date:
            # 公告日期为实际发布公告日期后次日，在此时可以直接买入
            # 为交易日则直接买入 
            if start_date in df_.date.values:
                start = df_[df_.date == start_date]
            # 如果不是交易日则后延
            else:
                # 最多尝试30天
                try_num = 0
                while try_num < 30:
                    try_num += 1
                    start_date += datetime.timedelta(1)
                    if start_date in df_.date.values:
                        start = df_[df_.date == start_date]
                        break
                # 没有找到则下一个日期或转债
                if(try_num==30):
                    print('fail: ', i, start_date)
                    continue
            # 持有到end，需存在行情数据
            dur = [start.index[0]+dur_i for dur_i in hold_days if (start.index[0]+dur_i) < len(df_.index)]
            end = df_.loc[dur]
    #       公告日开盘价到持有日收盘价 收益率
            return_list = list((end.close/start.open.values[0]).apply(lambda x: math.log(x)))
        # 字典 value
            dict_values = [i,start_date]
            dict_values.extend(return_list)
            append_dict = dict(zip(columns, dict_values))
            df_return = df_return.append(append_dict, ignore_index=True)
        
    return df_return






# 处理 df_return
# 将事件从最开始时间到结束时间平均分为N份，统计各个区间。 
# 平均值， 标准差，偏度，峰度，中位数，胜率
# numpy与pandas标准差计算结果不同，pandas计算的是样本标准差用（n-1）除的。
def stat_period(df_return, N=1):
    all_date = np.sort(df_return.date.unique())
    divide_date = [all_date[0] + i*(all_date[-1] - all_date[0])/N for i in range(N+1)]
    
    # 储存结果
    columns = ['date','stat']
    columns.extend(list(df_return.columns[2:]))
    df_period_return = pd.DataFrame(columns = columns)
    
    for i in range(N):
        start_date, end_date = divide_date[i], divide_date[i+1]
        df_ = df_return[(df_return.date>=start_date)&(df_return.date<=end_date)]
        # 去除合约，日期
        df_ = df_[df_.columns[2:]]
        # 计算平均值后转置，添加初始日期与统计类型
        df_mean = df_.mean()
        df_mean = df_mean.to_frame().T
        df_mean['date'] = start_date
        df_mean['stat'] = 'mean'
        # 标准差
        df_std = df_.std()
        df_std = df_std.to_frame().T
        df_std['date'] = start_date
        df_std['stat'] = 'std'
        # skew
        df_skew = df_.skew()
        df_skew = df_skew.to_frame().T
        df_skew['date'] = start_date
        df_skew['stat'] = 'skew'
        # kurt
        df_kurt = df_.kurt()
        df_kurt = df_kurt.to_frame().T
        df_kurt['date'] = start_date
        df_kurt['stat'] = 'kurt'
        # 中位数
        df_median = df_.median()
        df_median = df_median.to_frame().T
        df_median['date'] = start_date
        df_median['stat'] = 'median'
        # count
        # 不是nan的个数
        df_count = len(df_) - df_.isnull().sum()
        df_count = df_count.to_frame().T
        # 胜率
        df_wr =  (df_>0).sum()    # np.nan >0  is  False
        df_wr = df_wr.to_frame().T/df_count
        df_wr['date'] = start_date
        df_wr['stat'] = 'wr'
        # 最后给count 添加date与stat属性
        df_count['date'] = start_date
        df_count['stat'] = 'count'
        # result
        df_period_return = pd.concat([df_period_return, df_mean, df_std, 
            df_skew, df_kurt, df_median, df_wr, df_count], ignore_index=True)
    
    return df_period_return


# 从stat_period中获得字段的stat的pd，column为开始时间 index 为return_n
def extract_stat(df_stat_return, stat = 'mean'):
    df_result = df_stat_return[df_stat_return['stat'] == stat]
    # 保留开始日期
    df_result.index = df_result.date
    df_result = df_result[df_result.columns[2:]].T
    df_result.index = list(map(my_str.extract_number, df_result.index.to_list()))
    return df_result














