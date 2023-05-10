import pandas as pd
import numpy as np
from numpy_ext import rolling_apply
import copy, math

# 关于pandas经常使用到的一些操作 以及对现有函数的改进

# dataframe, 查找的列， 为value时删除，包括np.nan 
# return 操作后df 与被删除的df
def drop_row(df, col, value_list):
    drop_all = pd.DataFrame(columns = df.columns)
    for value in value_list:
        # nan特殊处理
        if type(value) == type(np.nan):
            if np.isnan(value):
                # 重新排序
                df = df.reset_index(drop = True)
                beishanchu = df[df[col].isnull() == True]
                df = df.drop(df.index[df[col].isnull() == True].values)
                df = df.reset_index(drop = True)
        else:
            df = df.reset_index(drop = True)
            beishanchu = df[df[col] == value]
            df = df.drop(df.index[df[col] == value].values)
            df = df.reset_index(drop = True)
        drop_all = pd.concat([drop_all, beishanchu], ignore_index=True)

    return df, drop_all


# merge的改进， 避免列被重新命名(新加的为a_，并且保持on的列不变
def nmerge(df, df_add, on):
    new_name = ['a_' + x for x in df_add.columns]
    new_name = dict(zip(df_add.columns,new_name))
    df_add = df_add.rename(columns = new_name)
    # 将 on 列 改回
    new_name = ['a_' + x for x in on]
    new_name = dict(zip(new_name,on))
    # on
    df_add = df_add.rename(columns = new_name)

    # merge
    return df.merge(df_add, on=on)


# 将df中所有行按照年份划分，返回一个列表包含各年度的行，从最早开始
def divide_row(df):
    first_year = df['date'].min().year
    last_year = df['date'].max().year
    result = []
    for y in range(first_year, last_year+1):
        select = list(map(lambda x: x.year==y, df['date']))
        df_ = df[select]
        result.append(df_)
    return result

# 按sortby排序，使用unique_id列第一次出现的行组成新的df
def extract_first(df, unique_id = 'thscode', sortby = 'date'):
    df_result = pd.DataFrame(columns = df.columns)
    df = df.sort_values(by = sortby)
    for i in df[unique_id].unique():
        # 默认按时间第一行
        df_ = df[df[unique_id]==i].iloc[0]
        df_ = df_.to_frame().T
        df_result = pd.concat([df_result, df_], ignore_index = True)
    
    return df_result

# 按unique_col将所有combine_col的值合并为所有values的list
def combine_row(df, unique_col='date', combine_col='order_status'):
    df_result = pd.DataFrame(columns = list(df.columns))
    for date in list(df[unique_col].unique()):
        list_status = list(df[df[unique_col] == date][combine_col].values)
        df_ = pd.DataFrame({unique_col:date, combine_col:[list_status]})
        df_result = pd.concat([df_result, df_])
    return df_result



# x, y 自变量与因变量的列名（单自变量）
# 返回DataFrame  0，1分别为回归系数与截距
def rolling_reg(df, x_name, y_name, n):
    # 如果这一字段的df长度小于n，直接返回nan,对应index
    if df.shape[0]<n:
        result_nan = np.ones(df.shape[0])*np.nan
        result = pd.Series(result_nan, df.index)
        return result
    # 回归的x和y
    x = df[x_name]
    y = df[y_name]
    def func(x, y):
        # 可能出现连续0值造成错误，这时使用np.nan填充
        try:
            result = np.polyfit(x, y, deg=1)
        except:
            # 一元回归情况
            result = np.ones(2)*np.nan
        return result
    result = rolling_apply(func, n, x, y)
# 添加index
    result = pd.DataFrame(result, index=df.index)
    return result




# 行情数据的常用计算
# 数据格式为 mutiindex (date,code)   close...

# 计算字段相较于上n个bar的收益率
def cal_return(df, cal='close', n=1, log=False):
    prevalue = df.groupby('code', sort=False).shift(n)[cal]
    if log:
        result = np.log(df[cal]/prevalue)
    else:
        result = df[cal]/prevalue - 1
    return result

# 相较上n个bar的变化
def cal_diff(df, cal='close', n=1):
    prevalue = df.groupby('code', sort=False).shift(n)[cal]
    result = df[cal] - prevalue
    return result

# 计算字段MA
def cal_MA(df, cal, period=5):
    df = copy.deepcopy(df)
# inde必须为 'code'和'date'，并且code内部的date排序
    df = df.reset_index()
    df = df.sort_values(by='code')
    df = df.set_index(['code','date'])
    df = df.sort_index(level=['code','date'])
# 计算MA
    new_col = cal + '_MA_' + str(period)
    df[new_col] =  df.groupby('code', sort=False).rolling(period)[cal].mean().values
# 将index变回 date code
    df = df.reset_index()
    df = df.sort_values(by='date')
    df = df.set_index(['date','code'])
    df = df.sort_index(level=['date','code']) 
    return df


# 计算对数收益率, ln(close/open), 波动率（年化）， 平均值   
def cal_RV(df, n=10):
    df = copy.deepcopy(df)
# inde必须为 'code'和'date'，并且code内部的date排序
    df = df.reset_index()
    df = df.sort_values(by='code')
    df = df.set_index(['code','date'])
    df = df.sort_index(level=['code','date'])
    # 计算日内对数收益率
    df['R'] = (df['close']/df['open']).apply(lambda x: math.log(x))
# 计算对数收益率波动率
    df['RV'] =  df.groupby('code', sort=False).rolling(n)['R'].std().values
# 将index变回 date code
    df = df.reset_index()
    df = df.sort_values(by='date')
    df = df.set_index(['date','code'])
    df = df.sort_index(level=['date','code']) 
    return df

# 收盘价计算年化波动率  过去n bar数据
def cal_HV(df, n=20):
    df = copy.deepcopy(df)
    # inde必须为 'code'和'date'，并且code内部的date排序
    df = df.reset_index()
    df = df.sort_values(by='code')
    df = df.set_index(['code','date'])
    df = df.sort_index(level=['code','date'])
    # 计算每日对数收益率   shift会自动在二级index中shift
    df['returns'] = (df['close']/(df['close'].shift())).apply(lambda x: np.log(x))
# 计算对数收益率波动率
    name = 'HV_' + str(n)
    df[name] =  df.groupby('code', sort=False).rolling(n)['returns'].std().values * np.sqrt(250)
# 将index变回 date code
    df = df.reset_index()
    df = df.sort_values(by='date')
    df = df.set_index(['date','code'])
    df = df.sort_index(level=['date','code']) 
    return df.drop('returns', axis=1)


# 获得df中x_name列为自变量 y_name列为因变量的线性回归结果 
def cal_reg(df, x_name, y_name, n):
    df = copy.deepcopy(df)
    # inde必须为 'code'和'date'，并且code内部的date排序
    df = df.reset_index()
    df = df.sort_values(by='code')
    df = df.set_index(['code','date'])
    df = df.sort_index(level=['code','date'])

    # 回归    去掉二级index中的code
    df_reg = df.groupby('code', sort=False).apply(lambda df: rolling_reg(df.reset_index('code'), x_name, y_name, n))
    # 命名规则
    name_beta = x_name + '-' + y_name + '--beta' +str(n)
    name_alpha = x_name + '-' + y_name + '--alpha'+str(n)
    df[[name_beta,name_alpha]] = df_reg

    # 将index变回 date code
    df = df.reset_index()
    df = df.sort_values(by='date')
    df = df.set_index(['date','code'])
    df = df.sort_index(level=['date','code']) 

    return df





