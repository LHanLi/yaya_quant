import pandas as pd
import numpy as np

# dataframe, 查找的列， 为value时删除， 输出被删除的df
def drop_row(df, col, value):
    # nan特殊处理
    if type(value) == np.float:
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
    
    return df, beishanchu




