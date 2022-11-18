import numpy as np
import pandas as pd
import datetime
import copy


# convert continue variable to discrete and count, lena fenge fenshu
def count_continue(_a,lena):
    a = copy.copy(_a)
    a.sort()
    a_d = (a[-1]-a[0])/lena
    a_count = np.zeros(lena)
    a_cut = a[0] + a_d
    j = 0
    for i in a:
        if(i<=a_cut):
            a_count[j] += 1
        else:
            j += 1
            a_count[j] += 1
            a_cut += a_d
    return a_count

# label = 1, 2, ...,lena
def class_continue(a,lena):
    a_max = a.max()
    a_min = a.min()
    new_a = -np.ones(len(a))
    linspace = np.linspace(a_min,a_max,lena+1)
    linspace[-1] = a_max + (a_max-a_min)/(10*lena)
    
    for i in range(len(a)):
        for j in range(1,lena+1):
            if a[i] >= linspace[j]:
                continue
            else:
                new_a[i]=j
                break
            
    return new_a
    
    
#basic  need trade.data in df
def is_trade_day(data, date):
    return date in data.df.index




