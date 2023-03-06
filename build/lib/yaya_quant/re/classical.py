import numpy as np
import pandas as pd
import datetime


# signal
# MA          time_series: np.array
def MA_array(time_series, period):
    ma = []
    for i in range(len(time_series)):
        if(i-period+1>=0):
            ma.append(time_series[i-period+1:i+1].mean())   #period include now
        else:
            ma.append(time_series[i])
    ma = np.array(ma)
    return ma
    
def MA(time_series, period):
    if(len(time_series)-period>=0):
            ma = time_series[-period:].mean()   #period include now
    return ma



# adjust ATR
def atr(h,l,c,period):
    h = h[-period-1:]
    l = l[-period-1:]
    c = c[-period-1:]
    TR = [max(max(h[i+1]-l[i+1],np.abs(c[i]-h[i+1])),np.abs(c[i]-l[i+1])) for i in range(period)]
    TR = np.array(TR)
    return TR.mean()/c[-1]
    


