import numpy as np




#sharp ratio
def sharpe(time_series, bench, period):
#   Ep = (time_series[-1] - time_series[-period+1])/time_series[- period+1]
    change = [(time_series[i] - time_series[i-1])/time_series[i-1] for i in reversed(range(len(time_series) - period,len(time_series)))]
    change = np.array(change)
    sigma = change.std()
    Ep = change.mean()* (250**0.5)
    return (Ep - bench)/sigma

# max drawdown  input is a time series
def drawdown(net_value):
    a = np.maximum.accumulate(net_value)
    return max((a-net_value)/a)
    
# annualized rate of return    an account and its end_date
def arr(account, end_date):
    start_date = account.date[0]
    dur = (end_date - start_date).days
    end_value = account.value(end_date)
    start_value = account.value(start_date)
    r = end_value / start_value
    return r**(250/dur) -1
