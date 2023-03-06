import numpy as np




#sharp ratio and annualized return
def sharpe(net, bench):
    change = np.array([(net[i] - net[i-1])/net[i-1] for i in range(len(net))])
    sigma = change.std()*np.sqrt(250)
    dur = len(net)
    r =  net[-1]/net[0]
    Ep = r**(250/dur) -1
    return (Ep - bench)/sigma, sigma, Ep

# max drawdown  input is a time series
def drawdown(net_value):
    a = np.maximum.accumulate(net_value)
    return max((a-net_value)/a)
    
