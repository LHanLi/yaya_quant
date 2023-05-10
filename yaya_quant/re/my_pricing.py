import numpy as np
import pandas as pd
from scipy.stats import norm


# 期权


# spot price, strike price, time to deal(交易日), annual volaticlity, rate, call/put
def BSM(S, K, T, sigma, r=0.03, option='call'):
    # 波动率，利率全部统一到交易日
    T = T/250
    r = r*365/250
    d1 = (np.log(S/K) + (r + 0.5*sigma**2)*T)/(sigma*np.sqrt(T))
    d2 = (np.log(S/K) + (r - 0.5*sigma**2)*T)/(sigma * np.sqrt(T))

    if option == 'call':
        p = (S*norm.cdf(d1) - K*np.exp(-r*T)*norm.cdf(d2))
    elif option == 'put':
        p = (K*np.exp(-r*T)*norm.cdf(-d2) - S*norm.cdf(-d1))
    return p



# 债券

# 每年现金流
#bond_return = [0.003,0.005,0.01,0.015,0.02,1.15]
# 每年现金流， 剩余年限， 无风险利率
def bond(bond_return, t, r=0.03):
    P = 0
    bond_return = copy.copy(bond_return)
    bond_return.reverse()
    # 向上取整，
    T = int(t+0.99)
    for i in range(T):
        P += bond_return[i]*100/(1+r)**(t-i) 
    return P



