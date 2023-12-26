import numpy as np
import pandas as pd
from scipy.stats import norm
import math


# 期权

# BS模型期权定价
# S 标的价格， K 行权价格  T 剩余到期时间（交易日）  sigma 年化波动率  r 无风险收益率 c
# all/put
def BSM(S, K, T, sigma, r=0.03, option='call'):
    # 利率全部统一到交易日
    T = T/250
    r = r*365/250
    d1 = (np.log(S/K) + (r + 0.5*sigma**2)*T)/(sigma*np.sqrt(T))
    d2 = (np.log(S/K) + (r - 0.5*sigma**2)*T)/(sigma * np.sqrt(T))

    if option == 'call':
        p = (S*norm.cdf(d1) - K*np.exp(-r*T)*norm.cdf(d2))
    elif option == 'put':
        p = (K*np.exp(-r*T)*norm.cdf(-d2) - S*norm.cdf(-d1))
    return p

# Delta
def Delta(S, K, T, sigma, r=0.03, option='call'):
    # 利率全部统一到交易日
    T = T/250
    r = r*365/250
    d1 = (np.log(S/K) + (r + 0.5*sigma**2)*T)/(sigma*np.sqrt(T))

    if option == 'call':
        delta = norm.cdf(d1)
    elif option == 'put':
        delta = norm.cdf(d1) - 1
    return delta

# Gamma
def Gamma(S, K, T, sigma, r=0.03, option='call'):
    # 利率全部统一到交易日
    T = T/250
    r = r*365/250
    d1 = (np.log(S/K) + (r + 0.5*sigma**2)*T)/(sigma*np.sqrt(T))
    d2 = (np.log(S/K) + (r - 0.5*sigma**2)*T)/(sigma * np.sqrt(T))

    gamma = (np.exp(-d1**2/2)/(np.sqrt(2*math.pi)))/(S*sigma*np.sqrt(T))
    return gamma

def Theta(S, K, T, sigma, r=0.03, option='call'):
    # 利率全部统一到交易日
    T = T/250
    r = r*365/250
    d1 = (np.log(S/K) + (r + 0.5*sigma**2)*T)/(sigma*np.sqrt(T))
    d2 = (np.log(S/K) + (r - 0.5*sigma**2)*T)/(sigma * np.sqrt(T))

    if option == 'call':
        theta = -S*sigma*(np.exp(-d1**2/2)/(np.sqrt(2*math.pi)))/(2*np.sqrt(T)) - r*K*np.exp(-r*T)*norm.cdf(d2)
    elif option == 'put':
        theta = -S*sigma*(np.exp(-d1**2/2)/(np.sqrt(2*math.pi)))/(2*np.sqrt(T)) + r*K*np.exp(-r*T)*norm.cdf(-d2)
    return theta

def Vega(S, K, T, sigma, r=0.03, option='call'):
    # 利率全部统一到交易日
    T = T/250
    r = r*365/250
    d1 = (np.log(S/K) + (r + 0.5*sigma**2)*T)/(sigma*np.sqrt(T))
    d2 = (np.log(S/K) + (r - 0.5*sigma**2)*T)/(sigma * np.sqrt(T))

    vega = S*np.sqrt(T)*np.exp(-d1**2/2)/np.sqrt(2*math.pi)  
    return vega

# 隐含波动率（可以为负值）,  P 期权价格
def IV(P,S,K,T,r=0.03, option='call'):      #从0.001 - 1.000进行二分查找
    sigma_min = 0           # 设定波动率初始最小值
    sigma_max = 100           # 设定波动率初始最大值
#    sigma_mid = (sigma_min + sigma_max) / 2
    V_min = BSM(S, K, T, sigma_min, r, option)
    V_max = BSM(S, K, T, sigma_max, r, option)
#    V_mid = BSM(S,K,sigma_mid,r,T, option)
    if P < V_min:
        print('IV less than sigma_min')
        return sigma_min                            # 隐波记为0
    elif P > V_max:
        print('IV big than sigma_max')
        return sigma_max
    # 波动率差值收敛到0.01%为止
    diff = sigma_max - sigma_min
    while abs(diff) > 0.0001:
        sigma_mid = (sigma_min + sigma_max) / 2
        V_mid = BSM(S, K, T, sigma_mid, r, option)
        # V_mid小于价格，说明sigma_mid小于隐含波动率
        if P > V_mid:
            sigma_min = sigma_mid
        else:
            sigma_max = sigma_mid
        diff = sigma_max - sigma_min
    return sigma_mid


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




