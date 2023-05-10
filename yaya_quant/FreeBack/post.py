from yaya_quant.re import my_pd
from yaya_quant.re import my_eval
import numpy as np


class Post():
    rf = 0.03
# 持仓是发出交易信号后成交的， df_market multiindex (date, code)   open, close
    def __init__(self, df_hold, df_market):
        # 去掉从未持仓过的
        self.hold = df_hold.loc[:, (df_hold != 0).any(axis=0)]
# 用以计算当日净值  包含全部code的价格 
        close = df_market.pivot_table('close', 'date', 'code')
        close.fillna(method='ffill', inplace=True)
        close['cash'] = 1
        self.close = close[self.hold.index[0]:] 
# 当日开盘价，因为持仓代表已经买入，所以买卖价格都是当日open,收益也使用此价格计算
        price = df_market.pivot_table('open', 'date', 'code')
        price.fillna(method='ffill', inplace=True)
        price['cash'] = 1
        self.price = price[self.hold.index[0]:] 
        self.returns = self.price/self.price.shift() - 1

    def run(self): 
# 组合净值（当日收盘价计算）
        self.net = (self.hold * self.close).sum(axis=1)
# 开盘价计算的当日市值
        self.df_net = self.hold * self.price
# 各资产市值占比 包含现金  
        self.weight = self.df_net.apply(lambda x: x/x.sum(), axis=1)
# 每日收益率矩阵(开盘价交易)
        # 前一日持仓才能获得当日收益 
        # 计算依据   P_{i,j}*H_{i,j} = P_{i,j}*H_{i-1,j} 换仓前后市值不变  
        self.contri = self.weight.shift()*self.returns
        # 验算
        #n_weight = ((self.weight != 0).replace([True,False],[1,0])).sum(axis=1)
        #n_contri = ((self.contri.isnull()).replace([False,True],[1,0])).sum(axis=1) 
    # 每日对数收益率
        self.lr = np.log(self.contri.sum(axis = 1)+1)
    # 各合约对组合贡献
        self.contribution =  np.exp((np.log(self.contri+1)).sum()) - 1
# 换手率
        self.get_turnover()
# 收益评价指标 以收盘净值计算
        # 默认无风险收益率 0.03
        self.sharpe, self.ar = my_eval.sharpe(self.net, self.rf)
        self.drawdown = my_eval.drawdown(self.net)

# 换手率    
    def get_turnover(self):
        # 持仓变化量
        delta_hold = self.hold - self.hold.shift()
        # 成交额
        amount = delta_hold*self.price
        amount = np.abs(amount.drop('cash', axis=1))
        amount = amount.sum(axis=1)
        # 市值
        total = self.df_net.sum(axis=1)
        # 换手率
        turnover = amount/total
        # 年化换手率
        self.turnover = turnover.mean()*250





# 从df_hold(包含cash) df_market 行情表（mutiindex (date，code)
# 按照开盘价计算净值(信号次日开盘价，持仓当日开盘价)
def hold2contri(df_hold, df_market):
# 计算计算头寸矩阵， 以日期为行， 代码为列， 包含cash
    # 次日开盘价
    df_open = df_market.pivot_table('open','date','code')
    df_open['cash'] = 1
    df_open = df_open[df_hold.columns]
# 截取需要的时间 （不能先截取，虽然速度会快，但是hold是包含全部secu的表，所以可能会找不到需要的code数据）
    df_open = df_open[df_hold.index[0]:]
# 信号次日开盘价计算净值（即持仓当日开盘价）
    df_net = df_hold*df_open
# 每个日期  全部资产权重
    df_weight = df_net.apply(lambda x: x/x.sum(), axis=1)
# 可以获得当日收益的持仓部分
    df_weight = df_weight.shift()

    # 计算每日收益率矩阵（按资产加权不能使用对数收益率！！！）
    df_return = df_open/df_open.shift() - 1
    # 当日收益率矩阵
    contri = df_weight*df_return
    return contri

# 每日 每合约对组合贡献收益率
def get_return(contri):
    return np.log(contri.sum(axis=1)+1)

# 持仓的每一合约对组合贡献的收益（注意，不能相乘或相加）
def devide_return(contri):
    return np.exp((np.log(contri+1)).sum())-1


