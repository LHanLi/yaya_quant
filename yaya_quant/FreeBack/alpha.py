import pandas as pd
import numpy as np
from yaya_quant.re import my_plot
from yaya_quant.re import my_eval

# 常用函数
# 相同时间，从小到大排序，均匀映射到(0,1]
def Rank(factor):
    # 因子排名
    rank = factor.groupby('date').rank()
  # normlize
    return rank/rank.groupby('date').max()

# 因子库
class Factors():
    # 因子值由market数据生成，日期为所使用数据的全部bar中最后一根
    def __init__(self, market):
        self.market = market

# 量价因子   Volume Price

# 可转债因子 Convertiable Bond    yu_e  zhuangujia dur_days  正股数据(a_***)
# 估值类因子
# 转股溢价率
    def CB_premium(self):
        factor = self.market['close'] * self.market['zhuangujia']/(self.market['a_close']) - 100
        return pd.DataFrame(factor.rename('factor'))
# 区间溢价率 在其平价附近区间内的转股溢价率排名百分比

# 收盘价
    def CB_cheap(self):
        factor = self.market['close']
        return pd.DataFrame(factor.rename('factor'))
# 双低值
    def CB_DoubleCheap(self):
        factor = self.market['close'] + self.market['close'] * self.market['zhuangujia']/(self.market['a_close']) - 100
        return pd.DataFrame(factor.rename('factor'))
# 上市时间
    def CB_durdays(self):
        factor = self.market['dur_days']
        return pd.DataFrame(factor.rename('factor'))
# 余额   yu_e 单位为亿元，表示面值100的张数对应的余额 即1yu_e为1e6张转债
    def CB_yue(self):
        factor = self.market['yu_e']
        return pd.DataFrame(factor.rename('factor'))
# 市值   
    def CB_cap(self):
        factor = self.market['yu_e']*1e6 * self.market['close']
        return pd.DataFrame(factor.rename('factor'))
# 市值与正股市值占比(与溢价率高度相关)
    def CB_capr(self):
        factor = (self.market['close']*self.market['yu_e']*1e6)/(self.market['a_free_circulation']*self.market['a_close'])
        return pd.DataFrame(factor.rename('factor'))
# 换手率
    def CB_turnover(self):
        factor = self.market['vol']/(1e6*self.market['yu_e'])
        return pd.DataFrame(factor.rename('factor'))
# 正股20日波动率 内日收益率波动
    def CB_volatility(self):
        factor = self.market['a_HV_20']
        return pd.DataFrame(factor.rename('factor'))

# 组合因子
# 双低加小市值
    def CB_alpha0(self):
        factor0 = self.CB_DoubleCheap()
        factor1 = self.CB_cap()
        return Rank(factor0)+Rank(factor1)

# 指数

# 因子投资组合
class Portfolio():
# 次日开盘价避免未来函数 df_market.pivot_table('open','date','code').shift(-1)
# holdweight 持仓权重矩阵  例如流通市值 
    def __init__(self, factor, price, holdweight=None):
        # 先按照截面排序归一化  
        self.factor = Rank(factor)
        self.price = price.fillna(method='ffill')
        if type(holdweight) != type(None):
            # 退市后即为0
            holdweight = holdweight.fillna(0)
            self.holdweight = holdweight.apply(lambda x: x/x.sum(), axis=1)
        else:
            self.holdweight = None
        # 每日收益率
        returns = self.price/self.price.shift() - 1
        returns.fillna(0)
        self.returns = returns
# 全部区间 return
# divide 
    def run(self, divide = (0, 0.2, 0.4, 0.6, 0.8, 1), periods=(1, 5, 20)):
        self.periods = periods
        # 最后一个区间为（0，1）表示等权配置收益指数
        # 如果是list则直接为a_b
        if type(divide) == type(list()):
            self.a_b = divide + [(0,1)]
        # 选取factor区间[(0,0.2),(0.2,0.4)...]
        else:
            self.a_b = [(divide[i],divide[i+1]) for i in range(len(divide)-1)] + [(0,1)]
    # 生成持仓表 -> 获得 df_contri(index date  columns code) -> 获得净值每日对数收益率 -> 获得换手率
    # 全部为矩阵操作
        self.matrix_hold()
        self.matrix_contri()
        self.matrix_lr()
        self.matrix_holdn()
        self.matrix_turnover()

# plot
# 因子组合收益（单边做多，考虑交易成本（默认单边万7））
    def HoldReturn(self, i_period, cost=7):
        plt, fig, ax = my_plot.matplot()
        ax2 = ax.twinx()
        for i in range(len(self.a_b)):
            returns = self.mat_returns[i_period][i]
#            ax.plot((1+returns).cumprod(), label=str(self.a_b[i]), alpha=0.3)
            turnover = self.mat_turnover[i_period][i]
            # 真实净值变化
            returns = (1-turnover.shift().fillna(0)*cost/10000)*(1+returns) 
            ax.plot(returns.cumprod(), label=str(self.a_b[i]))
            # 持有数量
            ax2.plot(self.mat_holdn[i_period][i], alpha=0.3)
        ax.legend()
        ax.set_title('Period: %d bar(s)'%self.periods[i_period])
        ax.set_ylabel('P&L')
        ax.set_xlabel('Date')
        ax2.set_ylabel('Number of holdings')
        plt.savefig("HoldReturn.png")
        plt.show()
# 各组对数收益率-等权对数收益率
    def LogCompare(self, i_period):
        plt, fig, ax = my_plot.matplot()
        benchmark = self.mat_lr[i_period][-1].cumsum()
        # 画图曲线颜色和透明度区分
        # 等全指数不画
        number = len(self.a_b)-1
        number0 = int(number/2)
        number1 = number - number0
        #前一半为绿色，后一半为红色 （做多因子数值高组，做空因子数值低组）
        color_list = ['C2']*number0 + ['C3']*number1
        # 颜色越靠近中心越浅
        alpha0 = (np.arange(number0)+1)[::-1]/number0
        alpha1 = (np.arange(number1)+1)/number1
        alpha_list = np.append(alpha0, alpha1)
        for i in range(number):
            log_return = self.mat_lr[i_period][i].cumsum()
            ax.plot(log_return - benchmark, label=str(self.a_b[i]) + ' turnover=%.1f'%(self.mat_turnover[i_period][i].mean()*250),
                    c=color_list[i], alpha=alpha_list[i])
        # 因子收益
        LS = (self.mat_lr[i_period][-2] - self.mat_lr[i_period][0]).cumsum()
        factor_return =  100*(np.exp(LS[-1])**(365/(LS.index[-1]-LS.index[0]).days)-1) 
        ax.plot(LS, c='C0', label='L&S  anu.{r:.2f}%'.format(r=factor_return))
        ax.legend()
        ax.set_title('Period: %d bar(s)'%self.periods[i_period])
        ax.set_ylabel('Cumulative Log Return')
        ax.set_xlabel('Date')
        plt.savefig("LogCompare.png")
        plt.show()

# mat[period][factor range]  list[factor range]
# 获得每个持仓周期 每个因子区间的 hold （虚拟持仓 只保证比例关系正确, 和为1)  a_b factor range from a to b 区间内市值等权重
    def matrix_hold(self):
    # 每个bar按因子需要的持仓
        factor = self.factor.reset_index()
        # 选取因子值 满足a_b list中全部条件的 放置于list_hold
        bar_hold = [factor[(i[0]<factor['factor']) & (factor['factor']<=i[1])] for i in self.a_b]
        # 在date没有出现的code补np.nan
        bar_hold = [i.pivot_table('factor', 'date', 'code') for i in bar_hold]
        # 非null的持仓数量为 1/price（持仓金额相等）
        bar_hold = [i.isnull() for i in bar_hold]
        bar_hold = [i.replace([True,False],[0,1])*(1/self.price) for i in bar_hold]
        bar_hold = [i.fillna(0) for i in bar_hold]
        # 当holdweight不为None时考虑此权重
        if type(self.holdweight) != type(None):
            bar_hold = [i*self.holdweight for i in bar_hold]
    # matrix hold
        mat_hold = []
        for period in self.periods:
            # 以period为周期 调整持仓的持仓表
            # 选取的index  period = 3  0,0,0,3,3,3,6...
            list_take_hold = [[hold.index[int(i/period)*period] for i in range(len(hold.index))] 
                    for hold in bar_hold]
            list_hold = [bar_hold[i].loc[list_take_hold[i]]
                    for i in range(len(bar_hold))]
            # 复原index
            for hold in list_hold:
                hold.index = bar_hold[0].index
            mat_hold.append(list_hold)
        self.mat_hold = mat_hold
# 每个时间持仓标的数量
    def matrix_holdn(self):
        mat_holdn = []
        for period_list in self.mat_hold:
            list_holdn = [(i!=0).sum(axis=1) for i in period_list]
            mat_holdn.append(list_holdn)
        self.mat_holdn = mat_holdn 
# matrix contri     每日净值对数收益率： np.log(matrix_contri[i_period][i_a_b].sum(axis=1)+1)
    def matrix_contri(self):
        mat_weight = []
        mat_contri = []
        for list_hold in self.mat_hold: 
            # 合约市值权重  不是真实的市值 
            list_cap = [hold * self.price for hold in list_hold]
            list_weight = [cap.apply(lambda x: x/x.sum(), axis=1).fillna(0) for cap in list_cap]
            # 当日收益(日weight)
            list_contri = [(weight.shift()*self.returns).fillna(0) for weight in list_weight]
            # 或者乘以次日收益
#            list_contri = [(weight*self.returns.shift(-1)).fillna(0) for weight in list_weight]
            mat_contri.append(list_contri)
            mat_weight.append(list_weight)
        self.mat_contri = mat_contri
        self.mat_weight = mat_weight
# matrix logreturn  and returns
    def matrix_lr(self):
        mat_lr = []
        mat_returns = []
        for list_contri in self.mat_contri:
            list_returns = [contri.sum(axis=1) for contri in list_contri]
            list_lr = [np.log(returns+1) for returns in list_returns]
            mat_lr.append(list_lr)
            mat_returns.append(list_returns)
        self.mat_lr = mat_lr
        self.mat_returns = mat_returns
    def matrix_turnover(self):
        mat_turnover = []
        for list_hold in self.mat_hold:
            # 持仓变化 初始期为0
            list_delta_hold = [hold - hold.shift().fillna(0) for hold in list_hold]
            # 成交量
            list_amount = [np.abs(delta_hold*self.price) for delta_hold in list_delta_hold]
            list_amount = [amount.sum(axis=1) for amount in list_amount]
            # 市值  等于持仓个数
            #list_cap = [(hold*self.price).sum(axis=1) for hold in list_hold]
            list_cap = [(hold != 0).sum(axis=1) for hold in list_hold]
            # 换手率  如果清仓则会计算为np.inf 替换为1
            list_turnover = [list_amount[i]/list_cap[i] for i in range(len(list_hold))]
            list_turnover = [i.apply(lambda x: x if x!=np.inf else 1) for i in list_turnover]
            # 年化换手率
#            list_turnover = [i.mean()*250 for i in list_turnover]
            mat_turnover.append(list_turnover)
        self.mat_turnover = mat_turnover


# 单独对某因子策略分析
# 接受assess运行后的实例
class Post():
    rf = 0.03
# which对应mat_**[,]的因子策略，benchmark 如果不加入则默认为等权组合   交易成本 默认为万7  
    def __init__(self, assess,which=(0,0), benchmark=None, cost=7):
    # 传递数据
        self.cost = cost
        self.contri = assess.mat_contri[which[0]][which[1]]
        self.returns = assess.mat_returns[which[0]][which[1]]
        self.turnover = assess.mat_turnover[which[0]][which[1]]
        self.holdn = assess.mat_holdn[which[0]][which[1]]
        # 持仓 Series
        self.hold = assess.mat_hold[which[0]][which[1]]
        s = pd.Series(dtype='float64')
        for d in self.hold.index:
            series = pd.Series({d:list(self.hold.columns[(self.hold != 0).loc[d]])})
            s = pd.concat([s,series])
        self.holdlist = s
        # 真实净值收益
        self.returns = (1-self.turnover.shift().fillna(0)*self.cost/10000)*(1+self.returns) 
        # 如果benchmark没有则默认取等权指数
        if type(benchmark) == type(None):
            self.benchmark = np.exp(assess.mat_lr[which[0]][-1].cumsum())
        else:
            self.benchmark = benchmark
    # 计算数据
    # 净值
        self.net = self.returns.cumprod()
    # 年化收益率
        years = (self.net.index[-1]-self.net.index[0]).days/365
        return_total = self.net[-1]/self.net[0]
        self.return_annual = return_total**(1/years)-1
    # 年化波动率 shrpe
        self.std_annual = np.exp(np.std(np.log(self.returns))*np.sqrt(250)) - 1
        self.sharpe = (self.return_annual - self.rf)/self.std_annual
    # 回撤
        a = np.maximum.accumulate(self.net)
        self.drawdown = (a-self.net)/a

    def pnl(self, timerange=None, filename=None):
        plt, fig, ax = my_plot.matplot()
        # 只画一段时间内净值（用于展示局部信息,只列出sharpe）
        if type(timerange) != type(None):
            # 时间段内净值与基准
            net = self.net.loc[timerange[0]:timerange[1]]
            returns = self.returns.loc[timerange[0]:timerange[1]]
            benchmark = self.benchmark.loc[timerange[0]:timerange[1]]
            # 计算夏普
            years = (timerange[1]-timerange[0]).days/365
            return_annual = (net[-1]/net[0])**(1/years)-1
            std_annual = np.exp(np.std(np.log(returns))*np.sqrt(250)) - 1 
            sharpe = (return_annual - self.rf)/std_annual
            ax.text(0.7,0.05,'Sharpe:  {}'.format(round(sharpe,2)), transform=ax.transAxes)
            ax.plot(net/net[0], c='C0', label='p&l')
        # colors of benchmark
            colors_list = ['C4','C5','C6','C7']
            for i in range(len(benchmark.columns)):
                ax.plot(benchmark[benchmark.columns[i]]/benchmark[benchmark.columns[i]][0], 
                        c=colors_list[i], label=benchmark.columns[i])
        else: 
    #评价指标
            ax.text(0.7,0.05,'Annual Return:  {}%\nSharpe:                 {}\nMax Drawdown: {}%'.format(
            round(100*self.return_annual,2), round(self.sharpe,2), round(100*max(self.drawdown),2)), transform=ax.transAxes)
        # 净值与基准
            ax.plot(self.net, c='C0', label='p&l')
        # benchmark 匹配回测时间段
            benchmark = self.benchmark.loc[self.net.index[0]:]
        # colors of benchmark
            colors_list = ['C4','C5','C6','C7']
            for i in range(len(benchmark.columns)):
                ax.plot(benchmark[benchmark.columns[i]]/benchmark[benchmark.columns[i]][0], 
                    c=colors_list[i], label=benchmark.columns[i])
            plt.legend()
            # 回撤
            ax2 = ax.twinx()
            ax2.fill_between(self.drawdown.index,-100*self.drawdown, 0, color='C1', alpha=0.1)
            ax.set_ylabel('Net')
            ax2.set_ylabel('DrawDown (%)')
        if type(filename) == type(None):
            plt.savefig('pnl.png')
        else:
            plt.savefig(filename)
        plt.show()

    def disassemble(self, timerange=None):
        # 整个时间还是一段时间内
        if type(timerange) == type(None):
            contri = self.contri
        else:
            contri = self.contri[timerange[0]:timerange[1]]
        # 各合约对组合贡献（先将contri收益率转化为对数收益率，然后按合约各自求和）
        contribution = (contri+1).prod() -1
        # 各合约持仓bars数量
        hold_bars = (contri!=0).sum()
        # 总计从n_total中选择了n个合约买入
        n_total = len(contribution)
        contribution = contribution[contribution!=0]
        n = len(contribution)
        contribution = contribution.sort_values() 
        self.contribution = contribution

        plt, fig, ax = my_plot.matplot()
        ax.plot(contribution.values*100, c='C1')
        ax.set_ylabel('Hold Return (%)')
        ax.text(0.05,0.9, 'Select %d secu from %d'%(n,n_total), transform=ax.transAxes)
        ax2 = ax.twinx()
        ax2.plot(hold_bars[contribution.index].values, alpha=0.3)
        ax2.set_ylabel('Hold Days') 
        plt.savefig('disassemble.png')
        plt.show()




