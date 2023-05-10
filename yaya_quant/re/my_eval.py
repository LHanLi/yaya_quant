import numpy as np
from yaya_quant.re import my_plot


# 直观展示策略净值
# 需要输入基准指数 series
def show(benchmark, netvalue, simple=False, filename=None):
    sharpe_ratio, annual_return = sharpe(netvalue, 0.03)
    drawdown_ = drawdown(netvalue)
    plt, fig, ax = my_plot.matplot()
    if simple:
        ax.text(0.7,0.05,'Sharpe:  {}'.format(round(sharpe_ratio,2)), transform=ax.transAxes)
        ax.plot(netvalue/netvalue[0], c='C0', label='p&l')
        # colors of benchmark
        colors_list = ['C4','C5','C6','C7']
        for i in range(len(benchmark.columns)):
            ax.plot(benchmark[benchmark.columns[i]]/benchmark[benchmark.columns[i]][0], 
                    c=colors_list[i], label=benchmark.columns[i])

    else: 
    #评价指标
        ax.text(0.7,0.05,'Annual Return:  {}%\nSharpe:                 {}\nMax Drawdown: {}%'.format(
            round(100*annual_return,2), round(sharpe_ratio,2), round(100*max(drawdown_),2)), transform=ax.transAxes)
        # 净值与基准
        ax.plot(netvalue/netvalue[0], c='C0', label='p&l')
        # colors of benchmark
        colors_list = ['C4','C5','C6','C7']
        for i in range(len(benchmark.columns)):
            ax.plot(benchmark[benchmark.columns[i]]/benchmark[benchmark.columns[i]][0], 
                    c=colors_list[i], label=benchmark.columns[i])
        plt.legend()
        # 回测
        ax2 = ax.twinx()
        ax2.fill_between(drawdown_.index,-100*drawdown_, 0, color='C1', alpha=0.1)
    if type(filename) == type(None):
        plt.savefig('pnl.png')
    else:
        plt.savefig(filename)


#计算评价指标，输入为日度净值的series

#sharp 
# bench为无风险收益率
# ratio and annualized return
'''def sharpe(net, bench):
    years = (net.index[-1]-net.index[0]).days/365
    return_total = net[-1]/net[0]
    return_annual = return_total**(1/years)-1
#    return_annual = (1+np.mean((net - net.shift())/net))**250-1
    std_annual = np.std((net - net.shift())/net.shift)*np.sqrt(250)
    sharpe = (return_annual - bench)/std_annual
    return sharpe, return_annual
'''
# max drawdown  input is a time series
def drawdown(net_value):
    a = np.maximum.accumulate(net_value)
    return (a-net_value)/a
    

# 接收收益率序列, bench为无风险利率
def sharpe(returns, bench):
    # 转化为对数收益率
    returns = np.log(1+returns)
    years = (returns.index[-1] - returns.index[0]).days/365
    return_total = np.exp(returns.sum())
    return_annual = return_total**(1/years)-1
    std_annual = np.exp(returns.std()*np.sqrt(250)) - 1
    sharpe = (return_annual - bench)/std_annual
    return sharpe, std_annual, return_annual




