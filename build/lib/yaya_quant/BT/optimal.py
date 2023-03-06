import datetime  # For datetime objects
import os.path  # To manage paths
import sys  # To find out the script name (in argv[0])
import pandas as pd
# Import the backtrader platform
import backtrader as bt
import numpy as np
import csv
import pymysql
from sqlalchemy import create_engine
from backtrader_plotting import Bokeh
from backtrader_plotting.schemes import Tradimo



# data prepare
engine = create_engine('mysql+pymysql://root:a23187@127.0.0.1/financial_tpd')
data0 =  pd.read_sql_table('commodity_index',engine,index_col='date',columns=['index_name','open','high','low','close','volume','oi'])




#甲醇 [乙二醇]  【纯碱】 线性低密度聚乙烯 棕榈油 豆粕 【苯乙烯】 铁矿 焦煤 螺纹 
test_set = ['MAFI.WI','LFI.WI','PFI.WI','MFI.WI','IFI.WI','JMFI.WI','RBFI.WI']

#
nan_set = ['PMFI.WI','RIFI.WI','RSFI.WI','JRFI.WI','BBFI.WI','FBFI.WI','LRFI.WI']
new_set = ['PGFI.WI','LUFI.WI','PFFI.WI','BCFI.WI','LHFI.WI','PKFI.WI','CJFI.WI','URFI.WI','NRFI.WI','RRFI.WI','SSFI.WI','EBFI.WI','SAFI.WI','SCFI.WI','CYFI.WI','FUFI.WI','BFI.WI','WRFI.WI','APLFI.WI','SPFI.WI','EGFI.WI',]

class zhenfu_ind(bt.Indicator):
    lines = ('zhenfu',)
    def __init__(self, back_period):
        self.params.back_period = back_period
    # 这个很有用，会有 not maturity生成
        self.addminperiod(self.params.back_period)

    def next(self):
    #        print('log db next %s'%self.datas[0].datetime.date(0))
    # default is db
        total = 0
        for i in range(self.params.back_period):
            total +=  abs(self.datas[0].high[-i]-self.datas[0].low[-i])/self.datas[0].close[-i]
        zhenfu = total/self.params.back_period
        self.lines.zhenfu[0] = zhenfu

def BT(df_price,Strategy,start=datetime.datetime(2014, 1, 1),end=datetime.datetime(2022,6, 1)):
    import csv
    data = bt.feeds.PandasData(dataname=df_price,
                            fromdate = start,
                            todate = end
                              )

    # Create a cerebro entity
    cerebro = bt.Cerebro(stdstats=False)
    cerebro.addobserver(bt.observers.Broker)
    cerebro.addobserver(bt.observers.Trades)
    cerebro.addobserver(bt.observers.BuySell)
#    cerebro.addobserver(bt.observers.DrawDown)
#    cerebro.addobserver(bt.observers.TimeReturn)
    # Add the Data Feed to Cerebro
    codename='WI'
    cerebro.adddata(data,name=codename)

    # strategy and run
    cerebro.addstrategy(Strategy)
    #cerebro.optstrategy(
    #        MyStrategy,
    #        back_period=range(5,10))
    #!!!!!!!!!!!!!!!!!!!!!!!collections -> collections._collections_abc!!!!!!!!!!!!!!!!!!!!!
    #strats = cerebro.optstrategy(Strategy, maperiod=range(5, 20))

    # Set our desired cash start
    cerebro.broker.setcash(1000000.0)
    # 设置每笔交易交易的股票数量
    #cerebro.addsizer(bt.sizers.FixedSize, stake=1)
#    cerebro.addsizer(bt.sizers.AllInSizer,percents=90)

    # Set the commission
    cerebro.broker.setcommission(commission=0)

    # trade with that day close price!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!future function!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! 
    cerebro.broker.set_coc(True)

#    # pingjia 
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='pnl') # 返回收益率时序数据
    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='_AnnualReturn') # 年化收益率
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='_SharpeRatio') # 夏普比率
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='_DrawDown') # 回撤

    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    # Run over everything
    results = cerebro.run()
    # Print out the final result
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())

    # 从返回的 result 中提取回测结果
    strat = results[0]
    # 返回日度收益率序列
    daily_return = pd.Series(strat.analyzers.pnl.get_analysis())
    # 打印评价指标
    print("--------------- AnnualReturn -----------------")
    print(strat.analyzers._AnnualReturn.get_analysis())
    print("--------------- SharpeRatio -----------------")
    print(strat.analyzers._SharpeRatio.get_analysis())
    print("--------------- DrawDown -----------------")
    print(strat.analyzers._DrawDown.get_analysis())

    # log result to huice_log.csv
#    f = open("huice_log.txt", "a+")
#    f.write("%.3f     %s \n"%(cerebro.broker.getvalue(),strat.analyzers._SharpeRatio.get_analysis()['sharperatio']))
#    f.close()
    # Plot the result
    # py3.10 https://blog.csdn.net/m0_65167078/article/details/121942610
#    cerebro.plot(numfigs = 1,iplot=True,style='candle')

    
    #b = Bokeh(style='bar', plot_mode='single',scheme=Tradimo())
    #cerebro.plot(b)






def run_Strategy(short =1, long=5,df_price=data0):
    # MA  短周期向上穿过长周期买入，向下卖出。 反之做空
    class MA0Strategy(bt.Strategy):
        params = (
            ('short_period', short),
            ('long_period',long),
            ('huice',0.1),
            ('atr_period',10)
        )

        def log(self, txt, dt=None):
            ''' Logging function fot this strategy'''
            dt = dt or self.datas[0].datetime.date(0)
            print('%s, %s' % (dt.isoformat(), txt))

        def __init__(self):
            # Keep a reference to the "close" line in the data[0] dataseries
            self.dataclose = self.datas[0].close

            # To keep track of pending orders and buy price/commission
            self.order = None
            self.buyprice = None
            self.buycomm = None

            # big eneough initial value
            self.sma_short = bt.indicators.SimpleMovingAverage(self.dataclose, period=self.params.short_period)
            self.sma_long = bt.indicators.SimpleMovingAverage(self.dataclose, period=self.params.long_period)
            
            self.huice = zhenfu_ind(back_period = self.params.long_period)

        def start(self):
            print("the world call me!")
            self.mystats = open("log_huice_broker.txt", "w")
            self.mystats.write('datetime value \n')

        def prenext(self):
            print("not mature")

        def notify_order(self, order):
            if order.status in [order.Submitted, order.Accepted]:
                # Buy/Sell order submitted/accepted to/by broker - Nothing to do
                return

            # Check if an order has been completed
            # Attention: broker could reject order if not enougth cash
            if order.status in [order.Completed]:
                if order.isbuy():
                    self.log(
                        'BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                        (order.executed.price,
                         order.executed.value,
                         order.executed.comm))

                    self.buyprice = order.executed.price
                    self.buycomm = order.executed.comm
                else:  # Sell
                    self.log('SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                             (order.executed.price,
                              order.executed.value,
                              order.executed.comm))

                self.bar_executed = len(self)

            elif order.status in [order.Canceled, order.Margin, order.Rejected]:
                self.log('Order Canceled/Margin/Rejected')

            self.order = None

        def notify_trade(self, trade):
            if not trade.isclosed:
                return

            self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' %
                     (trade.pnl, trade.pnlcomm))

        def next(self):
            # Simply log the closing price of the series from the reference
    #        self.log('Open, %.2f Close, %.2f low, %.2f  high, %.2f' %(self.datas[0].open[0],self.datas[0].close[0],self.datas[0].low[0],self.datas[0].high[0]))

            # Check if an order is pending ... if yes, we cannot send a 2nd one
            if self.order:
                return

            pos = self.getposition()

            if(self.sma_short > self.sma_long and self.sma_short[-1]<self.sma_long[-1]):
                self.order = self.close()                                #先平仓
                long_amount = self.broker.getvalue()/self.dataclose[0]  
                self.order = self.buy(self.datas[0], size=long_amount, name=self.datas[0]._name)
                self.log('BUY CREATE,cur hold: %.2f,cur value: %.2f, buy amount: %.2f' % (pos.size,self.broker.getvalue(),long_amount))
    # 跟踪止损
                self.order = self.sell(exectype=bt.Order.StopTrail, trailpercent=self.params.huice)
            if(self.sma_short < self.sma_long and self.sma_short[-1]>self.sma_long[-1]):
                self.order = self.close()                                #先平仓
                short_amount = self.broker.getvalue()/self.dataclose[0] 
                self.order = self.sell(self.datas[0], size=short_amount, name=self.datas[0]._name)
                self.log('SELL CREATE,cur hold:%.2f,cur value:%.2f, sell amount: %.2f'%(pos.size, self.broker.getvalue(),short_amount))
                self.order = self.buy(exectype=bt.Order.StopTrail, trailpercent=self.params.huice)

            self.mystats.write('%s %.3lf \n'%(self.datas[0].datetime.date(0),self.broker.getvalue()))
                
        def stop(self):
            self.mystats.close()
            print("death")
        
    BT(df_price,MA0Strategy)


# 4 + 3 + 2 + 1 = 10组
period_dict = {1:[5,10,20,60],5:[10,20,60],10:[20,60],20:[60]}
# every secu
#for i in data0.index_name.unique():
for i in test_set:
#    if(i not in nan_set and i not in new_set):
    df_price = data0[data0['index_name']==i]
    # every time period
    for short in [1,5,10,20]:
        for long in period_dict[short]:
            run_Strategy(short,long,df_price) 
            os.remove("%s_%s_%s.txt"%(i,short,long))
            os.rename("log_huice_broker.txt","%s_%s_%s.txt"%(i,short,long))
