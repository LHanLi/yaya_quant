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



# single

# MA  短周期向上穿过长周期买入，向下卖出。 反之做空
class MA0Strategy(bt.Strategy):
    params = (
        ('short_period', 1),
        ('long_period',20),
        ('huice',0.04),
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
        self.trade_times = 0

        # big eneough initial value
        self.sma_short = bt.indicators.SimpleMovingAverage(self.dataclose, period=self.params.short_period)
        self.sma_long = bt.indicators.SimpleMovingAverage(self.dataclose, period=self.params.long_period)
        
        self.huice = zhenfu_ind(back_period = self.params.long_period)
        
    def start(self):
        print("the world call me!")
        self.mystats = open("log_huice_broker.txt", "w")
        self.mystats.write('datetime value hold\n')

    def prenext(self):
        print("not mature")

    def _cancel(self,oref):
        order = self.order[oref]
        order.cancel(self)
        order.cancel()
        self.notify_order(order)
        
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
            self.trade_times += 1

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
        
        if(pos == 0):
            [self.cancel(o) for o in self.broker.orders if o.status < 4] # 取消所有未成订单 否则跟踪止损单会在之后成交
        
        if(self.sma_short < self.sma_long and self.sma_short[-1]>self.sma_long[-1]):
            self.order = self.close()                                #先平仓
            [self.cancel(o) for o in self.broker.orders if o.status < 4] # 取消所有未成订单 否则跟踪止损单会在之后成交
#            short_amount = self.broker.getvalue()/self.dataclose[0] * 0.9
#            self.order = self.sell(self.datas[0], size=short_amount, name=self.datas[0]._name)
#            self.log('SELL CREATE,cur hold:%.2f,cur value:%.2f, sell amount: %.2f'%(pos.size, self.broker.getvalue(),short_amount))
#            self.order = self.buy(size=short_amount,exectype=bt.Order.StopTrail, trailpercent=self.params.huice)

        
#        if(not pos):
            # shuangxiangduokong
        if(self.sma_short > self.sma_long and self.sma_short[-1]<self.sma_long[-1]):
        # BUY, BUY, BUY!!! (with all possible default parameters)
        # 满仓
            self.order = self.close()                                #先平仓
            [self.cancel(o) for o in self.broker.orders if o.status < 4] # 取消所有未成订单 否则跟踪止损单会在之后成交
            long_amount = self.broker.getvalue()/self.dataclose[0] * 0.9
            self.order = self.buy(self.datas[0], size=long_amount, name=self.datas[0]._name)
            self.log('BUY CREATE, cur hold: %.2f,cur value: %.2f, buy amount: %.2f' % (pos.size,self.broker.getvalue(),long_amount))
# 跟踪止损
            self.order = self.sell(size=long_amount,exectype=bt.Order.StopTrail, trailpercent=self.params.huice)

# log
        self.mystats.write('%s %.3lf %.3lf\n'%(self.datas[0].datetime.date(0),self.broker.getvalue(),pos.size))


    def stop(self):
#        self.mystats.write('%s'%self.trade_times)
        self.mystats.close()
        print("death")

        
        
        
# DB shuangxiang
class DBStrategy(bt.Strategy):
    params = (
        ('back_period', 20),
        ('db_period',3),
        ('huiche',0.05)
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
        self.preopen = 1000000
        self.lowest = 1000000
        self.preday = 0
        self.find = True
        
        self.r_preopen = 0
        self.r_highest = 0
        self.r_preday = 0
        self.r_find = True
        
    def start(self):
        print("the world call me!")
 
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
        
        # yinxian
        if self.datas[0].close[0]<self.datas[0].open[0]:
            # new yinxian
            if self.find == True:
                self.lowest = self.datas[0].close[0]
                self.preopen = self.datas[0].open[0]
                self.preday = len(self)
                self.find = False
            # update lowest
            elif self.datas[0].close[0] < self.lowest:
                self.lowest = self.datas[0].close[0]
                self.preopen = self.datas[0].open[0]
                self.preday = len(self)
        
        #yangxian
        if self.datas[0].close[0]>self.datas[0].open[0]:
            # new yangxian
            if self.r_find == True:
                self.r_highest = self.datas[0].close[0]
                self.r_preopen = self.datas[0].open[0]
                self.r_preday = len(self)
                self.r_find = False
            # update highest
            elif self.datas[0].close[0] > self.r_highest:
                self.r_highest = self.datas[0].close[0]
                self.r_preopen = self.datas[0].open[0]
                self.r_preday = len(self)
        
#        self.log('highest: %.2f, preopen: %.2f, find: %s'%(self.r_highest,self.r_preopen,self.r_find))
        pos = self.getposition()
        # long
        # surpass preopen
        if max(self.datas[0].close[0],self.datas[0].open[0])>self.preopen and self.find==False and not self.position:
            # chaoguo db_period  and yinxian zai back_period zhi nei
            if len(self)>(self.preday + self.params.db_period) and len(self) < (self.preday + self.params.back_period):
            # Keep track of the created order to avoid a 2nd order
#                    self.order = self.buy(exectype=bt.Order.Limit,price=self.preopen,valid=self.datas[0].datetime.date(self.params.db_period))
                self.order = self.buy()
                self.log('Buy CREATE, hold:%.2f, value:%.2f'%(pos.size, self.broker.getvalue()))
                self.order = self.sell(exectype=bt.Order.StopTrail, trailpercent=self.params.huiche)
            # chong xin xun zhao xin yin xian
            self.find = True
            
        # short
        # lowwer than preopen
        if min(self.datas[0].close[0],self.datas[0].open[0])<self.r_preopen and self.r_find==False and not self.position:
            # chaoguo db_period  and yinxian zai back_period zhi nei
            if len(self)>(self.r_preday + self.params.db_period) and len(self) < (self.r_preday + self.params.back_period):
                self.order = self.sell()
                self.log('SELL CREATE, hold:%.2f, value:%.2f'%(pos.size, self.broker.getvalue()))
                # Keep track of the created order to avoid a 2nd order
#                    self.order = self.buy(exectype=bt.Order.Limit,price=self.preopen,valid=self.datas[0].datetime.date(self.params.db_period))
                self.order = self.buy(exectype=bt.Order.StopTrail, trailpercent=self.params.huiche)
            # chong xin xun zhao xin yin xian
            self.r_find = True

    def stop(self):
        print("death")
        
        
        
        
        
 # mutiway
# Cross_mom  前几日涨幅排序，做多做空排名靠前和靠后品种
class Cross_mom_Strategy(bt.Strategy):
    params = (
        ('back_period', 20),
        ('precent',3)
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

        
    def start(self):
        print('begin')
        self.mystats = csv.writer(open("log_huice_broker.csv", "w"))
        self.mystats.writerow(['datetime',
                               'value','hold'])
 
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
        
        #前back_period天收盘价涨幅排序
        sort_value = [[i,self.datas[i].close[0] - self.datas[i].close[-self.params.back_period]] for i in range(len(self.datas))]
#        sort_std = [ for i in range(len(self.datas))]
        sort_value.sort(key = lambda x: x[1])
        sort_value = np.array(sort_value)
        sort = sort_value[:,0]
        
        
        #获取持仓
        long_bond_name = []
        short_bond_name = []
        for _p in self.broker.positions:
            if self.broker.getposition(_p).size > 0:
                long_bond_name.append([_p._name,self.broker.getposition(_p).size])
            if self.broker.getposition(_p).size < 0:
                short_bond_name.append([_p._name,self.broker.getposition(_p).size])
        
#做空做做多前后n
#       n = int(self.params.precent*len(self.datas))
        n = int(self.params.precent)
        will_long_bond_name = [self.datas[int(i)]._name for i in sort[:n]]
        will_short_bond_name = [self.datas[int(i)]._name for i in sort[-n:]]
        
        #每隔 back_period 天进行平衡
        if(len(self)%self.params.back_period == 0):
#            self.order = self.close()
#q平仓不在新开仓中的品种
            for i in sort:
                if self.datas[int(i)]._name in will_long_bond_name:
                    pass
                if self.datas[int(i)]._name in will_short_bond_name:
                    pass
                self.order_target_percent(self.datas[int(i)],0,name = self.datas[int(i)]._name)
                    
            for i in range(n):
                self.order = self.order_target_percent(self.datas[int(sort[i])],1/(2*n), name=self.datas[int(sort[i])]._name)
                self.order = self.order_target_percent(self.datas[int(sort[-i-1])],-1/(2*n), name=self.datas[int(sort[-i-1])]._name)
                self.log('will long: %s, will short: %s'%(self.datas[int(sort[i])]._name,self.datas[int(sort[-i-1])]._name))
            self.log('Trade,cur long: %s,cur short: %s, cur value:%.2f'%(long_bond_name,short_bond_name,self.broker.getvalue()))
            # self.order = self.order_target_percent(self.datas[0], 0.98, name=self.datas[0]._name)

        # log
        self.mystats.writerow([self.data.datetime.date(-1).strftime('%Y-%m-%d'),
                                 '%.4f' % self.stats.broker.value[0],
                              'cur long: %s, cur short: %s'%(long_bond_name,short_bond_name)]) 

            
    def stop(self):
        print("death")
        self.mystats.writerow([self.data.datetime.date(0).strftime('%Y-%m-%d'),
                               '%.4f' % self.stats.broker.value[0]])
