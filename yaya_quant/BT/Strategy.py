import datetime  # For datetime objects
import pandas as pd
# Import the backtrader platform
import backtrader as bt
import numpy as np
import talib
from yaya_quant.re import my_io 
from yaya_quant.re import my_pd

# 策略框架
# 每日(间隔interval（1）天) Model、每周五 Model_Week、每月底 Model_Month
class Model(bt.Strategy):
    interval = 1
    # 策略说明
    instruction = "策略框架，记录必要信息，处理退市、停牌等"
    # 参数
    dict_params = dict(buy_per=0.1)
    df_params = pd.DataFrame(dict_params, index=[0])
    params = dict(dataframe=df_params,)
    # 输出数据 result[0] 
    df_result = pd.DataFrame(columns = ['date', 'value', 'cash', 'order'])
    # 交易记录
    df_status = pd.DataFrame(columns = ['date',  'order_status'])
    # 记录每个日期持仓
    df_hold = pd.DataFrame()  # result[1]
    date = None
    cash = None
    value = None
    hold_dict = None
# order_list存储订单检查名  orderlist存储订单对象
    order_list = []
    orderlist = []
    market_list = []
    delisting_list = []

    def execute(self):
        print('empty execute')

    def __init__(self):
        print('__init__')
# 日志函数
    def log(self, txt, dt=None):
        ''' Logging function'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))
#        f = open('log.txt','a+')
#        f.write('%s, %s\n' % (dt.isoformat(), txt))
#        f.close()
# 最先执行
    def start(self):
        print('start')
# 准备
    def prenext(self):
        self.next()
#        self.log("not mature")
# every bar
    def next(self):
# 记录重要数据
        self.date = self.datas[0].datetime.date(0)
        self.cash = self.broker.getcash()
        self.value = self.broker.getvalue()
        self.hold_dict = dict()
#        for i in self.datas:
#            size = self.getposition(i).size
#            if size != 0:
#            self.hold_dict[i._name] = size
        name_list = [i._name for i in self.datas]
        size_list = [self.getposition(i).size for i in self.datas]
        # 添加现金
        name_list.append('cash')
        size_list.append(self.cash)
        self.hold_dict = dict(zip(name_list, size_list))
        df_ = pd.DataFrame(self.hold_dict, index=[self.date])
        self.df_hold = pd.concat([self.df_hold,df_])
# 删除掉持仓为0的值
        self.order_list = []
# 获取可交易标的列表 市的
        self.market_list = list(
            filter(
                lambda i: i.market > 0,
                self.datas))
        # 按持仓市值排序，保证先卖再买
        self.market_list.sort(key=lambda x: self.broker.getvalue([x]),
                         reverse=True)
        # 持有的停牌,退市标的
        self.delisting_list = list(filter(
            lambda i: i.volume==0 and self.getposition(i).size>0, self.datas
        ))
# 取消所有尚未执行订单
        for o in self.orderlist:
            self.cancel(o)
            self.orderlist=[]
# 清仓持有的退市、停牌转债
        for secu in self.delisting_list:
            size = self.getposition(secu).size
            if size != 0:
                order = self.sell(data=secu, size=size)
                self.log('sell delisting %s'%secu._name)
                self.orderlist.append(order)
                self.order_list.append(['sell', 'Market', secu._name, size])
        # 是间隔天 执行策略
        if len(self)%self.interval == 0:
            self.execute()
        # 记录数据
        df_next = pd.DataFrame({'date':self.date, 'value': self.value,  'cash': self.cash, 
                             'order':[self.order_list]}, index=[0])
        self.df_result = pd.concat([self.df_result, df_next])
# 结束
    def stop(self):
        self.log("death")
## 检查每张订单状态，如果失败或者执行则记录到df_status
    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return
        if order.status in [order.Completed]:
            order_status = [order.executed.size, order.executed.price, order.executed.comm]
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            order_status = ['Failed']
        self.order = None
        # 记录数据
        date = self.data.datetime.date(0)
        df_ = pd.DataFrame({'date':date, 'order_status':[order_status]}, index=[0])
        self.df_status = pd.concat([self.df_status, df_])
# 每月
class Model_Month(bt.Strategy):
    # 策略说明
    instruction = "策略框架，记录必要信息，处理退市、停牌等"
    # 参数
    dict_params = dict(buy_per=0.1)
    df_params = pd.DataFrame(dict_params, index=[0])
    params = dict(dataframe=df_params,)
    # 输出数据 result[0] 
    df_result = pd.DataFrame(columns = ['date', 'value', 'cash', 'order'])
    # 交易记录
    df_status = pd.DataFrame(columns = ['date',  'order_status'])
    # 记录每个日期持仓
    df_hold = pd.DataFrame()  # result[1]
    date = None
    cash = None
    value = None
    hold_dict = None
# order_list存储订单检查名  orderlist存储订单对象
    order_list = []
    orderlist = []
    market_list = []
    delisting_list = []

    def execute(self):
        print('empty execute')

    def __init__(self):
        print('__init__')
# 日志函数
    def log(self, txt, dt=None):
        ''' Logging function'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))
#        f = open('log.txt','a+')
#        f.write('%s, %s\n' % (dt.isoformat(), txt))
#        f.close()
# 最先执行
    def start(self):
        print('start')
# 准备
    def prenext(self):
        self.next()
#        self.log("not mature")
# every bar
    def next(self):
# 记录重要数据
        self.date = self.datas[0].datetime.date(0)
        self.cash = self.broker.getcash()
        self.value = self.broker.getvalue()
        self.hold_dict = dict()
#        for i in self.datas:
#            size = self.getposition(i).size
#            if size != 0:
#            self.hold_dict[i._name] = size
        name_list = [i._name for i in self.datas]
        size_list = [self.getposition(i).size for i in self.datas]
        # 添加现金
        name_list.append('cash')
        size_list.append(self.cash)
        self.hold_dict = dict(zip(name_list, size_list))
        df_ = pd.DataFrame(self.hold_dict, index=[self.date])
        self.df_hold = pd.concat([self.df_hold,df_])
# 删除掉持仓为0的值

        self.order_list = []
# 获取可交易标的列表 市的
        self.market_list = list(
            filter(
                lambda i: i.market > 0,
                self.datas))
        # 按持仓市值排序，保证先卖再买
        self.market_list.sort(key=lambda x: self.broker.getvalue([x]),
                         reverse=True)
        # 持有的停牌,退市标的
        self.delisting_list = list(filter(
            lambda i: i.volume==0 and self.getposition(i).size>0, self.datas
        ))
# 取消所有尚未执行订单
        for o in self.orderlist:
            self.cancel(o)
            self.orderlist=[]
# 清仓持有的退市、停牌转债
        for secu in self.delisting_list:
            size = self.getposition(secu).size
            if size != 0:
                order = self.sell(data=secu, size=size)
                self.log('sell delisting %s'%secu._name)
                self.orderlist.append(order)
                self.order_list.append(['sell', 'Market', secu._name, size])
# 每月最后一个交易日
        if len(self.datas[0]) < self.datas[0].buflen():  # 不是最后一根bar
            if self.date.month != self.datas[0].datetime.date(1).month:
        # 执行策略
                self.execute()
        # 记录数据
        df_next = pd.DataFrame({'date':self.date, 'value': self.value,  'cash': self.cash, 
                             'order':[self.order_list]}, index=[0])
        self.df_result = pd.concat([self.df_result, df_next])
# 结束
    def stop(self):
        self.log("death")
## 检查每张订单状态，如果失败或者执行则记录到df_status
    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return
        if order.status in [order.Completed]:
            order_status = [order.executed.size, order.executed.price, order.executed.comm]
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            order_status = ['Failed']
        self.order = None
        # 记录数据
        date = self.data.datetime.date(0)
        df_ = pd.DataFrame({'date':date, 'order_status':[order_status]}, index=[0])
        self.df_status = pd.concat([self.df_status, df_])
# 每周
class Model_Week(bt.Strategy):
    # 默认每周五进行处理
    i_day = 4
    # 策略说明
    instruction = "策略框架，记录必要信息，处理退市、停牌等"
    # 参数
    dict_params = dict(buy_per=0.1)
    df_params = pd.DataFrame(dict_params, index=[0])
    params = dict(dataframe=df_params,)
    # 输出数据 result[0] 
    df_result = pd.DataFrame(columns = ['date', 'value', 'cash', 'order'])
    # 交易记录
    df_status = pd.DataFrame(columns = ['date',  'order_status'])
    # 记录每个日期持仓
    df_hold = pd.DataFrame()  # result[1]
    date = None
    cash = None
    value = None
    hold_dict = None
# order_list存储订单检查名  orderlist存储订单对象
    order_list = []
    orderlist = []
    market_list = []
    delisting_list = []

    def execute(self):
        print('empty execute')

    def __init__(self):
        print('__init__')
# 日志函数
    def log(self, txt, dt=None):
        ''' Logging function'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))
#        f = open('log.txt','a+')
#        f.write('%s, %s\n' % (dt.isoformat(), txt))
#        f.close()
# 最先执行
    def start(self):
        print('start')
# 准备
    def prenext(self):
        self.next()
#        self.log("not mature")
# every bar
    def next(self):
# 记录重要数据
        self.date = self.datas[0].datetime.date(0)
        self.cash = self.broker.getcash()
        self.value = self.broker.getvalue()
        self.hold_dict = dict()
#        for i in self.datas:
#            size = self.getposition(i).size
#            if size != 0:
#            self.hold_dict[i._name] = size
        name_list = [i._name for i in self.datas]
        size_list = [self.getposition(i).size for i in self.datas]
        # 添加现金
        name_list.append('cash')
        size_list.append(self.cash)
        self.hold_dict = dict(zip(name_list, size_list))
        df_ = pd.DataFrame(self.hold_dict, index=[self.date])
        self.df_hold = pd.concat([self.df_hold,df_])
# 删除掉持仓为0的值

        self.order_list = []
# 获取可交易标的列表 市的
        self.market_list = list(
            filter(
                lambda i: i.market > 0,
                self.datas))
        # 按持仓市值排序，保证先卖再买
        self.market_list.sort(key=lambda x: self.broker.getvalue([x]),
                         reverse=True)
        # 持有的停牌,退市标的
        self.delisting_list = list(filter(
            lambda i: i.volume==0 and self.getposition(i).size>0, self.datas
        ))
# 取消所有尚未执行订单
        for o in self.orderlist:
            self.cancel(o)
            self.orderlist=[]
# 清仓持有的退市、停牌转债
        for secu in self.delisting_list:
            size = self.getposition(secu).size
            if size != 0:
                order = self.sell(data=secu, size=size)
                self.log('sell delisting %s'%secu._name)
                self.orderlist.append(order)
                self.order_list.append(['sell', 'Market', secu._name, size])
# 每周五
        if self.date.weekday() == self.i_day: 
        # 执行策略
            self.execute()
        # 记录数据
        df_next = pd.DataFrame({'date':self.date, 'value': self.value,  'cash': self.cash, 
                             'order':[self.order_list]}, index=[0])
        self.df_result = pd.concat([self.df_result, df_next])
# 结束
    def stop(self):
        self.log("death")
## 检查每张订单状态，如果失败或者执行则记录到df_status
    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return
        if order.status in [order.Completed]:
            order_status = [order.executed.size, order.executed.price, order.executed.comm]
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            order_status = ['Failed']
        self.order = None
        # 记录数据
        date = self.data.datetime.date(0)
        df_ = pd.DataFrame({'date':date, 'order_status':[order_status]}, index=[0])
        self.df_status = pd.concat([self.df_status, df_])










# 基本量价策略
# 所有策略均保持8成仓位



##########################################################################################################
############################################  动量类  ####################################################
#########################################################################################################





# 单边做多，出现金叉则一次性买入，反向信号平多单。
class SmaCross(bt.Strategy):
    instruction = "单边做多，出现金叉则一次性买入，反向信号平多单。每次买入卖出均为9成仓"
    # 参数
    dict_params = dict(period_fast=1, period_slow=5)
    df_params = pd.DataFrame(dict_params, index=[0])
    params = dict(dataframe=df_params,)
    # 输出数据df
    df_data = pd.DataFrame(columns = ['date', 'value', 'cash', 'hold', 
                                      'order'], index=[0])
    df_status = pd.DataFrame(columns = ['date',  'order_status'], index=[0])

    def __init__(self):
        # 记录交易次数
        self.trade_times = 0 
        # 移动平均线指标
        self.fast_sma = bt.ind.MovingAverageSimple(
            self.data.close, period=self.p.dataframe['period_fast'][0])
        self.slow_sma = bt.ind.MovingAverageSimple(
            self.data.close, period=self.p.dataframe['period_slow'][0])
        self.crossover = bt.ind.CrossOver(self.fast_sma, self.slow_sma) # 前一个bar<= 本bar> 则为cross over
#        self.crossover.csv = True
        self.order = None
# 日志函数
    def log(self, txt, dt=None):
        ''' Logging function fot this strategy'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))
# 准备
    def prenext(self):
        self.log("not mature")
# 最先执行
    def start(self):
        self.log("Start!")
# every bar
    def next(self):
# 记录数据
        date = self.datetime.date()
        value = self.broker.getvalue()
        cash = self.broker.getcash()
        hold = self.position.size
        # 这个bar提交的订单
        order = None
# 如果有未决订单则跳过
#        if self.order:
#            self.log('skip next')
#            return
        # 没有仓位,有信号则buy，有仓位且没有买入信号则sell 
        if not self.position:
            if self.crossover > 0:
                # 开仓时价格
                price = self.data.close[0]
                # 计算开仓张数
                size = self.broker.getvalue()/price * 0.9
                self.buy(size=size)
                self.log('buy, secu: %s, size: %.2lf, price: %.2lf'%(self.data._name, size, price))
                order = ['buy', 'Market', self.data._name, size]
        elif self.crossover < 0:
            self.order = self.close()                            
            self.log('sell, secu: %s, size: %.2lf, price: %.2lf'%(self.data._name, self.position.size, self.data.close[0]))
            order = ['sell', 'Market', self.data._name, self.position.size]
# 记录数据
        df_next = pd.DataFrame({'date':date, 'value': value, 'cash': cash, 'hold':hold,
                                'order':[order]}, index=[0])
        self.df_data = pd.concat([self.df_data, df_next])
# 结束
    def stop(self):
# 记录数据
        df_status = my_pd.combine_row(self.df_status,'date','order_status')
        df_result = self.df_data.merge(df_status, on=['date'], how='outer')
        df_result.set_index(['date'], inplace=True)
        my_io.save_pkl(df_result, 'df_bt_result.pkl')
        self.log("death")
# 一次订单
    def notify_order(self, order):
        # 记录数据
        date = self.data.datetime.date(0)
    # 提交， 接收
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return
        # Check if an order has been completed
        # Attention: broker could reject order if not enougth cash
        if order.status in [order.Completed]:
            order_status = [order.executed.size, order.executed.price, -order.executed.comm]
            if order.isbuy():
                self.log(
                    'BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                    (order.executed.price,
                        order.executed.size,
                        order.executed.comm))
            else:  # Sell
                self.log('SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                            (order.executed.price,
                            order.executed.size,
                            order.executed.comm))
            self.bar_executed = len(self)
            self.trade_times += 1
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')
            order_status = ['Failed']
        self.order = None
        #记录数据, order_status在log时记录
        df_ = pd.DataFrame({'date':date, 'order_status':[order_status]}, index=[0])
        self.df_status = pd.concat([self.df_status, df_])
# 一次交易
    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f, comm %.2f' %
                    (trade.pnl, trade.pnlcomm, trade.commission))





# 多标的SMA
# 单边做多，出现金叉则一次性买入，反向信号平多单。
class muti_Sma(bt.Strategy):
    instruction = "多股操作，单边做多，每天使用全部剩余现金等额建仓出现金叉的所有标的，卖出所有死叉标的"
    # 参数
    dict_params = dict(period_fast=1, period_slow=5, hold=20)
    df_params = pd.DataFrame(dict_params, index=[0])
    params = dict(dataframe=df_params,)
    # 输出数据df
    df_data = pd.DataFrame(columns = ['date', 'value', 'cash', 'hold', 
                                      'order'])
    df_status = pd.DataFrame(columns = ['date',  'order_status'])

    def __init__(self):
        # 记录交易次数
        self.trade_times = 0 
        # 移动平均线指标
        fastMA = {secu: bt.ind.MovingAverageSimple(secu, period=self.p.dataframe['period_fast'][0]) for secu in self.datas}
        slowMA = {secu: bt.ind.MovingAverageSimple(secu, period=self.p.dataframe['period_slow'][0]) for secu in self.datas}
        self.crossover = {secu: bt.ind.CrossOver(fastMA[secu], slowMA[secu]) for secu in self.datas}
        self.orderlist = []
# 日志函数
    def log(self, txt, dt=None):
        ''' Logging function fot this strategy'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))
# 准备
    def prenext(self):
        self.log("not mature")
# 最先执行
    def start(self):
        self.log("Start!")
# every bar
    def next(self):
# 记录数据
        date = self.datetime.date()
        value = self.broker.getvalue()
        cash = self.broker.getcash()
        hold = [self.getposition(i).size for i in self.datas]
        # 这个bar提交的订单
        order_log = []

# 取消所有尚未执行订单
        for o in self.orderlist:
            self.cancel(o)
            self.orderlist=[]

        # 当日满足买点合约名单
        buy_list = [secu for secu in self.datas if self.crossover[secu]>0]
        n_total = len(self.datas)
        n_buy = len(buy_list)
        # 如果有要新开仓
        if(n_buy!=0):
            cash = self.stats.broker.cash[0]*0.9
            buy_amount = cash/n_buy
            self.log('buy %d from %d'%(n_buy, n_total))

        # 对于每一个标的
        for secu in self.datas:
            if not self.getposition(secu):
                if self.crossover[secu] > 0:
                    # 订单信息
                    price = secu.close[0]
                    size = buy_amount/price
                    order = self.buy(data=secu, size=size)
                    self.orderlist.append(order)
                    self.log('buy, secu: %s, size: %.2lf'%(secu._name, size))
                    order_log.append(['buy','Market', secu._name, size])
            elif self.crossover[secu]<0:
                size = self.getposition(secu).size
                price = secu.close[0]
                order = self.close(data=secu, size=size)
                self.orderlist.append(order)
                self.log('sell, secu: %s, size: %.2lf'%(secu._name, size))
                order_log.append(['sell', 'Market', secu._name, size])
        # 记录数据
        print(date, value, cash, hold, order_log)
        df_next = pd.DataFrame({'date':date, 'value': value, 'cash': cash, 'hold':[hold],
                                'order':[order_log]}, index=[0])
        self.df_data = pd.concat([self.df_data, df_next]) 
# 结束
    def stop(self):
    # 记录数据
        df_status = my_pd.combine_row(self.df_status,'date','order_status')
        df_result = self.df_data.merge(df_status, on=['date'], how='outer')
        df_result.set_index(['date'], inplace=True)
        my_io.save_pkl(df_result, 'df_bt_result.pkl')
        self.log("death")
# 一次订单
    def notify_order(self, order):
    # 提交， 接收
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return
        # Check if an order has been completed
        # Attention: broker could reject order if not enougth cash
        if order.status in [order.Completed]:
            order_status = [order.executed.size, order.executed.price, -order.executed.comm]
            if order.isbuy():
                self.log(
                    'BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                    (order.executed.price,
                        order.executed.value,
                        order.executed.comm))
            else:  # Sell
                self.log('SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                            (order.executed.price,
                            order.executed.value,
                            order.executed.comm))
            self.bar_executed = len(self)
            self.trade_times += 1
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')
            order_status = ['Failed']
        self.order = None
        # 记录数据
        date = self.data.datetime.date(0)
        df_ = pd.DataFrame({'date':date, 'order_status':[order_status]}, index=[0])
        self.df_status = pd.concat([self.df_status, df_])
# 一次交易
    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f, comm %.2f' %
                    (trade.pnl, trade.pnlcomm, trade.commission))









##########################################################################################################
#############################################   估值类   #################################################
#########################################################################################################





































# single

# long = True is long, short is True is short
def MAStrategy(short_period,long_period,stoptrail,long,short,ifstop):
        # MA  短周期向上穿过长周期买入，向下卖出。 反之做空
    class Strategy(bt.Strategy):
        params = (
            ('short_period', short_period),
            ('long_period',long_period),
            ('huice',stoptrail),
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

#            self.huice = zhenfu_ind(back_period = self.params.long_period)

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

            if(long == True and short == False):
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
                    if ifstop == True:
                        self.order = self.sell(size=long_amount,exectype=bt.Order.StopTrail, trailpercent=self.params.huice)
            elif(long == False and short == True):
                if(self.sma_short < self.sma_long and self.sma_short[-1]>self.sma_long[-1]):
                    self.order = self.close()                                #先平仓
                    [self.cancel(o) for o in self.broker.orders if o.status < 4] # 取消所有未成订单 否则跟踪止损单会在之后成交
                    short_amount = self.broker.getvalue()/self.dataclose[0] * 0.9
                    self.order = self.sell(self.datas[0], size=short_amount, name=self.datas[0]._name)
                    self.log('SELL CREATE,cur hold:%.2f,cur value:%.2f, sell amount: %.2f'%(pos.size, self.broker.getvalue(),short_amount))
                    if ifstop == True:
                        self.order = self.buy(size=short_amount,exectype=bt.Order.StopTrail, trailpercent=self.params.huice)


        #        if(not pos):
                    # shuangxiangduokong
                if(self.sma_short > self.sma_long and self.sma_short[-1]<self.sma_long[-1]):
                # BUY, BUY, BUY!!! (with all possible default parameters)
                # 满仓
                    self.order = self.close()                                #先平仓
                    [self.cancel(o) for o in self.broker.orders if o.status < 4] # 取消所有未成订单 否则跟踪止损单会在之后成交
#                    long_amount = self.broker.getvalue()/self.dataclose[0] * 0.9
#                    self.order = self.buy(self.datas[0], size=long_amount, name=self.datas[0]._name)
#                    self.log('BUY CREATE, cur hold: %.2f,cur value: %.2f, buy amount: %.2f' % (pos.size,self.broker.getvalue(),long_amount))
        # 跟踪止损
        #            self.order = self.sell(size=long_amount,exectype=bt.Order.StopTrail, trailpercent=self.params.huice)
            elif(long == True and short == True):
                if(self.sma_short < self.sma_long and self.sma_short[-1]>self.sma_long[-1]):
                    self.order = self.close()                                #先平仓
                    [self.cancel(o) for o in self.broker.orders if o.status < 4] # 取消所有未成订单 否则跟踪止损单会在之后成交
                    short_amount = self.broker.getvalue()/self.dataclose[0] * 0.9
                    self.order = self.sell(self.datas[0], size=short_amount, name=self.datas[0]._name)
                    self.log('SELL CREATE,cur hold:%.2f,cur value:%.2f, sell amount: %.2f'%(pos.size, self.broker.getvalue(),short_amount))
                    if ifstop == True :
                        self.order = self.buy(size=short_amount,exectype=bt.Order.StopTrail, trailpercent=self.params.huice)


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
                    if ifstop == True:
                    # 跟踪止损
                        self.order = self.sell(size=long_amount,exectype=bt.Order.StopTrail, trailpercent=self.params.huice)
    
    
    
    # log
            self.mystats.write('%s %.3lf %.3lf\n'%(self.datas[0].datetime.date(0),self.broker.getvalue(),pos.size))


        def stop(self):
    #        self.mystats.write('%s'%self.trade_times)
            self.mystats.close()
            print("death")
            
    return Strategy

        
        
        
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
