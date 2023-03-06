import datetime  # For datetime objects
#import os.path  # To manage paths
#import sys  # To find out the script name (in argv[0])
#import pandas as pd
# Import the backtrader platform
import backtrader as bt
#import numpy as np
#import csv
#import pymysql
#from sqlalchemy import create_engine
from backtrader_plotting import Bokeh
from backtrader_plotting.schemes import Tradimo


# data prepare
#engine = create_engine('mysql+pymysql://root:a23187@127.0.0.1/financial_tpd')
#data0 =  pd.read_sql_table('commodity_index',engine,index_col='date',columns=['index_name','open','high','low','close','volume','oi'])


def Data(df_price, start=datetime.datetime(2014, 1, 21), end=datetime.datetime(2021,1, 21)):
    
    data = bt.feeds.PandasData(dataname=df_price,
                            fromdate = start,
                            todate = end
                              )
    return data


# df_price
# index: date    index_name, open, high, low, close, volume, oi
# muti
# single

# BT single security 
def BT(data, Strategy, account=1000000.0, code_name='test', huice_name='test', plot=False):
    # constant
    comm = 0.0005
    
    
    
    
    # Create a cerebro entity
    cerebro = bt.Cerebro(stdstats=False)
    cerebro.addobserver(bt.observers.Broker)
    cerebro.addobserver(bt.observers.Trades)
    cerebro.addobserver(bt.observers.BuySell)
    cerebro.addobserver(bt.observers.DrawDown)
    cerebro.addobserver(bt.observers.TimeReturn)

    # Set the commission 万5(by default) comm
    cerebro.broker.setcommission(commission=comm)



    # Add the Data Feed to Cerebro
    cerebro.adddata(data,name=code_name)

    # strategy and run
    cerebro.addstrategy(Strategy)

    # Set our account cash start
    cerebro.broker.setcash(account)

    # trade with that day close price!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!future function!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! 
#    cerebro.broker.set_coc(True)

#   评价
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='pnl') # 返回收益率时序数据
    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='_AnnualReturn') # 年化收益率
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='_SharpeRatio') # 夏普比率
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='_DrawDown') # 回撤

    
 #  运行回测
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    results = cerebro.run()
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

    # 添加回测结果到文件 daily_return.csv
    f = open("%s_daily_return.csv"%huice_name,"w")
    f.write(daily_return)
    f.close()
    
#    f = open("huice_log.txt", "a+")
#    f.write("%s    %s    %s    %.3f    %s \n"%(huice_name,start,end,cerebro.broker.getvalue(),strat.analyzers._SharpeRatio.get_analysis()['sharperatio']))
#    f.close()
    
 
#  画图
# py3.10 https://blog.csdn.net/m0_65167078/article/details/121942610
#    cerebro.plot(numfigs = 1,iplot=True,style='candle')
    if(plot == True):
        b = Bokeh(style='bar', plot_mode='single',scheme=Tradimo())
        cerebro.plot(b)
    return cerebro





def BT_muti(df_price,Strategy,start=datetime.datetime(2021, 1, 21),end=datetime.datetime(2022,1, 21)):
    import csv
        
    # Create a cerebro entity
    cerebro = bt.Cerebro(stdstats=False)
    cerebro.addobserver(bt.observers.Broker)
    cerebro.addobserver(bt.observers.Trades)
    cerebro.addobserver(bt.observers.BuySell)
    cerebro.addobserver(bt.observers.DrawDown)
    cerebro.addobserver(bt.observers.TimeReturn)

# Add the Data Feed to Cerebro      df_price.index_name.unique()
    for name in df_price.index_name.unique():
        df_ = df_price[df_price.index_name == name]
        data = bt.feeds.PandasData(dataname=df_,
                                fromdate = start,
                                todate = end
                           )
        cerebro.adddata(data,name=name)

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
    cerebro.addsizer(bt.sizers.AllInSizer,percents=90)

    # Set the commission 万5
    cerebro.broker.setcommission(commission=0.0005)

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
#    np.save('daily_return.npy',daily_return)
    # 打印评价指标
    print("--------------- AnnualReturn -----------------")
    print(strat.analyzers._AnnualReturn.get_analysis())
    print("--------------- SharpeRatio -----------------")
    print(strat.analyzers._SharpeRatio.get_analysis())
    print("--------------- DrawDown -----------------")
    print(strat.analyzers._DrawDown.get_analysis())

    # Plot the result
    # py3.10 https://blog.csdn.net/m0_65167078/article/details/121942610
#    cerebro.plot(numfigs = len(test_set),iplot=True,style='candle')
#    cerebro.plot(style = "candlestick")

#    b = Bokeh(style='bar', plot_mode='single',scheme=Tradimo())
#    cerebro.plot(b)
    return cerebro
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
