import datetime  # For datetime objects
import pandas as pd
import backtrader as bt
import shutil
from yaya_quant.re import my_io
from yaya_quant.re import my_pd
from yaya_quant.re import my_eval
#from backtrader_plotting import Bokeh
#from backtrader_plotting.schemes import Tradimo
# 按照 create, data, strategy, run的顺序执行backtrader回测
'''
cerebro = BT_main.create()
cerebro = BT_main.data(cerebro, df_price)
cerebro = BT_main.strategy(cerebro, Strategy)
BT_main.run(cerebro)
'''
"""
对bt源代码改动：
1.  ./cerebro.py    line 1245-1248   注释掉，默认不加入trades与DataTrades。
 
2. ./observers/buysell.py    line 49  51 互换，红色买入，绿色卖出。

"""


# 创建 cerebro
def create(details=False, account=1000000.0, slippage=0.0, comm=0.0006):
    cerebro = bt.Cerebro()
#    cerebro = bt.Cerebro(stdstats=False)
#    cerebro.addobserver(bt.observers.Broker)

    # 设置交易手续费，初始资金
    cerebro.broker.setcommission(commission=comm) 
    cerebro.broker.set_slippage_perc(slippage)
    cerebro.broker.setcash(account)
    print("初始资金： %.lf"%account)
    print("双边手续费万： %.3lf"%(comm*10000))
    print("滑点万： %.lf"%(slippage*10000))

    # 当天收盘价成交!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!future function!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! 
#    cerebro.broker.set_coc(True)

   # trades 此功能在交易特别多时会非常耗时，默认添加此项
    if details: 
        cerebro.addobserver(bt.observers.Trades)
        cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='TradeAnalyzer')  #逐笔交易分析

    return cerebro


# 从market 到 全部有数据的df bt_data
# 整合多合约数据，将时间列统一，未上市补0,退市按照最后时间数据，vol为0
def integrate(df):
#    field = list(market.columns)
#    bt_data = market.reset_index()
#    # 全部需要字段
#    field = list(market.columns)
#    field_price = ['open','close','high','low']
#    # 非价格字段
#    field = list(set(field) - set(field_price))
#    bt_data = market.reset_index()
#    # 先把nan全部填充为0 否则pivot会很慢甚至失败 （包含未出现的均线都将变为0）
#    bt_data = bt_data.fillna(0)
#    # 价格数据向前填充
#    pivot_table_price = bt_data.pivot_table(field_price, 'date', 'code')
#    pivot_table_price.fillna(method='ffill', inplace=True)
#    # 其他数据不填充
#    pivot_table = bt_data.pivot_table(field, 'date', 'code')
    df_integrate = pd.DataFrame(columns=df.columns)
    date_all = df['date'].unique()
    df_date = pd.DataFrame({'date':date_all})
    df_groupby = df.groupby('code', sort=False)
    for i in df_groupby:
        df_ = i[1]
        # 最后日期
        end_date = df_['date'].max()
        end_row = df_[df_['date']==end_date]
        # 包含所有日期df
        df_date_ = df_date
        df_date_['code'] = df_['code'].values[0]
        # 将所有日期补齐，除了code外都用np.nan填充
        df_ = df_.merge(df_date_, on=['date','code'], how='outer')
        # 除了date market外，end_date后的数据填充为end_row
        for i in range(len(df_)):
            this_date = df_.loc[i]['date']
            if this_date>end_date:
                df_.loc[i] = list(end_row.values[0])
                df_.loc[i,['date','market']] = [this_date, 0]
        df_integrate = pd.concat([df_integrate, df_], ignore_index=True)
    df_integrate = df_integrate.fillna(value=0)
    return df_integrate
# 添加数据
# df_price  date, code, open, high, close, low, vol, ...
# index: datetime(e.g. pd.to_datetime(20140121))  
# 默认数据： datetime, open, high, low, close, vol, oi  
# 自动查询extra列字段添加
def from_df_get_data(df_price, extra=None, check=True):
    start = df_price.date.min()
    end = df_price.date.max()
    # 默认数据列
    default_data = ['date','open','high','low','close','vol','oi'] 
    # 获取列名所在列号,没有则返回-1
    def find(col):
        try:
            return df_price.columns.get_loc(col)
        except Exception:
            return -1
    # 如果需要添加个性数据
    if(isinstance(extra, list)):
        data_col = [find(i) for i in extra]
        class pdData(bt.feeds.PandasData):
            lines = tuple(extra)
            params = tuple(zip(extra,data_col))
        if check:
            print("个性数据所在位置： ", list(zip(extra,data_col)))
        data_col = [find(i) for i in default_data]
        if check:
            print("数据所在位置： ", list(zip(default_data,data_col)))
        data = pdData(dataname=df_price,
                                datetime = data_col[0],
                                open = data_col[1],
                                high = data_col[2],
                                low = data_col[3],
                                close = data_col[4],
                                volume = data_col[5],
                                openinterest = data_col[6],
                                fromdate = start,
                                todate = end)
    else:
        data_col = [find(i) for i in default_data]
        if check:
            print("数据所在位置： ", list(zip(default_data,data_col)))
        data = bt.feeds.PandasData(dataname=df_price,
                                datetime = data_col[0],
                                open = data_col[1],
                                high = data_col[2],
                                low = data_col[3],
                                close = data_col[4],
                                volume = data_col[5],
                                openinterest = data_col[6],
                                fromdate = start,
                                todate = end)
    return data
# 个性数据使用列表添加
def data(cerebro, df_price, extra=None):
    # 从含有单只合约的df_price中获取bt标准data格式
    code_name_list = df_price.code.unique()
    # 单只合约
    if(len(code_name_list)==1):
        code_name = code_name_list[0]
        print("合约： %s"%code_name)
        df_price.sort_values(by='date')
        data_ = from_df_get_data(df_price, extra=extra) 
        cerebro.adddata(data_,name=code_name)
    else:
        # 只有前3个个合约输出检查信息以及绘图
        for i in range(len(code_name_list)):
            code_name = code_name_list[i]
            df_ = df_price[df_price.code == code_name]
            df_ = df_.sort_values(by='date')
            if(i<5):
                print("合约： %s"%code_name)
                data_ = from_df_get_data(df_, extra=extra)
            else:
                data_ = from_df_get_data(df_, extra=extra, check=False)
                # 不进行绘图
                data_.plotinfo.plot=False
            cerebro.adddata(data_,name=code_name)
        print('%d secu added'%len(code_name_list))
    return cerebro



# 添加策略
def strategy(cerebro, Strategy, df_para=None):
    # 不适用默认参数
    if isinstance(df_para, pd.DataFrame):
        cerebro.addstrategy(Strategy, dataframe=df_para)
        # 策略说明
        print(Strategy.instruction)
        # 参数
        print('define para')
        print(df_para)
    else:
        cerebro.addstrategy(Strategy) 
        print(Strategy.instruction)
        print(Strategy.dict_params)
    return cerebro



# 进行回测，收集数据，作图
def run(cerebro, filename='test', benchmark=None, report=False, plot=False):
    #  运行回测
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    results = cerebro.run()
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue()) 
    # 收集结果
    strat = results[0] 
# 记录数据
    df_status = my_pd.combine_row(strat.df_status,'date','order_status')
    df_result = strat.df_result.merge(df_status, on=['date'], how='outer')
# 设置时间列为index
    df_result = df_result.set_index(['date'])
    df_result.index = pd.DatetimeIndex(df_result.index)
# 持仓表
    df_hold = strat.df_hold
    df_hold.index = pd.DatetimeIndex(df_hold.index)
    my_io.save_pkl((df_result,df_hold), '%s.pkl'%filename)
# log文件改名
#    shutil.move('log.txt','%s.txt'%filename)
# 比较基准
    if not isinstance(type(benchmark), type(None)):
        my_eval.show(benchmark, df_result['value'], filename=filename+".png")
# 收益曲线报告
    if report:
        returns = df_result['value']
        returns = (returns - returns.shift())/returns.shift()
        qs.reports.html(returns, output='return.html', title="return report", rf=0.03)
        shutil.move('quantstats-tearsheet.html','%s.html'%filename)
    # 画图
    if plot:
        from btplotting import BacktraderPlotting 
        p = BacktraderPlotting(style='bar', multiple_tabs=True,
                            barup='red', bardown='green',
                            barup_outline='red', bardown_outline='green',
                            barup_wick='red', bardown_wick='green',
                            filename='%s.html'%filename)
        cerebro.plot(p)



# 运行策略的一组参数,只能每个cerebro重建运行，否则会相互影响。
def batch_run(df_price, Strategy, df_paras, benchmark=None, extra=None, report=False, plot=False, account=1000000.0, slippage=0.0, comm=0.0006):
    para_names = list(df_paras.columns)
    for i in range(len(df_paras)):
        cerebro = create(account=account, slippage=slippage, comm=comm)
        cerebro = data(cerebro, df_price, extra=extra)
        df_para = pd.DataFrame(df_paras.loc[i]).T
        df_para = df_para.reset_index(drop=True)  # 必须重新设置index 否则报错
        cerebro = strategy(cerebro, Strategy, df_para)
        para_values = list(df_para.values[0])
        # 回测结果文件命名，参数名：参数值-下一个参数...
        filename = ''
        for i in range(len(para_names)):
            if i !=0:
                filename += '-'
            filename += para_names[i] + '@'  + str(para_values[i])
        run(cerebro, filename=filename, benchmark=benchmark, report=report, plot=plot)






















# BT single security 
def BT(data, Strategy, df_para=None, account=1000000.0, slippage = 0.0, 
       comm = 0.0006, code_name='test', huice_name='test', plot=False):
    # 创建 cerebro
    cerebro = bt.Cerebro(stdstats=False)
    cerebro.addobserver(bt.observers.Broker)
    cerebro.addobserver(bt.observers.Trades)
    cerebro.addobserver(bt.observers.BuySell)
    cerebro.addobserver(bt.observers.DrawDown)
#    cerebro.addobserver(bt.observers.TimeReturn)
    # 交易手续费
    cerebro.broker.setcommission(commission=comm)
#    cerebro.broker.set_slippage_fixed(slippage_fixed) #固定滑点
    cerebro.broker.set_slippage_perc(slippage)

    # 数据，账户
    cerebro.adddata(data,name=code_name)
    # Set our account cash start
    cerebro.broker.setcash(account)
    # 策略参数可以选择使用df传递
    if isinstance(df_para, pd.DataFrame):
        cerebro.addstrategy(Strategy, dataframe=df_para)
    else:
        cerebro.addstrategy(Strategy)

    # 当天收盘价成交!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!future function!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! 
#    cerebro.broker.set_coc(True)

#   评价策略指标 Analyzer
#    cerebro.addanalyzer(BacktraderPlottingLive)
#    cerebro.addanalyzer(bt.analyzers.PyFolio, _name='PyFolio')
    # 每年收益
    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='AnnualReturn')
    # 夏普比率 
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='SharpeRatio', 
                        riskfreerate=0.03, annualize=True)
#  时序收益率序列
#    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='timereturn')
    # 回撤 
#    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='DrawDown') 
    # 逐笔交易分析
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='TradeAnalyzer')
    
 #  运行回测
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    results = cerebro.run()
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
 
    # 从返回的 result 中提取回测结果
    strat = results[0]
    # 返回日度收益率序列
#    timereturn = pd.Series(strat.analyzers.timereturn.get_analysis())

    # 打印评价指标
#    print("---------------Annual Return----------------")
#    print(strat.analyzers.AnnualReturn.get_analysis())
#    print("---------------SharpeRatio-----------------")
#    print(strat.analyzers.SharpeRatio.get_analysis())
#    print("----------------DrawDown-------------------")
#    print(strat.analyzers.DrawDown.get_analysis())
#    print("---------------TradeAnalyzer----------------")
#    print(strat.analyzers.TradeAnalyzer.get_analysis()['pnl']['gross'['total']])
#    for a in strat.analyzers:
#        a.print()

    # 添加回测结果到文件 daily_return.csv
 #   f = open("%s_daily_return.csv"%huice_name,"w")
 #   f.write(timereturn)
 #   f.close()
#    timereturn.to_csv('timereturn.csv')

#    f = open("huice_log.txt", "a+")
#    f.write("%s    %s    %s    %.3f    %s \n"%(huice_name,start,end,cerebro.broker.getvalue(),strat.analyzers._SharpeRatio.get_analysis()['sharperatio']))
#    f.close() 
 
#  画图
# py3.10 https://blog.csdn.net/m0_65167078/article/details/121942610
#    cerebro.plot(numfigs = 1,iplot=True,style='candle')
    if(plot == True):
#        b = Bokeh(style='bar', plot_mode='single',scheme=Tradimo())
#        cerebro.plot(b)
        p = BacktraderPlotting(style='bar', multiple_tabs=True,
                               barup='red', bardown='green',
                               barup_outline='red', bardown_outline='green',
                               barup_wick='red', bardown_wick='green',
                               filename='plot_%s.html'%huice_name)
        cerebro.plot(p)

    return cerebro




# df_price.code 是各合约关键词
def BT_muti(df_price,Strategy,start=datetime.datetime(2021, 1, 21),end=datetime.datetime(2022,1, 21)):
    # Create a cerebro entity
    cerebro = bt.Cerebro(stdstats=False)
    cerebro.addobserver(bt.observers.Broker)
    cerebro.addobserver(bt.observers.Trades)
    cerebro.addobserver(bt.observers.BuySell)
    cerebro.addobserver(bt.observers.DrawDown)

# Add the Data Feed to Cerebro      df_price.code.unique()
    for name in df_price.code.unique():
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
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
