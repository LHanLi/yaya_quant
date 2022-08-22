import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from bokeh.plotting import figure, output_notebook, show ,save
import matplotlib.pyplot as plt
import copy
from yaya_quant.BT import main
from yaya_quant.BT import Strategy
from yaya_quant.re import eval as quant_eval

'''
# read heyue data
starttime = pd._libs.tslibs.timestamps.Timestamp('2014-06-13 00:00:00')
engine = create_engine('mysql+pymysql://root:a23187@127.0.0.1/THS')
df_heyue =  pd.read_sql_table('trading_daily_price_1',engine,columns=['date','code','open','high','low','close','volume','open_interest'])
df_heyue = df_heyue[df_heyue.date>starttime]

# 提取主力合约直接连接
secu_A = 'I'
main_list = ['01','05','09']
heyue_total = df_heyue[df_heyue.code.apply(lambda x: secu_A == ''.join([i for i in x[:2] if i.isalpha()]))]
'''


# 显示每年的每一时间对应主力合约 真实主连
def show_main(heyue_total):
    max_oi = heyue_total[['date','open_interest']].groupby(by = 'date', as_index = False).max()
    I_main = pd.merge(max_oi,heyue_total, on = ['date','open_interest'], how = 'left')
    
    year_list = list(set([str(i.date())[:4] for i in I_main.date]))
    year_list.sort()
    fig,ax = plt.subplots(len(year_set),1)
    # ax index
    i = 0
    for year in year_list:
        select = [year in str(i) for i in I_main.date]
        main_temp = copy.copy(I_main[select])
        main_temp.index = main_temp.date
        del main_temp['date']
        ax[i].plot(main_temp.close, color='C0',label='main')
        ax[i].axes.yaxis.set_ticks([])
        ax[i].get_xaxis().set_visible(False)
        ax[i].set_ylabel(year)
        ax2 = ax[i].twinx()
        ax2.scatter(main_temp.code.apply(lambda x: x.split('.')[0][-2:]).sort_values().index,
                    main_temp.code.apply(lambda x: x.split('.')[0][-2:]).sort_values().map(lambda x: int(x)).values,
                    color='C1',s=1,ls='--',label='I_main yuefen')
        i += 1
    #ax.legend(loc = 'upper left')
    #ax2.legend(loc = 'upper right')
    #ax.set_title('I_main')
    #lt.savefig('I_main.png')
    plt.show()
    
   

# 按交割月前月10日换月，给出raw和复权指数  
# return 0 is heyue_hold, return 1 is reset
def get_main_heyue_hold(heyue_total,main_list):
    # get hold df
    def qianyue10ri(yuefen):
        if yuefen == '01':
            return "12-10"
        else:
            qianyue = float(yuefen) - 1
            return "%02d-10"%(qianyue)
    # hold what in specific date
    hold = [main_list[2] if str(i).split(' ')[0][-5:] < qianyue10ri(main_list[2]) and  str(i).split(' ')[0][-5:] > qianyue10ri(main_list[1])
            else main_list[1] if str(i).split(' ')[0][-5:] < qianyue10ri(main_list[1]) and str(i).split(' ')[0][-5:] > qianyue10ri(main_list[0])
            else main_list[0]
           for i in heyue_I.date]

    #  heyue_hold is finially trade secu
    code_yuefen = [i.split('.')[0][-2:] for i in heyue_total.code]
    heyue_hold = heyue_total[np.array(hold) == np.array(code_yuefen)]
    heyue_hold.index = heyue_hold.date
    del heyue_hold['date']

    # 将跳空复权
    heyue_hold_reset = copy.copy(heyue_hold)
    # first secu
    code = heyue_hold_reset.code.iloc[0]
    i = 0
    convert = 1
    while i < heyue_hold_reset.shape[0]-1:
    #    heyue_hold_reset.iloc[i].open = heyue_hold_reset.iloc[i].open * convert  will not change the value
        heyue_hold_reset.iloc[i,1] = heyue_hold_reset.iloc[i,1] * convert
        heyue_hold_reset.iloc[i,2] = heyue_hold_reset.iloc[i,2] * convert
        heyue_hold_reset.iloc[i,3] = heyue_hold_reset.iloc[i,3] * convert
        heyue_hold_reset.iloc[i,4] = heyue_hold_reset.iloc[i,4] * convert
        # 如果下一次循环换月，则改变convert
        if heyue_hold_reset.iloc[i+1].code != heyue_hold_reset.iloc[i].code:
            # reset by close price
            convert = heyue_hold_reset.iloc[i].close/heyue_hold_reset.iloc[i+1].close
        i += 1
    # last day
    heyue_hold_reset.iloc[i,1] = heyue_hold_reset.iloc[i,1] * convert
    heyue_hold_reset.iloc[i,2] = heyue_hold_reset.iloc[i,2] * convert
    heyue_hold_reset.iloc[i,3] = heyue_hold_reset.iloc[i,3] * convert
    heyue_hold_reset.iloc[i,4] = heyue_hold_reset.iloc[i,4] * convert
    
    return heyue_hold,heyue_hold_reset
  

  
  
# backtrader in every secu and store result in result_dict
# heyue_I is all secu
def backtrader_allsecu(Strategy,heyue_total,main_list):
    def is_main(str_):
        if str_ == main_list[0] or str_ == main_list[1] or str_ == main_list[2]:
            return True
        else:
            return False

    # secu need to backtrade
    heyue_back = heyue_total.code.unique()[[is_main(i.split('.')[0][-2:]) for i in heyue_total.code.unique()]]

    # store result  key is secu code
    result_dict = dict()
    for secu in heyue_back:
        df_price = heyue_total[heyue_total.code == secu]
        df_price.index = df_price.date
        main.BT(df_price,Strategy,df_price.date.iloc[0],df_price.date.iloc[-1],secu)
        df = pd.read_csv('log_huice_broker.txt',sep=r'\s+',index_col=0,parse_dates=True)
        result_dict[secu] = df
    
    return result_dict
  

  
# reset celue net value, need heyue_hold
def reset_celue_value(result_dict,heyue_hold):
    #策略原始净值
    net_raw_list = []
    # hold 张数
    hold_list = []
    for i in range(heyue_hold.shape[0]):
        # 从result_dict 中提取对应合约、日期的策略净值
        try:
            value = result_dict[heyue_hold.code.iloc[i]].loc[str(heyue_hold.index[i].date())].value
            hold = result_dict[heyue_hold.code.iloc[i]].loc[str(heyue_hold.index[i].date())].hold
        except:
            value = 1000000
            hold = 0
        net_raw_list.append(value)
        hold_list.append(hold)
    
    net_raw_list = np.array(net_raw_list)
    hold_list = np.array(hold_list)
    print(hold_list)
    # 策略净值前复权
    # 换月手续费未考虑
    i = 0
    convert = 1
    #while i<heyue_hold.shape[0]-1:
    while i < heyue_hold.shape[0]-1:
        net_raw_list[i] = net_raw_list[i] * convert
        # 如果下一次循环换月，则改变convert
        if heyue_hold.iloc[i+1].code != heyue_hold.iloc[i].code:
            convert = net_raw_list[i]/net_raw_list[i+1]
        i += 1
    net_raw_list[i] = net_raw_list[i] * convert
    # set first value equal to heyue_hold.close's first value
    celue_return = net_raw_list * (heyue_hold.close.iloc[0]/1000000)
    heyue_hold['net']  = celue_return
    heyue_hold['hold'] = hold_list
    return heyue_hold
