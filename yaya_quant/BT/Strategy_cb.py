import pandas as pd
import backtrader as bt
import numpy as np
from yaya_quant.re import my_pd
from yaya_quant.re import my_io
from yaya_quant.BT import Strategy

# 针对可转债策略
 


# 以持有一揽子低价债券获得正股波动带来的收益为策略出发点
class DoubleCheap(Strategy.Model):
    # 策略说明
    instruction = "余额最小low_oi, 检查转股溢价率*100+绝对价格，等权持仓最低buy_per转债 "
    # 参数
    dict_params = dict(buy_per=0.5, low_oi=0.1)
    df_params = pd.DataFrame(dict_params, index=[0])
    params = dict(dataframe=df_params,)
    def execute(self):
        # 筛选
        # 低余额
        low_oi = [[i, i.yu_e[0]] for i in self.market_list]
        n = int(len(low_oi) * self.p.dataframe['low_oi'][0])
        low_oi.sort(key = lambda x:x[1])
        pool = low_oi[:n]
        pool = [i[0] for i in pool] 
    # 100*转股溢价率+价格最低的转债
        sort_by = [[i, i.close[0]+100*(i.close[0]*i.zhuangujia[0]/(100*i.a_close[0])-1)] for i in pool]
        sort_by.sort(key = lambda x: x[1])
    # 买入转债只数(包含已经持仓）
        n = int(len(sort_by) * self.p.dataframe['buy_per'][0])
        if n!=0:
            object_list = sort_by[:n]
            buy_list = [i[0]._name for i in object_list]
    # 每只转债目标市值
            buy_amount = self.value*0.9/n
    # 不在名单中直接清仓，在名单中的比较与目标数量的关系来判断卖出还是买入
            for secu in self.market_list:
                # 退市股已经处理过
                if secu not in self.delisting_list:
                    target_size = buy_amount/secu.close[0]
                    hold_size = self.getposition(secu).size
                    if secu._name not in buy_list:
                        # 全部卖出
                        size = hold_size
                        if size != 0:
                            order = self.sell(data=secu, size=size)
                            self.orderlist.append(order)
                            self.order_list.append(['sell', 'Market', secu._name, size])
                    elif target_size < hold_size:
                        # 卖出至target_size
                        # 向下取整 10手整数倍
                        size = self.hold_dict[secu._name] - target_size
                        size = int(size/10)*10
                        if size != 0:
                            order = self.sell(data=secu, size=size)
                            self.orderlist.append(order)
                            self.order_list.append(['sell', 'Market', secu._name, size])
                    else:
                        # 买入至target_size
                        size = target_size - hold_size
                        size = int(size/10)*10
                        # 可能不足十张或者持仓不变
                        if size != 0:
                            order = self.buy(data=secu, size=size)
                            self.orderlist.append(order)
                            self.order_list.append(['buy', 'Market', secu._name, size])


# 持有最低溢价率
class Worth(Strategy.Model):
#    interval = 5
    # 策略说明
    instruction = "余额最小low_oi, 换手率最小turnover, 检查转股溢价率*100+绝对价格，等权持仓最低buy_per转债 "
    # 参数
    dict_params = dict(buy_per=0.1, low_oi=0.95, turnover=0.95)
    df_params = pd.DataFrame(dict_params, index=[0])
    params = dict(dataframe=df_params,)
    def execute(self):
        # 筛选
        # 低余额
        low_oi = [[i, i.yu_e[0]] for i in self.market_list]
        n = int(len(self.market_list) * self.p.dataframe['low_oi'][0])
        low_oi.sort(key = lambda x:x[1])
        pool1 = low_oi[:n]
        pool1 = [i[0] for i in pool1]
        # 低换手
        low_turnover = [[i, i.vol[0]*100/i.yu_e[0]] for i in self.market_list]
        n = int(len(self.market_list) * self.p.dataframe['turnover'][0])
        pool2 = low_turnover[:n]
        pool2 = [i[0] for i in pool2]
        pool = list(set(pool1) & set(pool2))
    # 100*转股溢价率+价格最低的转债
        sort_by = [[i, 100*(i.close[0]*i.zhuangujia[0]/(100*i.a_close[0])-1)] for i in pool]
        sort_by.sort(key = lambda x: x[1])
    # 买入转债只数(包含已经持仓）
        n = int(len(sort_by) * self.p.dataframe['buy_per'][0])
        if n!=0:
            object_list = sort_by[:n]
            buy_list = [i[0]._name for i in object_list]
    # 每只转债目标市值
            buy_amount = self.value*0.9/n
    # 不在名单中直接清仓，在名单中的比较与目标数量的关系来判断卖出还是买入
            for secu in self.market_list:
                # 退市股已经处理过
                if secu not in self.delisting_list:
                    target_size = buy_amount/secu.close[0]
                    hold_size = self.getposition(secu).size
                    if secu._name not in buy_list:
                        # 全部卖出
                        size = hold_size
                        if size != 0:
                            order = self.sell(data=secu, size=size)
                            self.orderlist.append(order)
                            self.order_list.append(['sell', 'Market', secu._name, size])
                    elif target_size < hold_size:
                        # 卖出至target_size
                        # 向下取整 10手整数倍
                        size = self.hold_dict[secu._name] - target_size
                        size = int(size/10)*10
                        if size != 0:
                            order = self.sell(data=secu, size=size)
                            self.orderlist.append(order)
                            self.order_list.append(['sell', 'Market', secu._name, size])
                    else:
                        # 买入至target_size
                        size = target_size - hold_size
                        size = int(size/10)*10
                        # 可能不足十张或者持仓不变
                        if size != 0:
                            order = self.buy(data=secu, size=size)
                            self.orderlist.append(order)
                            self.order_list.append(['buy', 'Market', secu._name, size])

# 溢价率 价格  市值  满足三者均低
class TripleCheap(Strategy.Model):
    # 策略说明
    instruction = "余额最小low_oi, 换手率最小turnover, 检查转股溢价率*100+绝对价格，等权持仓最低buy_per转债 "
    # 参数
    dict_params = dict(low_oi=0.5, jiage=0.5, yijialv=0.5)
    df_params = pd.DataFrame(dict_params, index=[0])
    params = dict(dataframe=df_params,)
    def execute(self):
        # 筛选
        # 低余额
        low_oi = [[i, i.yu_e[0]] for i in self.market_list]
        n = int(len(self.market_list) * self.p.dataframe['low_oi'][0])
        low_oi.sort(key = lambda x:x[1])
        pool1 = low_oi[:n]
        pool1 = [i[0] for i in pool1]
        # 低价格
        jiage = [[i, i.close[0]] for i in self.market_list]
        n = int(len(self.market_list) * self.p.dataframe['jiage'][0])
        jiage.sort(key = lambda x:x[1])
        pool2 = low_oi[:n]
        pool2 = [i[0] for i in pool2]
        # 低溢价
        yijialv = [[i, 100*(i.close[0]*i.zhuangujia[0]/(100*i.a_close[0])-1)] for i in self.market_list]
        n = int(len(self.market_list) * self.p.dataframe['yijialv'][0])
        yijialv.sort(key = lambda x:x[1])
        pool3 = low_oi[:n]
        pool3 = [i[0] for i in pool3]
        pool = list(set(pool1) & set(pool2))
        pool = list(set(pool) & set(pool3))
    # 买入转债只数(包含已经持仓）
        n = int(len(pool))
        if n!=0:
            buy_list = [i._name for i in pool]
    # 每只转债目标市值
            buy_amount = self.value*0.9/n
    # 不在名单中直接清仓，在名单中的比较与目标数量的关系来判断卖出还是买入
            for secu in self.market_list:
                # 退市股已经处理过
                if secu not in self.delisting_list:
                    target_size = buy_amount/secu.close[0]
                    hold_size = self.getposition(secu).size
                    if secu._name not in buy_list:
                        # 全部卖出
                        size = hold_size
                        if size != 0:
                            order = self.sell(data=secu, size=size)
                            self.orderlist.append(order)
                            self.order_list.append(['sell', 'Market', secu._name, size])
                    elif target_size < hold_size:
                        # 卖出至target_size
                        # 向下取整 10手整数倍
                        size = self.hold_dict[secu._name] - target_size
                        size = int(size/10)*10
                        if size != 0:
                            order = self.sell(data=secu, size=size)
                            self.orderlist.append(order)
                            self.order_list.append(['sell', 'Market', secu._name, size])
                    else:
                        # 买入至target_size
                        size = target_size - hold_size
                        size = int(size/10)*10
                        # 可能不足十张或者持仓不变
                        if size != 0:
                            order = self.buy(data=secu, size=size)
                            self.orderlist.append(order)
                            self.order_list.append(['buy', 'Market', secu._name, size])




######################################       每日           ###############################################
class Day_DoubleCheap(Strategy.Model):
    # 策略说明
    instruction = "检查转股溢价率*100+绝对价格，等权持仓最低buy_per转债 "
    # 参数
    dict_params = dict(buy_per=0.05)
    df_params = pd.DataFrame(dict_params, index=[0])
    params = dict(dataframe=df_params,)
    def execute(self):
        pool = self.market_list
    # 100*转股溢价率+价格最低的转债
        sort_by = [[i, i.close[0]+100*(i.close[0]*i.zhuangujia[0]/(100*i.a_close[0])-1)] for i in pool]
        sort_by.sort(key = lambda x: x[1])
    # 买入转债只数(包含已经持仓）
        n = int(len(sort_by) * self.p.dataframe['buy_per'][0])
        if n!=0:
            object_list = sort_by[:n]
            buy_list = [i[0]._name for i in object_list]
    # 每只转债目标市值
            buy_amount = self.value*0.9/n
    # 不在名单中直接清仓，在名单中的比较与目标数量的关系来判断卖出还是买入
            for secu in self.market_list:
                # 退市股已经处理过
                if secu not in self.delisting_list:
                    target_size = buy_amount/secu.close[0]
                    hold_size = self.getposition(secu).size
                    if secu._name not in buy_list:
                        # 全部卖出
                        size = hold_size
                        if size != 0:
                            order = self.sell(data=secu, size=size)
                            self.orderlist.append(order)
                            self.order_list.append(['sell', 'Market', secu._name, size])
                    elif target_size < hold_size:
                        # 卖出至target_size
                        # 向下取整 10手整数倍
                        size = self.hold_dict[secu._name] - target_size
                        size = int(size/10)*10
                        if size != 0:
                            order = self.sell(data=secu, size=size)
                            self.orderlist.append(order)
                            self.order_list.append(['sell', 'Market', secu._name, size])
                    else:
                        # 买入至target_size
                        size = target_size - hold_size
                        size = int(size/10)*10
                        # 可能不足十张或者持仓不变
                        if size != 0:
                            order = self.buy(data=secu, size=size)
                            self.orderlist.append(order)
                            self.order_list.append(['buy', 'Market', secu._name, size])

class Day_Worth(Strategy.Model):
#    interval = 5
    # 策略说明
    instruction = "检查转股溢价率*100，等权持仓最低buy_per转债 "
    # 参数
    dict_params = dict(buy_per=0.05)
    df_params = pd.DataFrame(dict_params, index=[0])
    params = dict(dataframe=df_params,)
    def execute(self):
        pool = self.market_list
    # 100*转股溢价率+价格最低的转债
        sort_by = [[i, 100*(i.close[0]*i.zhuangujia[0]/(100*i.a_close[0])-1)] for i in pool]
        sort_by.sort(key = lambda x: x[1])
    # 买入转债只数(包含已经持仓）
        n = int(len(sort_by) * self.p.dataframe['buy_per'][0])
        if n!=0:
            object_list = sort_by[:n]
            buy_list = [i[0]._name for i in object_list]
    # 每只转债目标市值
            buy_amount = self.value*0.9/n
    # 不在名单中直接清仓，在名单中的比较与目标数量的关系来判断卖出还是买入
            for secu in self.market_list:
                # 退市股已经处理过
                if secu not in self.delisting_list:
                    target_size = buy_amount/secu.close[0]
                    hold_size = self.getposition(secu).size
                    if secu._name not in buy_list:
                        # 全部卖出
                        size = hold_size
                        if size != 0:
                            order = self.sell(data=secu, size=size)
                            self.orderlist.append(order)
                            self.order_list.append(['sell', 'Market', secu._name, size])
                    elif target_size < hold_size:
                        # 卖出至target_size
                        # 向下取整 10手整数倍
                        size = self.hold_dict[secu._name] - target_size
                        size = int(size/10)*10
                        if size != 0:
                            order = self.sell(data=secu, size=size)
                            self.orderlist.append(order)
                            self.order_list.append(['sell', 'Market', secu._name, size])
                    else:
                        # 买入至target_size
                        size = target_size - hold_size
                        size = int(size/10)*10
                        # 可能不足十张或者持仓不变
                        if size != 0:
                            order = self.buy(data=secu, size=size)
                            self.orderlist.append(order)
                            self.order_list.append(['buy', 'Market', secu._name, size])

######################################           每月              #######################################
class Month_DoubleCheap(Strategy.Model_Month):
    # 策略说明
    instruction = "检查转股溢价率*100+绝对价格，等权持仓最低buy_per转债 "
    # 参数
    dict_params = dict(buy_per=0.05)
    df_params = pd.DataFrame(dict_params, index=[0])
    params = dict(dataframe=df_params,)
    def execute(self):
        pool = self.market_list
    # 100*转股溢价率+价格最低的转债
        sort_by = [[i, i.close[0]+100*(i.close[0]*i.zhuangujia[0]/(100*i.a_close[0])-1)] for i in pool]
        sort_by.sort(key = lambda x: x[1])
    # 买入转债只数(包含已经持仓）
        n = int(len(sort_by) * self.p.dataframe['buy_per'][0])
        if n!=0:
            object_list = sort_by[:n]
            buy_list = [i[0]._name for i in object_list]
    # 每只转债目标市值
            buy_amount = self.value*0.9/n
    # 不在名单中直接清仓，在名单中的比较与目标数量的关系来判断卖出还是买入
            for secu in self.market_list:
                # 退市股已经处理过
                if secu not in self.delisting_list:
                    target_size = buy_amount/secu.close[0]
                    hold_size = self.getposition(secu).size
                    if secu._name not in buy_list:
                        # 全部卖出
                        size = hold_size
                        if size != 0:
                            order = self.sell(data=secu, size=size)
                            self.orderlist.append(order)
                            self.order_list.append(['sell', 'Market', secu._name, size])
                    elif target_size < hold_size:
                        # 卖出至target_size
                        # 向下取整 10手整数倍
                        size = self.hold_dict[secu._name] - target_size
                        size = int(size/10)*10
                        if size != 0:
                            order = self.sell(data=secu, size=size)
                            self.orderlist.append(order)
                            self.order_list.append(['sell', 'Market', secu._name, size])
                    else:
                        # 买入至target_size
                        size = target_size - hold_size
                        size = int(size/10)*10
                        # 可能不足十张或者持仓不变
                        if size != 0:
                            order = self.buy(data=secu, size=size)
                            self.orderlist.append(order)
                            self.order_list.append(['buy', 'Market', secu._name, size])

class Month_Worth(Strategy.Model_Month):
    # 策略说明
    instruction = "检查转股溢价率*100，等权持仓最低buy_per转债 "
    # 参数
    dict_params = dict(buy_per=0.05)
    df_params = pd.DataFrame(dict_params, index=[0])
    params = dict(dataframe=df_params,)
    def execute(self):
        pool = self.market_list
    # 100*转股溢价率最低的转债
        sort_by = [[i, 100*(i.close[0]*i.zhuangujia[0]/(100*i.a_close[0])-1)] for i in pool]
        sort_by.sort(key = lambda x: x[1])
    # 买入转债只数(包含已经持仓）
        n = int(len(sort_by) * self.p.dataframe['buy_per'][0])
        if n!=0:
            object_list = sort_by[:n]
            buy_list = [i[0]._name for i in object_list]
    # 每只转债目标市值
            buy_amount = self.value*0.9/n
    # 不在名单中直接清仓，在名单中的比较与目标数量的关系来判断卖出还是买入
            for secu in self.market_list:
                # 退市股已经处理过
                if secu not in self.delisting_list:
                    target_size = buy_amount/secu.close[0]
                    hold_size = self.getposition(secu).size
                    if secu._name not in buy_list:
                        # 全部卖出
                        size = hold_size
                        if size != 0:
                            order = self.sell(data=secu, size=size)
                            self.orderlist.append(order)
                            self.order_list.append(['sell', 'Market', secu._name, size])
                    elif target_size < hold_size:
                        # 卖出至target_size
                        # 向下取整 10手整数倍
                        size = self.hold_dict[secu._name] - target_size
                        size = int(size/10)*10
                        if size != 0:
                            order = self.sell(data=secu, size=size)
                            self.orderlist.append(order)
                            self.order_list.append(['sell', 'Market', secu._name, size])
                    else:
                        # 买入至target_size
                        size = target_size - hold_size
                        size = int(size/10)*10
                        # 可能不足十张或者持仓不变
                        if size != 0:
                            order = self.buy(data=secu, size=size)
                            self.orderlist.append(order)
                            self.order_list.append(['buy', 'Market', secu._name, size])

class Month_Yue(Strategy.Model_Month):
    # 策略说明
    instruction = "检查yue，等权持仓最低buy_per转债 "
    # 参数
    dict_params = dict(buy_per=0.05)
    df_params = pd.DataFrame(dict_params, index=[0])
    params = dict(dataframe=df_params,)
    def execute(self):
        pool = self.market_list
    # yue最低的转债
        sort_by = [[i, i.yu_e] for i in pool]
        sort_by.sort(key = lambda x: x[1])
    # 买入转债只数(包含已经持仓）
        n = int(len(sort_by) * self.p.dataframe['buy_per'][0])
        if n!=0:
            object_list = sort_by[:n]
            buy_list = [i[0]._name for i in object_list]
    # 每只转债目标市值
            buy_amount = self.value*0.9/n
    # 不在名单中直接清仓，在名单中的比较与目标数量的关系来判断卖出还是买入
            for secu in self.market_list:
                # 退市股已经处理过
                if secu not in self.delisting_list:
                    target_size = buy_amount/secu.close[0]
                    hold_size = self.getposition(secu).size
                    if secu._name not in buy_list:
                        # 全部卖出
                        size = hold_size
                        if size != 0:
                            order = self.sell(data=secu, size=size)
                            self.orderlist.append(order)
                            self.order_list.append(['sell', 'Market', secu._name, size])
                    elif target_size < hold_size:
                        # 卖出至target_size
                        # 向下取整 10手整数倍
                        size = self.hold_dict[secu._name] - target_size
                        size = int(size/10)*10
                        if size != 0:
                            order = self.sell(data=secu, size=size)
                            self.orderlist.append(order)
                            self.order_list.append(['sell', 'Market', secu._name, size])
                    else:
                        # 买入至target_size
                        size = target_size - hold_size
                        size = int(size/10)*10
                        # 可能不足十张或者持仓不变
                        if size != 0:
                            order = self.buy(data=secu, size=size)
                            self.orderlist.append(order)
                            self.order_list.append(['buy', 'Market', secu._name, size])

#####################################          每周               ########################################
class Week_DoubleCheap(Strategy.Model_Week):
    # 策略说明
    instruction = "检查转股溢价率*100+绝对价格，等权持仓最低buy_per转债 "
    # 参数
    dict_params = dict(buy_per=0.05)
    df_params = pd.DataFrame(dict_params, index=[0])
    params = dict(dataframe=df_params,)
    def execute(self):
        pool = self.market_list
    # 100*转股溢价率+价格最低的转债
        sort_by = [[i, i.close[0]+100*(i.close[0]*i.zhuangujia[0]/(100*i.a_close[0])-1)] for i in pool]
        sort_by.sort(key = lambda x: x[1])
    # 买入转债只数(包含已经持仓）
        n = int(len(sort_by) * self.p.dataframe['buy_per'][0])
        if n!=0:
            object_list = sort_by[:n]
            buy_list = [i[0]._name for i in object_list]
    # 每只转债目标市值
            buy_amount = self.value*0.9/n
    # 不在名单中直接清仓，在名单中的比较与目标数量的关系来判断卖出还是买入
            for secu in self.market_list:
                # 退市股已经处理过
                if secu not in self.delisting_list:
                    target_size = buy_amount/secu.close[0]
                    hold_size = self.getposition(secu).size
                    if secu._name not in buy_list:
                        # 全部卖出
                        size = hold_size
                        if size != 0:
                            order = self.sell(data=secu, size=size)
                            self.orderlist.append(order)
                            self.order_list.append(['sell', 'Market', secu._name, size])
                    elif target_size < hold_size:
                        # 卖出至target_size
                        # 向下取整 10手整数倍
                        size = self.hold_dict[secu._name] - target_size
                        size = int(size/10)*10
                        if size != 0:
                            order = self.sell(data=secu, size=size)
                            self.orderlist.append(order)
                            self.order_list.append(['sell', 'Market', secu._name, size])
                    else:
                        # 买入至target_size
                        size = target_size - hold_size
                        size = int(size/10)*10
                        # 可能不足十张或者持仓不变
                        if size != 0:
                            order = self.buy(data=secu, size=size)
                            self.orderlist.append(order)
                            self.order_list.append(['buy', 'Market', secu._name, size])

class Week_Worth(Strategy.Model_Week):
    # 策略说明
    instruction = "检查转股溢价率*100，等权持仓最低buy_per转债 "
    # 参数
    dict_params = dict(buy_per=0.05)
    df_params = pd.DataFrame(dict_params, index=[0])
    params = dict(dataframe=df_params,)
    def execute(self):
        pool = self.market_list
    # 100*转股溢价率最低的转债
        sort_by = [[i, 100*(i.close[0]*i.zhuangujia[0]/(100*i.a_close[0])-1)] for i in pool]
        sort_by.sort(key = lambda x: x[1])
    # 买入转债只数(包含已经持仓）
        n = int(len(sort_by) * self.p.dataframe['buy_per'][0])
        if n!=0:
            object_list = sort_by[:n]
            buy_list = [i[0]._name for i in object_list]
    # 每只转债目标市值
            buy_amount = self.value*0.9/n
    # 不在名单中直接清仓，在名单中的比较与目标数量的关系来判断卖出还是买入
            for secu in self.market_list:
                # 退市股已经处理过
                if secu not in self.delisting_list:
                    target_size = buy_amount/secu.close[0]
                    hold_size = self.getposition(secu).size
                    if secu._name not in buy_list:
                        # 全部卖出
                        size = hold_size
                        if size != 0:
                            order = self.sell(data=secu, size=size)
                            self.orderlist.append(order)
                            self.order_list.append(['sell', 'Market', secu._name, size])
                    elif target_size < hold_size:
                        # 卖出至target_size
                        # 向下取整 10手整数倍
                        size = self.hold_dict[secu._name] - target_size
                        size = int(size/10)*10
                        if size != 0:
                            order = self.sell(data=secu, size=size)
                            self.orderlist.append(order)
                            self.order_list.append(['sell', 'Market', secu._name, size])
                    else:
                        # 买入至target_size
                        size = target_size - hold_size
                        size = int(size/10)*10
                        # 可能不足十张或者持仓不变
                        if size != 0:
                            order = self.buy(data=secu, size=size)
                            self.orderlist.append(order)
                            self.order_list.append(['buy', 'Market', secu._name, size])

class Week_Yue(Strategy.Model_Week):
    # 策略说明
    instruction = "检查yue，等权持仓最低buy_per转债 "
    # 参数
    dict_params = dict(buy_per=0.05)
    df_params = pd.DataFrame(dict_params, index=[0])
    params = dict(dataframe=df_params,)
    def execute(self):
        pool = self.market_list
    # yue最低的转债
        sort_by = [[i, i.yu_e] for i in pool]
        sort_by.sort(key = lambda x: x[1])
    # 买入转债只数(包含已经持仓）
        n = int(len(sort_by) * self.p.dataframe['buy_per'][0])
        if n!=0:
            object_list = sort_by[:n]
            buy_list = [i[0]._name for i in object_list]
    # 每只转债目标市值
            buy_amount = self.value*0.9/n
    # 不在名单中直接清仓，在名单中的比较与目标数量的关系来判断卖出还是买入
            for secu in self.market_list:
                # 退市股已经处理过
                if secu not in self.delisting_list:
                    target_size = buy_amount/secu.close[0]
                    hold_size = self.getposition(secu).size
                    if secu._name not in buy_list:
                        # 全部卖出
                        size = hold_size
                        if size != 0:
                            order = self.sell(data=secu, size=size)
                            self.orderlist.append(order)
                            self.order_list.append(['sell', 'Market', secu._name, size])
                    elif target_size < hold_size:
                        # 卖出至target_size
                        # 向下取整 10手整数倍
                        size = self.hold_dict[secu._name] - target_size
                        size = int(size/10)*10
                        if size != 0:
                            order = self.sell(data=secu, size=size)
                            self.orderlist.append(order)
                            self.order_list.append(['sell', 'Market', secu._name, size])
                    else:
                        # 买入至target_size
                        size = target_size - hold_size
                        size = int(size/10)*10
                        # 可能不足十张或者持仓不变
                        if size != 0:
                            order = self.buy(data=secu, size=size)
                            self.orderlist.append(order)
                            self.order_list.append(['buy', 'Market', secu._name, size])








##########################################            单一转债 时间序列        #########################################################



# 动态平衡
# 可转债与正股应当正相关，beta比1要小且和转股溢价率负相关  
# 每日检查，比较lookback日前收盘价，正股涨而转债跌时做多转债，转债涨而正股跌时平仓，每次买入平仓均为buy_per
# 最大仓位为0.8(下单前检查，不保证一直小于9成仓位），不可做空。
class DynamicBalance(bt.Strategy):
    # 策略说明
    instruction = "每日检查，比较lookback_days日前至今转债正股价格变化(开盘与当日收盘0为当日），\
正股涨而转债跌时做多转债，转债涨而正股跌时平仓，每次买入平仓均为当前资产的buy_per,\
仓位低于0.8时操作，不可做空。"
    # 参数
    dict_params = dict(lookback_days=5, buy_per=0.1)
    df_params = pd.DataFrame(dict_params, index=[0])
    params = dict(dataframe=df_params,)
    # 输出数据df
    df_data = pd.DataFrame(columns = ['date', 'value', 'cash', 'hold', 
                                      'order'])
    df_status = pd.DataFrame(columns = ['date',  'order_status'])

    def __init__(self):
        print('__init__') 
        #记录当前交易次数
        self.trade_times = 0 
        # 相对lookback days天前涨幅
        self.p_r = self.data.close/self.data.open(-self.p.dataframe['lookback_days'][0]) 
        self.stock_r = self.data.a_close/self.data.a_open(-self.p.dataframe['lookback_days'][0])
        self.order = None
# 日志函数
    def log(self, txt, dt=None):
        ''' Logging function'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))
# 最先执行
    def start(self):
        print('start')
# 准备
    def prenext(self):
        self.log("not mature")
# every bar
    def next(self):
        # 记录数据
        date = self.datetime.date()
        value = self.broker.getvalue()
        cash = self.broker.getcash()
        hold = self.position.size
        # 这个bar提交的订单
        order_log = None

#        print(self.data.a_open[0])
        # 获取当前持仓张数
        hold_size = self.getposition().size
        # 可以买入数量
        buy_size = self.broker.getvalue()/self.data.close[0] * self.p.dataframe['buy_per'][0]
        # 最大持有数量
        max_size = self.broker.getvalue()/self.data.close[0] * 0.8

        # 转债跌而正股涨, buy 
        if self.p_r[0]<1 and self.stock_r[0]>1:
            if(hold_size<max_size):
                self.log('sub buy')
                self.buy(size=buy_size)
                order_log = ['buy', 'Market', self.data._name, buy_size]
        # 转债涨而正股跌， sell
        elif self.p_r[0]>1 and self.stock_r[0]<1:
            if(hold_size!=0):
                self.log('sub sell')
            if(hold_size>buy_size):
                self.sell(size=buy_size)
                order_log = ['sell', 'Market', self.data._name, buy_size]
            # 持仓小于卖出量则直接平仓 
            else:                           
                self.order = self.close()
                order_log = ['sell', 'Market', self.data._name, self.position.size]
        # 记录数据
        df_next = pd.DataFrame({'date':date, 'value': value, 'cash': cash, 'hold':hold,
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
    # 记录数据
        date = self.datetime.date()
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
            order_status = ['Failed']
            self.log('Order Canceled/Margin/Rejected')
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























