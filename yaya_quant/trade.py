
import numpy as np
import pandas as pd
import datetime


# my account
class Account:
    def __init__(self, cash, date,hold):    #现金, 持仓, 持仓多空情况, 日期
        self.cash = [cash]
        self.hold = [[hold]]
        self.date = [date]
    def basket(self):
        return [i.name for i in self.hold[-1]]
    def value(self,date):
        import data
        date_hold = date
        while(date_hold not in self.date):
            date_hold -= datetime.timedelta(days =1)
        where = self.date.index(date_hold)
        property_value = 0
        # if not trade day
        while(not is_trade_day(date)):
            date -= datetime.timedelta(days =1)
        for i in self.hold[where]:
            property_value += i.position * data.df[i.name]['CLOSE'].loc[date]
        cash_value = self.cash[where]
        return cash_value + property_value

# hold  name and 持有数量
class Hold:
    def __init__(self, name, position):
        self.name = name
        self.position = position

# 目标证券,买入仓位(占可用现金)
def trading(act, date, target, position):
    import data
    import copy
    account = copy.deepcopy(act)
    buy_ = 0
    delta_cash = position * data.df[target]['CLOSE'].loc[date]
    now_hold = copy.deepcopy(account.hold[-1])
    for i in now_hold:
        if(i.name == target):
            now_cash = account.cash[-1] - delta_cash
            i.position += position
            buy_ = 1
    if(buy_ == 0):
        hold = Hold(target, position)
        now_hold = copy.deepcopy(account.hold[-1])
        now_hold.append(hold)
        now_cash = account.cash[-1] - delta_cash
    if(account.date[-1] != date):
        account.hold.append(now_hold)
        account.cash.append(now_cash)
        account.date.append(date)
    else:
        account.cash[-1] = now_cash
        account.hold[-1] = now_hold
    return account

