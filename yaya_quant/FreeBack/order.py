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



class Order():

    def __init__(self, time, order_direction, _order_type):
        self.time = time
        self.order_direction = order_direction
        self.order_type = order_type




