import backtrader as bt




# ATR
class tr_ind(bt.Indicator):
    lines = ('tr',)
    def __init__(self, back_period):
        self.params.back_period = back_period
        # 这个很有用，会有 not maturity生成
        self.addminperiod(self.params.back_period)

    def next(self):
#        print('log db next %s'%self.datas[0].datetime.date(0))
    # default is db
        self.lines.tr[0] = max(self.datas[0].high[0]-self.datas[0].low[0],self.datas[0].high[0]-self.datas[0].close[-1],self.datas[0].close[-1]-self.datas[0].low[0])/self.datas[0].close[0]


# 
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
        

# Cross 前back period 涨幅 排序列表
class Cross_sort_ind(bt.Indicator):
    lines = ('sort',)
    def __init__(self,back_period):
        self.params.back_period = back_period
        # 这个很有用，会有 not maturity生成
        self.addminperiod(self.params.back_period)
    
    def next(self):
#        sort_value = [[i,self.datas[0].close[0] - self.datas[0].close[-self.params.back_period]] for i in range(4)]
#        sort_value.sort(lambda x,y:cmp(x[1],y[1]))
        sort_value = [self.datas[0].close[0]-self.datas[0].close[-self.params.back_period]]
#        self.lines.sort = sort_value[:,0]
        self.lines.sort[0] = sort_value
        
        
# 上涨突破前最低收盘价阴线的开盘价且不是相邻两天时，或者相反。
class db_ind(bt.Indicator):
    lines = ('db',)
    def __init__(self, back_period, db_period):
        self.params.back_period = back_period
        self.params.db_period = db_period
        # 这个很有用，会有 not maturity生成
        self.addminperiod(self.params.back_period)

    def next(self):
#        print('log db next %s'%self.datas[0].datetime.date(0))
    # default is db
        self.lines.db[0] = 0
        #阳线
        if self.data.close[0] > self.data.open[0]:
            # high enough  记录最低阴线close价
            yinclose = self.data.close[0]
            for i in range(self.params.back_period):
                #hulue yangxian
                if(self.data.close[-i] > self.data.open[-i]):
                    yangclose = self.data.close[-i]
                    continue
                else:
                    # ib
                    if(self.data.open[-i]<yangclose):
                        self.lines.db[0] = 0
                        break
                    # lowest close price day's open price lowwer than close[0]
                    if(self.data.close[-i]<yinclose):
                        yinclose = self.data.close[-i]
                        if(self.data.open[-i]<self.data.close[0] and i>self.params.db_period):
#                            print('db %s %s'%(self.datas[0].datetime.date(0), self.datas[0].datetime.date(-i)))
                            self.lines.db[0] = 1
                            break
        elif self.data.close[0] < self.data.open[0]:
            # 记录最高阳线close价
            yangclose = self.data.close[0]
            for i in range(self.params.back_period):
                #hulue yinxian
                if(self.data.close[-i] < self.data.open[-i]):
                    yinclose = self.data.close[-i]
                    continue
                else:
                    # ib
                    if(self.data.open[-i]>yinclose):
                        self.lines.db[0] = 0
                        break
                    # highest close price day's open price higher than close[0]
                    if(self.data.close[-i]>yangclose):
                        yangclose = self.data.close[-i]
                        if(self.data.open[-i]>self.data.close[0] and i>self.params.db_period):
#                            print('db %s %s'%(self.datas[0].datetime.date(0), self.datas[0].datetime.date(-i)))
                            self.lines.db[0] = -1
                            break
        else:
            self.lines.db[0] = 0
