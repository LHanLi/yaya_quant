import pandas as pd




# Word回测在此对象中进行，
# 拥有Account，Market， Broker
class World:
    def __init__(self, market, name='FreeBack'):
        self.name = name
# market: dataframe  mutiindex  (date, code)
        self.market = market
# 时间线，回测将严格按照时间线推进
        self.dateline = market.index.get_level_values(0)
# 设置账户（添加df_hold属性）
        codes = list(self.market.index.get_level_values(1).unique())
        codes.append('cash')
        self.df_hold = pd.DataFrame(columns=codes)

    def add_broker(self, broker):
        self.broker = broker
    
    def add_strategy(self, strategy):
        self.strategy = strategy

    def run(self):
        self.strategy
        















