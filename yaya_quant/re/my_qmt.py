from xtquant import xtdata
from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback
from xtquant.xttype import StockAccount
from xtquant import xtconstant
import pandas as pd
import datetime, queue, time, traceback, sys, random

class MyXtQuantTraderCallback(XtQuantTraderCallback):
    def on_disconnected(self):
        """
        连接断开
        :return:
        """
        print("connection lost")
    def on_order_error(self, order_error):
        """
        委托失败推送
        :param order_error:XtOrderError 对象
        :return:
        """
        print("on order_error callback")
        print(order_error.order_id, order_error.error_id, order_error.error_msg)
    def on_cancel_error(self, cancel_error):
        """
        撤单失败推送
        :param cancel_error: XtCancelError 对象
        :return:
        """
        print("on cancel_error callback")
        print(cancel_error.order_id, cancel_error.error_id, cancel_error.error_msg)

# myorder对象 保存必要订单信息
class MyOrder():
    # sell, buy
    # int 
    def __init__(self, code, order_type, order_vol):
        self.code = code
        # 24
        if order_type == 'sell':
            self.order_type = xtconstant.STOCK_SELL
        # 23
        if order_type == 'buy':
            self.order_type = xtconstant.STOCK_BUY
        self.order_vol = order_vol

class QMT():
    # 资金账号，qmtmini路径, 账号类型
    def __init__(self, acc_num, path, acctype='STOCK'):
        self.acc_num = acc_num 
        # 连接客户端
        print("demo test")
        # 模拟端
        # session_id为会话编号，策略使用方对于不同的Python策略需要使用不同的会话编号
        session_id = 100
        self.xt_trader = XtQuantTrader(path, session_id)
        # 创建交易回调类对象，并声明接收回调
        callback = MyXtQuantTraderCallback()
        self.xt_trader.register_callback(callback)
        # 启动交易线程
        self.xt_trader.start()
        # 建立交易连接，返回0表示连接成功
        connect_result = self.xt_trader.connect()
        if connect_result != 0:
            import sys
            sys.exit('链接失败，程序即将退出 %d'%connect_result)
        else:
            print('连接成功')
        # 订阅资金账号
        self.acc = StockAccount(acc_num, acctype)
        # 对交易回调进行订阅，订阅后可以收到交易主推，返回0表示订阅成功
        subscribe_result = self.xt_trader.subscribe(self.acc)
        if subscribe_result != 0:
            print('账号订阅失败 %d'%subscribe_result)
        print(subscribe_result)
    # 重新订阅资金账号
    def account(self, acc_num, acctype='CREDIT'):
        # 订阅资金账号
        self.acc = StockAccount(acc_num, acctype)
        # 对交易回调进行订阅，订阅后可以收到交易主推，返回0表示订阅成功
        subscribe_result = self.xt_trader.subscribe(self.acc)
        if subscribe_result != 0:
            print('账号订阅失败 %d'%subscribe_result)
        print(subscribe_result)
    # 获取账户信息
    def status(self):
        # 查询证券资产
        print("query asset:")
        asset = self.xt_trader.query_stock_asset(self.acc)
        self.cur_net = asset.total_asset
        self.cur_cash = asset.cash
        if asset:
            print("总资产:{}".format(self.cur_net))
            print("现金 {0}".format(self.cur_cash))
        # 查询所有的持仓
        cur_hold_vol = {}
        cur_hold_amount = {}
        print('code           amount')
        positions = self.xt_trader.query_stock_positions(self.acc)
        for i in positions:
            if i.volume != 0:
                cur_hold_vol[i.stock_code]=i.volume 
                cur_hold_amount[i.stock_code]=i.market_value
                print(i.stock_code, '    ',i.market_value)
        self.cur_hold_vol = pd.Series(cur_hold_vol)
        self.cur_hold_amount = pd.Series(cur_hold_amount).sort_values(ascending=False)
    # 获取某标的持仓金额
    def get_code_amount(self, code):
        positions = self.xt_trader.query_stock_positions(self.acc)
        for i in positions:
            if i.stock_code == code:
                return i.market_value
        return 0
    # 获取某标的持仓数量
    def get_code_vol(self, code):
        positions = self.xt_trader.query_stock_positions(self.acc)
        for i in positions:
            if i.stock_code == code:
                return i.volume
        return 0
    # 提交myorder
    def submyorder(self, myorder, price='marketMake'):
        tick_data = xtdata.get_full_tick([myorder.code])[myorder.code]
        # 按照最新价提交
        orderPrice = tick_data['lastPrice']
        # 按买卖中间价和最新成交价的低/高者报价
        if price=='marketMake':
            mid_price = (tick_data['bidPrice'][0] + tick_data['askPrice'][0])/2
            if myorder.order_type == xtconstant.STOCK_BUY:
                # 没有委托单时mid_price为0
                if mid_price != 0:
                    orderPrice = min(orderPrice, mid_price)
            else:
                orderPrice = max(orderPrice, mid_price)
        elif price=='fast':
            if myorder.order_type == xtconstant.STOCK_BUY:
                # 卖5价格
                orderPrice = tick_data['askPrice'][4]
            else:
                # 买5价格
                orderPrice = tick_data['bidPrice'][4]
        # 订单需冻结金额
        if myorder.order_type == xtconstant.STOCK_BUY:
            cash_need = orderPrice*myorder.order_vol
        if myorder.order_type == xtconstant.STOCK_SELL:
            cash_need = 0
        # 订单需冻结股份
        if myorder.order_type == xtconstant.STOCK_BUY:
            vol_need = 0
        if myorder.order_type == xtconstant.STOCK_SELL:
            vol_need = myorder.order_vol
        # 不超过可用资金, 否则返回False，不提交此myorder
        available_cash = self.xt_trader.query_stock_asset(self.acc).cash 
        available_vol = self.get_code_vol(myorder.code)
        if (cash_need > available_cash) | (vol_need>available_vol):
            # 提交失败
            return False
        else:
            self.xt_trader.order_stock(self.acc, myorder.code, myorder.order_type, \
                                       myorder.order_vol, xtconstant.FIX_PRICE, orderPrice)
            return True
    # 由target_amount获得买卖张数
    def get_myorders_list(self, target_amount, min_vol=10):
        # 交易计划 index：code  amount：目标仓位
        # 目标手数，按照最新盘口成交价、最小成交量、当前持仓确定目标手数
        buy_vol = {}
        sell_vol = {}
        # 盘口与持仓数据
        Price = xtdata.get_full_tick(list(target_amount.keys()))
        positions = self.xt_trader.query_stock_positions(self.acc)
        cur_hold_vol = {}
        cur_hold_amount = {}
        for i in positions:
            if i.volume != 0:
                cur_hold_vol[i.stock_code]=i.volume
                cur_hold_amount[i.stock_code]=i.market_value
        cur_hold_vol = pd.Series(cur_hold_vol)
        cur_hold_amount = pd.Series(cur_hold_amount)
        # 将目标市值转化为目标手数
        for code in target_amount.keys():
            # 当天还未成交则直接略过
            if Price[code]['lastPrice'] == 0:
                lastprice = Price[code]['lastClose']
            else:
                lastprice = Price[code]['lastPrice']
            # 如果无持仓 则买入
            if code not in cur_hold_vol.index:
                vol = target_amount[code]/lastprice
                vol = vol - vol%min_vol
                vol = int(vol)
                if vol != 0:
                    buy_vol[code] = vol
            else:
                deltaamount = target_amount[code] - cur_hold_amount[code]
                # 需要买入
                if deltaamount > 0:
                    vol = deltaamount/lastprice
                    vol = vol - vol%min_vol
                    vol = int(vol)
                    if vol != 0:
                        buy_vol[code] = vol
                else:
                    # 全部卖出
                    if target_amount[code] == 0:
                        sell_vol[code] = cur_hold_vol[code]
                    else:
                        vol = -deltaamount/lastprice
                        vol = vol - vol%min_vol
                        vol = int(vol)
                        if vol != 0:
                            sell_vol[code] = vol
        return buy_vol, sell_vol
    # 由买卖张数规划订单(拆单), 最小订单张数
    def split_orders(self, buy_vol = {}, sell_vol = {}, split_vol=20):
        # 拆单 最小张数 split_vol
        myorders_list = []
        for code in buy_vol.keys():
            # 买入张数
            vol = buy_vol[code]
            # 拆单后剩余张数
            excess_vol = vol%split_vol
            # 拆成单数
            n_orders = int(vol/split_vol)
            # 最小单
            for i in range(n_orders-1):
                myorder_ = MyOrder(code, 'buy', split_vol)
                myorders_list.append(myorder_)
            # 不到最小单位不执行，到最小单位加剩余
            if n_orders>=1:
                myorder_ = MyOrder(code, 'buy', split_vol+excess_vol)
                myorders_list.append(myorder_)
        # 卖单余数要卖出
        for code in sell_vol.keys():
            vol = sell_vol[code]
            # 拆单后剩余张数
            excess_vol = vol%split_vol
            # 拆成单数
            n_orders = int(vol/split_vol)
            for i in range(n_orders-1):
                myorder_ = MyOrder(code, 'sell', split_vol)
                myorders_list.append(myorder_)
            # 卖单余数要卖出
            if n_orders>=1:
                myorder_ = MyOrder(code, 'sell', split_vol+excess_vol)
                myorders_list.append(myorder_)
            else:
                myorder_ = MyOrder(code, 'sell', excess_vol)
                myorders_list.append(myorder_)
        # 打乱订单顺序
        random.shuffle(myorders_list)
        return myorders_list 
    # 提交订单列表 默认提交订单1min, 参数‘fast'追求快速提交订单
    def sub_orderlist(self, myorders_list, sub_start, sub_dur=1):
        if len(myorders_list) == 0:
            print('empty orders')
            return 0 
        myorders_queue = queue.Queue()
        for i in myorders_list:
            myorders_queue.put(i)
        codes = [i.code for i in myorders_list]
        # 订单提交轮次等于订单数量最多的合约的订单数量
        #n_sub =pd.value_counts(codes)[0]
        n_sub = pd.Series(codes).value_counts().iloc[0]
        # 订单提交时间（分钟） 平均每秒可以提交18笔
        norders = len(myorders_list)
        if sub_dur == 'fast':
            sub_dur = norders/(18*60)
        # 提交失败订单
        fail_list = []
        # 等待到达提交时间
        while datetime.datetime.now() < sub_start:
            pass
        # 分批提交时间
        sub_end = datetime.datetime.now() + datetime.timedelta(minutes=sub_dur)
        sub_range = pd.date_range(datetime.datetime.now(), sub_end, n_sub)
        for i in sub_range:
            # 每一轮提交过的合约不再提交
            subed_codes = []
            # 剩余订单数量
            len_queue = myorders_queue.qsize()
            while datetime.datetime.now() < i:
                pass
            # 将队列中剩余订单检查一遍
            for i in range(len_queue):
                myorder = myorders_queue.get()
                # 本轮提交过的标的不再提交
                if myorder.code in subed_codes:
                    myorders_queue.put(myorder)
                else:
                    # 提交失败(现金不足）则继续提交，然后将订单放入fail_list
                    if self.submyorder(myorder, 'marketMake'):
                        subed_codes.append(myorder.code)
                    else:
                        fail_list.append(myorder)
        if myorders_queue.empty() & (len(fail_list)==0):
            print('全部订单提交成功')
        elif len(fail_list) != 0:
            print('可用资金不足失败 suborder_fail')
            self.suborder_fail = fail_list
        else:
            print('请进一步检查')

    # 下单函数
    # 撤单
    #xt_trader.cancel_order_stock(acc, order_id)
    # 买入目标金额 如果可用资金不足则直接使用可用资金买入
    def amount_buy(self, code, amount, price='lastPrice'):
        tick_data = xtdata.get_full_tick([code])[code]
        # 按照最新价提交
        orderPrice = tick_data['lastPrice']
        # 最小价差 可转债 0.001
        delta = 0.001
        if price=='marketMake':
            dadiaomaiyi = tick_data['bidPrice'][0] + delta
            orderPrice = min(orderPrice, dadiaomaiyi)
        # 不超过可用资金
        aviliable_cash = self.xt_trader.query_stock_asset(self.acc).cash 
        if aviliable_cash < amount:
            amount = aviliable_cash
        # 张数（10张整数倍）
        min_vol = 10
        vol = (amount/orderPrice)
        vol = vol - vol%min_vol
        vol = int(vol)
        if vol>0:
            print('sub %s buy order %s at %s around %s'%(vol, code, orderPrice, amount))
            return self.xt_trader.order_stock(self.acc, code, xtconstant.STOCK_BUY, vol, xtconstant.FIX_PRICE, orderPrice)
    # 卖出目标金额
    def amount_sell(self, code, amount=None, price='lastPrice'):
        tick_data = xtdata.get_full_tick([code])[code]
        # 按照最新价提交
        orderPrice = tick_data['lastPrice']
        # 最小价差 可转债 0.001
        delta = 0.001
        if price=='marketMake':
            dadiaomaiyi = tick_data['askPrice'][0] - delta
            orderPrice = max(orderPrice, dadiaomaiyi)
        # 张数（10张整数倍）
        if amount != None:
            min_vol = 10
            vol = (amount/orderPrice)
            vol = vol - vol%min_vol
            vol = int(vol)
        else:
            vol = self.get_code_vol(code)
        if vol>0:
            print('sub %s sell order %s at %s around %s'%(vol, code, orderPrice, amount))
            return self.xt_trader.order_stock(self.acc, code, xtconstant.STOCK_SELL, vol, xtconstant.FIX_PRICE, orderPrice)
    # 全部未成订单
    def badorders(self):
        orderlist = self.xt_trader.query_stock_orders(self.acc, cancelable_only=True)
        bad_orders = []
        for i in orderlist:
            # 非申购订单
            if (i.order_status != 56) & (i.order_volume<10000):
                bad_orders.append(i)
        return bad_orders
    # 取消订单
    def cancelorder(self, orders_list):
        for i in orders_list:
            self.xt_trader.cancel_order_stock(self.acc, i.order_id)
    # 重新提交订单
    def resuborders(self, orders, price='marketMaker'): 
        #for i in orders:
        #    # 取消之前订单
        #    self.xt_trader.cancel_order_stock(self.acc, i.order_id)
        # 等待5秒（确保所有订单全部取消）
        time.sleep(5)
        for i in orders:
            # 先取消次订单
            self.xt_trader.cancel_order_stock(self.acc, i.order_id)
            # 再等待2秒（避免同一合约订单快速堆积大量订单）
            time.sleep(2)
            # 订单未成部分
            vol = i.order_volume - i.traded_volume
            # 重新按照myorder提交规则提交
            if i.order_type == xtconstant.STOCK_BUY:
                myorder = MyOrder(i.stock_code, 'buy', vol)
            elif i.order_type == xtconstant.STOCK_SELL:
                myorder = MyOrder(i.stock_code, 'sell', vol)
            else:
                print('lost order')
            if self.submyorder(myorder, price):
                pass
            else:
                print('fail sub %s %s %s'%(i.order_type, i.stock_code, vol))
    # 全部持仓等权
    def rebalance(self):
        target_amount = {}
        pingjun_amount = self.cur_net/len(self.cur_hold_vol.index)
        for code in self.cur_hold_vol.index:
            target_amount[code] = pingjun_amount
        return target_amount
    # 一键清仓
    def target_amount_closeall(self):
        target_amount = {}
        positions = self.xt_trader.query_stock_positions(self.acc)
        cur_hold_vol = {}
        for i in positions:
            if i.volume != 0:
                cur_hold_vol[i.stock_code]=i.volume 
        cur_hold_vol = pd.Series(cur_hold_vol)
        for code in cur_hold_vol.index:
            target_amount[code] = 0
        return target_amount