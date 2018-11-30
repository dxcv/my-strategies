# backtest.py用于对策略进行回测，由main调用
# 创建者：季俊男
# 创建日期：2018/11/27

import pymysql
import numpy as np
import pandas as pd
import datetime as dtt


class Order(object):
    """本类用于描述指令单对象，包括买卖方向，买卖品种，买卖数量"""
    def __init__(self, time, symbol, volume, is_buy):
        """初始化指令单，time表示指令单时间， symbol表示品种代码，volume表示数量，is_buy表示买卖方向"""
        self.time = time
        self.symbol = symbol
        self.volume = volume
        self.is_buy = is_buy


class MarketData(object):
    """用于获取指令单以及持仓的市场价格数据"""
    def __init__(self, cur:pymysql.cursors.Cursor):
        self.cur = cur

    def get_order_price(self, order:Order, field="dirty"):
        """从数据库中提取订单产品的价格数据，其中order表示订单，field表示价格类型，即数据库表的字段名，默认为债券全价"""
        sql = r"select {} from tb_sec where dt = %s and code0 = %s".format(field)
        _ = self.cur.execute(sql, (order.time, order.symbol))
        price = self.cur.fetchone()[0]
        return price

    def get_position_price(self, ps):
        pass


class Position(object):
    """本类用于记录持仓信息，接受order与相应的市场价格自动计算账户的现金以及持仓，用字典记录持仓，
    用DataFrame存储cash与持仓"""
    def __init__(self, cash, ps, time, market:MarketData):
        """利用初始持仓cash与ps初始化，cash表示现金，ps是字典形式的持仓，单位为张，time为时间"""
        self.cash = cash
        self.ps = ps
        self.time = time
        self.position = pd.DataFrame([[cash, ps]], [time], ["cash", "ps"])
        self.market = market

    def get_order(self, order:Order):
        """接受指令单，自动生成交易指令执行后的持仓数据，写入position"""
        dirty = self.market.get_order_price(order)
        if self.time <= order.time:
            self.time = order.time
        else:
            raise ValueError("新的指令单时间{}应当晚于上一次指令单时间{}".format(order.time, self.time))
        if order.is_buy:
            self.cash -= 1.00003 * self.market.get_order_price(order) * order.volume
            if self.cash < 0:
                raise ValueError("账户现金不够,指令单：{}".format(order.time))
            try:
                self.ps[order.symbol] += order.volume
            except TypeError as e:
                self.ps[order.symbol] = order.volume

