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
        self.symbol = time
        self.volume = time
        self.is_buy = is_buy


class MarketData(object):
    def __init__(self, cur:pymysql.cursors.Cursor):
        self.cur = cur

    def get_order_price(self, order:Order, field="dirty"):
        """从数据库中提取订单产品的价格数据，其中order表示订单，field表示价格类型，即数据库表的字段名，默认为债券全价"""
