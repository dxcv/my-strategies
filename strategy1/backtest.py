# backtest.py用于对策略进行回测，由main调用
# 创建者：季俊男
# 创建日期：2018/11/27

import pymysql
import numpy as np
import pandas as pd
import datetime as dtt
from database import Data

class Order(object):
    """本类用于描述指令单对象，包括买卖方向，买卖品种，买卖数量"""
    def __init__(self, time, symbol, volume, is_buy):
        """初始化指令单，time表示指令单时间， symbol表示品种代码，volume表示数量，is_buy表示买卖方向"""
        self.time = time
        self.symbol = symbol
        self.volume = volume
        self.is_buy = is_buy


def fee(order:Order):
    return 0.003 * order.volume


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

    def get_position_price(self, ps:dict, dt1, dt2):
        """根据持仓从数据库中提取相应的价格数据,以矩阵形式返回结果，行表示品种，列表示日期，参数ps表示字典，键为品种，值
        为持仓量，dt1表示开始日期，dt2表示结束日期，价格序列包含开始日期而不包含结束日期"""
        symbols = ps.keys()
        price_list = []
        sql = r"""select dirty from tb_sec where dt >= %s and dt < %s and code0 = %s"""
        for symbol in symbols:
            price_list.append(Data(sql, self.cur, (dt1, dt2, symbol)).select_col(0))
        prices = np.matrix(price_list, dtype=float).T
        dts = Data("select dt from tb_sec where dt >= %s and dt < %s", self.cur, (dt1, dt2))
        return prices, dts


class Position(object):
    """本类用于记录持仓信息，接受order与相应的市场价格自动计算账户的现金以及持仓，用字典记录持仓，
    用DataFrame存储cash与持仓"""
    def __init__(self, cash, ps, time, market:MarketData):
        """利用初始持仓cash与ps初始化，cash表示初始现金，ps是字典形式的初始持仓，单位为张，time为时间"""
        self.cash = cash
        self.ps = ps.copy()
        self.time = time
        self.position = pd.DataFrame([[cash, ps]], pd.to_datetime([time]), columns=["cash", "ps"])
        self.market = market

    def get_order(self, order:Order):
        """接受指令单，自动生成交易指令执行后的持仓数据，写入position"""
        dirty = self.market.get_order_price(order)
        if self.time > order.time:
            raise ValueError("新的指令单时间{}应当晚于上一次指令单时间{}".format(order.time, self.time))
        if order.is_buy:
            self.cash -= (dirty * order.volume + fee(order))
            if self.cash < 0:
                raise ValueError("账户现金不够,指令单：{}".format(order.time))
            else:
                try:
                    self.ps[order.symbol] += order.volume
                except KeyError as e:
                    self.ps[order.symbol] = order.volume
                self.time = order.time
                self.position = self.position.append(pd.DataFrame([[self.cash, self.ps.copy()]],
                                                                  pd.to_datetime([self.time]), columns=["cash", "ps"]))
                # print("指令成交")
        else:
            if order.symbol not in self.ps or order.volume > self.ps[order.symbol]:
                raise ValueError("{}无可用持仓或持仓不足，指令时间：{}".format(order.symbol, order.time))
            else:
                self.cash += (dirty * order.volume - fee(order))
                self.ps[order.symbol] -= order.volume
                if self.ps[order.symbol] == 0:
                    del self.ps[order.symbol]
                self.time = order.time
                self.position = self.position.append(pd.DataFrame([[self.cash, self.ps.copy()]],
                                                                  pd.to_datetime([self.time]), columns=["cash", "ps"]))
                # print("指令成交")

    def get_asset_value(self):
        """根据self.position计算连续时间上的账户资产价值，包括现金（cash), 其他资产（asset)及其总和(total),结果以
        DataFrame形式呈现"""
        dts = self.position.keys()
        for i in range(len(dts)-1):
            if dts[i] == dts[i+1]:
                continue
            else:
                ps = self.position.iloc[i, 1]
                cash = self.position.iloc[i, 0]
                prices, dt = self.market.get_position_price(ps, dts[i], dts[i+1])
                volumes = np.matrix(list(ps.values()), dtype=float)
                asset_matrix = np.dot(volumes, prices)
                cash_matrix = cash * np.ones(asset_matrix.shape)















if __name__ == "__main__":
    db = pymysql.connect("localhost", "root", "root", "strategy1")
    cur = db.cursor()
    market = MarketData(cur)
    position = Position(100000000, {}, dtt.date(2014, 1, 16), market)
    # order1 = Order(dtt.date(2014, 1, 20), "070205.IB", 100000, True)
    # order2 = Order(dtt.date(2014, 1, 27), "070205.IB", 100000, True)
    # order3 =  Order(dtt.date(2014, 1, 29), "070205.IB", 100000, False)
    # order4 = Order(dtt.date(2014, 2, 7), "140201.IB", 100000, False)
    # order5 = Order(dtt.date(2014, 1, 30), "070205.IB", 100000, False)
    # position.get_order(order1)
    # position.get_order(order2)
    # position.get_order(order3)
    # position.get_order(order5)




