# 本文件用于创建MySQL数据库
# 季俊男

import pymysql
import win32com.client as win32
import pywintypes
from WindPy import w
import math
import datetime as dtt
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import scipy.optimize as optimize
import os, sys


class Data(object):
    """本类用于从mysql中提取相应条件的数据"""
    def __init__(self, sql, cur, args=None):
        self.sql = sql
        self.cur = cur
        self.args = args
        self.data = Data.get_data(self)

    def __str__(self):
        return str(self.data)

    __repr__ = __str__

    def get_data(self):
        _ = self.cur.execute(self.sql, self.args)
        data = self.cur.fetchall()
        return data

    def select_col(self, col):
        return [d[col] for d in self.data]


class BondYTM(object):
    """本类用于计算续发固定利率附息国债到期收益率"""
    def __init__(self, terms, rate, dt0, par=100, freq=1):
        """类初始化函数，terms代表债券年限，rate表示发行利率，dt0表示发行日期，par表示债券面值，默认100
        freq表示一年附息频次，默认为1"""
        self.terms = terms
        self.rate = rate
        self.dt0 = dt0
        self.par = par
        self.freq = freq

    def get_ts(self, dt:dtt.date):
        dt_delta_days = (dt - self.dt0).days
        year_days = 365
        t0 = round(dt_delta_days * self.freq / year_days, 2)
        ts = [i+1-t0 for i in range(self.terms*self.freq)]
        return ts

    def bond_ytm(self, dt, price, guess=0.03):
        ts = self.get_ts(dt)
        coup = self.par * self.rate / self.freq
        ytm_func = lambda y: sum([coup/(1+y/self.freq)**t for t in ts])+self.par/(1+y/self.freq)**ts[-1]-price
        fprime = lambda y: sum([-t * coup / self.freq * (1 + y / self.freq) ** (t + 1) \
                                for t in ts]) - ts[-1] * self.par / self.freq * (1 + y / self.freq) ** (ts[-1] + 1)
        fprime2 = lambda y: sum([(t + 1) * t * coup / self.freq ** 2 * (1 + y / self.freq) ** (t + 2) for t in ts])\
                            + (ts[-1] + 1) * ts[-1] * self.par / self.freq ** 2 * (1 + y / self.freq) ** (ts[-1] + 2)
        # return optimize.fsolve(ytm_func, 0.03, fprime=fprime)
        return optimize.newton(ytm_func, guess)


class ReadExcel(object):
    """本类用于从Excel表格中读取数据"""
    def __init__(self, bondtype, year, xlapp):
        self.filename = r"D:\myreport1\利率债一级市场与二级市场关系研究\data\债券招投标结果（{}{}）.xlsx".format(bondtype, year)
        self.xlapp = xlapp
        self.wb = xlapp.Workbooks.Open(self.filename)
        self.ws = self.wb.Worksheets(1)
        self.row_num = self.ws.UsedRange.Rows.Count-2
        self.col_num = 31

    def extract(self):
        data = self.ws.Range(self.ws.Cells(2, 1), self.ws.Cells(self.row_num, 31)).Value
        self.wb.Close()
        return data


class Excel2DB(object):
    """本类用于从Excel中读取数据后写入数据库"""
    def __init__(self, db, bondtype):
        self.db = db
        self.cur = self.db.cursor()
        self.bondtype = bondtype
        if bondtype == "国债":
            self.table = "treasury"
        elif bondtype == "国开债":
            self.table = "cdb"
        else:
            raise IndexError("错误的债券类型参数")
        self.xlapp = win32.gencache.EnsureDispatch("Excel.Application")

    def insert(self):
        sql = "insert into {} values %s".format(self.table)
        try:
            for year in range(2013, 2019):
                readexcel = ReadExcel(self.bondtype, year, self.xlapp)
                data = readexcel.extract()
                for row in data:
                    r = tuple((e.strftime("%Y-%m-%d") if isinstance(e, pywintypes.TimeType) else e for e in row))
                    self.cur.execute(sql, (r,))
            self.db.commit()
        except Exception as p:
            self.db.rollback()
            print(p)
            print(r)

    def close(self):
        self.cur.close()


class Wind2DB(object):
    """本类用于从Wind中读取数据并写入数据库"""
    def __init__(self, db, bondtype, market="secondary"):
        self.db = db
        self.cur = self.db.cursor()
        if market == "primary":
            if bondtype == "国债":
                self.data = w.edb("M1001940,M1001942,M1001943,M1001944,M1001946", "2013-01-01", "2018-06-14")
                self.table = "tb_pri"
            elif bondtype == "国开债":
                self.data = w.edb("M1004440,M1004441,M1004442,M1004443,M1004444", "2013-01-01", "2018-06-14")
                self.table = "cdb_pri"
            else:
                raise IndexError("错误的bondtype参数类型")
        elif market == "secondary":
            if bondtype == "国债":
                self.data = w.edb("S0059744,S0059746,S0059747,S0059748,S0059749", "2013-01-01", "2018-06-14")
                self.table = "tb_sec"
            elif bondtype == "国开债":
                self.data = w.edb("M1004263,M1004265,M1004267,M1004269,M1004271", "2013-01-01", "2018-06-14")
                self.table = "cdb_sec"
            else:
                raise IndexError("错误的bondtype参数类型")
        else:
            raise IndexError("错误的market参数类型")

    def insert(self):
        sql = "insert into {} values %s".format(self.table)
        try:
            for dt, y1, y2, y3, y4, y5 in zip(self.data.Times, self.data.Data[0], self.data.Data[1], self.data.Data[2],
                                              self.data.Data[3], self.data.Data[4]):
                s = []
                for y in (dt, y1, y2, y3, y4, y5):
                    if isinstance(y, dtt.date):
                        s.append(y)
                    elif math.isnan(y):
                        s.append(None)
                    else:
                        s.append(y)
                self.cur.execute(sql, (s,))
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            print(s)
            print(e)


def create_table(db, bondtype, market="issue_result"):
    if market == "issue_result":
        excel2db = Excel2DB(db, bondtype)
        excel2db.insert()
        excel2db.close()
    elif market == "primary":
        wind2db = Wind2DB(db, bondtype, market="primary")
        wind2db.insert()
    elif market == "secondary":
        wind2db = Wind2DB(db, bondtype)
        wind2db.insert()
    else:
        raise IndexError("错误的market参数类型")


class DBPlot(object):
    """本类用于使用数据库的数据进行画图"""
    def __init__(self, db):
        self.db = db
        self.cur = self.db.cursor()

    def close(self):
        self.cur.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def get_deltadata(self, term, bondtype):
        if bondtype == "国债":
            table1 = "tb_pri"
            table2 = "tb_sec"
        elif bondtype == "国开债":
            table1 = "cdb_pri"
            table2 = "cdb_sec"
        else:
            raise IndexError("错误的参数bondtype")
        sql1 = "select dt, {0} from {1} where {0} is not null".format(term, table1)
        sql2 = "select dt, {0} from {1} ".format(term, table2)
        df1 = pd.read_sql(sql1, self.db, "dt")
        df2 = pd.read_sql(sql2, self.db, "dt")
        df3 = df2.diff()
        index_num = np.array([i-1 for i in range(len(df2)) if df2.index[i] in df1.index])
        df4 = pd.DataFrame(df1.iloc[:, 0].values-df2.iloc[index_num, 0].values, index=df1.index)
        return df1, df2, df3, df4

    def delta_plot(self, term, bondtype):
        """本方法用于绘制三合一的图，分别是二级市场利率走势图，一级市场发行当日二级市场到期收益率变动，以及一级市场
        发行结果相对上一交易日二级市场收盘价的变动"""
        df1, df2, df3, df4 = self.get_deltadata(term, bondtype)
        figure, axes = plt.subplots(3, 1, True)
        axes[0].plot(df2)
        axes[1].vlines(df3.loc[df4.index].index, 0, df3.loc[df4.index])
        axes[2].vlines(df4.index, 0, df4)
        plt.show()


def create_db(db):
    """创建数据库内的表格"""
    # create_table(db, "国债")
    create_table(db, "国开债")
    # create_table(db, "国债", "secondary")
    # create_table(db, "国开债", "secondary")
    # create_table(db, "国债", "primary")
    # create_table(db, "国开债", "primary")


def figures(db):
    """画图函数"""
    with DBPlot(db) as dbplot:
        dbplot.delta_plot("10Y", "国债")


def main():
    w.start()
    db = pymysql.connect("localhost", "root", "root", charset="utf8")



if __name__ == "__main__":
    w.start()
    db = pymysql.connect("localhost", "root", "root", "myreport1", charset="utf8")
    # create_db(db)
    # figures(db)
    # db.close()
    # print(sys.path[0])