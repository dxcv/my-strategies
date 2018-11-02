# 本文件用于创建MySQL数据库
# 季俊男

import pymysql
import win32com.client
import pywintypes
from WindPy import w
import math
import datetime as dtt
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import scipy.optimize as optimize
import re


def create_database(cur):
    """本函数用于创建数据库与表"""
    _ = cur.execute("create database if not exists strategy1 character set UTF8MB4")
    _ = cur.execute("use strategy1")
    sql_tb_pri = """
    create table if not exists tb_pri(
    `dt` date NOT NULL COMMENT '发行日期',
    `code` char(15) NOT NULL PRIMARY KEY COMMENT '债券代码',
    `term` int NOT NULL COMMENT '债券期限',
    `rate` float(6, 4) DEFAULT NULL COMMENT '中标利率',
    `mg_rate` float(7, 4) DEFAULT NULL COMMENT '边际中标利率',
    `multiplier` float(5, 2) DEFAULT NULL COMMENT '中标倍数',
    `mg_multiplier` float(5, 2) DEFAULT NULL COMMENT '边际中标倍数',
    `bondtype` char(15) DEFAULT "国债" COMMENT '债券类型'
    )ENGINE=InnoDB DEFAULT CHARSET = utf8MB3 COMMENT = '从wind一级发行专题中下载的数据'
    """
    _ = cur.execute(sql_tb_pri)

    sql_appendix1 = """
    create table if not exists appendix1(
    `dt` date NOT NULL COMMENT '招标日期',
    `code` char(15) NOT NULL PRIMARY KEY COMMENT '债券代码',
    `term` int NOT NULL COMMENT '债券期限',
    `amount` float(6, 2) DEFAULT NULL COMMENT '债券发行量',
    `rate` float(6, 4) NOT NULL COMMENT '加权利率',
    `mg_rate` float(6, 4) DEFAULT NULL COMMENT '边际利率',
    `multiplier` float(5, 2) DEFAULT NULL COMMENT '全场倍数',
    `mg_multiplier` float(5, 2) DEFAULT NULL COMMENT '边际倍数',
    `dt_pay` date DEFAULT NULL COMMENT '缴款日',
    `dt_list` date DEFAULT NULL COMMENT '上市日'
    )ENGINE=InnoDB DEFAULT CHARSET = utf8MB3 COMMENT = '从QB中下载的数据'
    """
    _ = cur.execute(sql_appendix1)


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

    def get_ts(self, dt: dtt.date):
        dt_delta_days = (dt - self.dt0).days
        year_days = 365
        t0 = round(dt_delta_days * self.freq / year_days, 2)
        ts = [i for i in range(self.terms*self.freq)]
        return t0, ts

    def bond_ytm(self, dt, price, guess=0.03):
        t0, ts = self.get_ts(dt)
        coup = self.par * self.rate / self.freq
        ytm_func = lambda y: (sum([coup / (1 + y / self.freq) ** t for t in ts]) + self.par / (1 + y / self.freq) **
                              ts[-1]) / (1 + t0 * y / self.freq) - price
        fprime = lambda y: (sum([-t * coup / self.freq * (1 + y / self.freq) ** (t + 1) for t in ts]) - ts[
            -1] * self.par / self.freq * (1 + y / self.freq) ** (ts[-1] + 1)) / (
                                       1 + t0 * y / self.freq) - t0 / self.freq * (1 + t0 * y / self.freq) ** (-2) * (
                                       sum([coup / (1 + y / self.freq) ** t for t in ts]) + self.par / (
                                           1 + y / self.freq) ** ts[-1])
        return optimize.newton(ytm_func, guess, fprime=fprime)


class ReadExcel(object):
    """本类用于从Excel表格中读取数据"""
    def __init__(self, bondtype, year, data_path, xlapp):
        self.bondtype = bondtype
        self.year = year
        if bondtype in ["国开债", "国债"]:
            self.filename = data_path + r"\债券招投标结果（{}{}）.xlsx".format(bondtype, year)
        elif bondtype == "QB补充":
            self.filename = data_path + r"\利率债发行-{}.xlsx".format(year)
        else:
            raise IndexError("错误的债券类型参数")
        self.xlapp = xlapp
        self.wb = xlapp.Workbooks.Open(self.filename)
        self.ws = self.wb.Worksheets(1)

    def extract(self):
        """从excel中提取数据"""
        if self.bondtype == "国债":
            res = self.__data1()
        elif self.bondtype == "QB补充":
            res = self.__data2()
        elif self.bondtype == "国开债":
            res = None
        return res

    def __data1(self):
        """从国债招投标结果中提取附息国债的数据"""
        cont_pattern = re.compile(r"\d{2}00\d{2}x+", re.I)
        init_pattern = re.compile(r"\d{2}00\d{2}[^xX]+")
        data = self.ws.Range(self.ws.Cells(2,1), self.ws.Cells(2,31).End(4)).Value
        # 首发国债数据
        init = [([d[2].strftime("%Y-%m-%d"), d[0], d[4], d[29], d[10], self.multipliers(d[20], 2),
                 self.mg_multipliers(d[14], d[15]), d[30]], ) for d in data if re.match(init_pattern, d[0])]
        # 续发国债数据
        cont = [([d[2].strftime("%Y-%m-%d"), d[0], d[4], d[27], d[13], self.multipliers(d[20], 2),
                 self.mg_multipliers(d[14], d[15]), d[30]], ) for d in data if re.match(cont_pattern, d[0])]
        init.extend(cont)
        return init

    def __data2(self):
        """从利率债发行结果中提取需要的数据"""
        p = re.compile(r"\d{2}附息国债")
        data = self.ws.Range(self.ws.Cells(3, 1), self.ws.Cells(3, 12).End(4)).Value
        res = [([self.cdt2dt(d[0]), self.name2code(d[2]), self.term2int(d[3]), d[4], d[5], d[6], d[7],
                 self.qb_mg_multipliers(d[8]), self.cdt2dt(d[10]), self.cdt2dt(d[11])], )
               for d in data if re.match(p, d[2])]
        return res

    def cdt2dt(self, cdt):
        """将中文的日期改为标准格式的日期，例如01月11日添加上年份成为2018-1-11"""
        p = re.compile(r"\d{2}")
        if cdt is None:
            return None
        else:
            res = re.findall(p, cdt)
            dt = [str(self.year), res[0], res[1]]
            return "-".join(dt)

    @staticmethod
    def mg_multipliers(a, b):
        if a is None or b is None:
            return None
        else:
            res = round(a/b, 2)
            if res>10:  # 当边际倍数大于10时，该指标的意义就不大了
                res = 10
            return res

    @staticmethod
    def multipliers(a, b):
        if a is None:
            return None
        else:
            return round(a, b)

    @staticmethod
    def qb_mg_multipliers(a):
        if a is None:
            return None
        elif float(a) > 10:
            return 10
        else:
            return a

    @staticmethod
    def name2code(name):
        """根据国债名称获得债券代码"""
        ym_p = re.compile(r"\d{2}")
        x_p = re.compile(r"X\d+", re.I)
        res_ym = re.findall(ym_p, name)
        res_x = re.search(x_p, name)
        if res_x:
            if res_x.group() == "X1":
                res = res_ym[0] + "00" + res_ym[1] + "X" + ".IB"
            else:
                res = res_ym[0]+"00"+res_ym[1]+res_x.group()+".IB"
        else:
            res = res_ym[0]+"00"+res_ym[1]+".IB"
        return res

    @staticmethod
    def term2int(term:str):
        """将字符串形式的期限转换为整数"""
        p = re.compile(r"\d+Y")
        res = re.match(p, term)
        if res:
            res = int(res.group()[:-1])
        return res


class Excel2DB(object):
    """本类用于从Excel中读取数据后写入数据库"""
    def __init__(self, data_path, db):
        self.data_path = data_path  # 存放excel文件的路径
        self.xlapp = win32com.client.Dispatch("Excel.Application")
        self.db = db
        self.cur = self.db.cursor()

    def insert1(self, bondtype, year):
        """按表格类型与年份将单张excel表格写入数据库"""
        rd = ReadExcel(bondtype, year, self.data_path, self.xlapp)
        data = rd.extract()
        if bondtype == "国债":
            table = "tb_pri"
        elif bondtype == "QB补充":
            table = "appendix1"
        else:
            table = None
        sql = "insert into {} values %s".format(table)
        try:
            _ = self.cur.executemany(sql, data)
        except pymysql.err.IntegrityError as e:
            self.db.rollback()
            print(bondtype, year, e)
        else:
            self.db.commit()

    def insert2(self, dt1='2017-7-29', dt2='2017-11-21'):
        """从wind数据库上下载的招投标结果缺失了2017年7月29日至11月21日之间的数据，可从QB的表格内补充该部分数据至tb_pri"""
        sql = """insert into tb_pri(dt, code, term, rate, mg_rate, multiplier, mg_multiplier) 
              select dt, code, term, rate, mg_rate, multiplier, mg_multiplier from appendix1 
              where dt between %s and %s"""
        self.cur.execute(sql, (dt1, dt2))
        self.db.commit()

    def insert(self, years):
        create_database(self.cur)
        for year in years:
            self.insert1("国债", year)
            self.insert1("QB补充", year)
        if 2017 in years:
            self.insert2()
        upper_sql = """update tb_pri set code = upper(code)"""
        XX__sql = """update tb_pri set code = concat(left(code, 6), "X2.IB") where code regexp '.{6}XX.IB'"""
        self.cur.execute(upper_sql)  # 部分债券代码的X是小写，会对表格联结产生影响
        self.cur.execute(XX__sql)
        self.db.commit()

    def update(self, mode=0):
        """由于从WIND下载的17年7月之后的招投标结果缺少边际利率，全场倍数与边际倍数,有的缺少中标利率，因此可以使用QB的数据来进行补充
        可以对三个字段分别更新或者一起更新（mode=0)"""
        sql1 = """update tb_pri t inner join appendix1 a on t.code = a.code 
        set t.multiplier = a.multiplier where t.multiplier is NULL"""
        sql2 = """update tb_pri t inner join appendix1 a on t.code = a.code
        set t.mg_multiplier = a.mg_multiplier where t.mg_multiplier is NULL"""
        sql3 = """update tb_pri t inner join appendix1 a on t.code = a.code
        set t.mg_rate = a.mg_rate where (t.mg_rate is NULL or t.mg_rate > 50) and a.mg_rate is not NULL"""
        sql4 = """update tb_pri t inner join appendix1 a on t.code = a.code
        set t.rate = a.rate where t.rate is NULL"""
        try:
            if mode == 0:
                self.cur.execute(sql1)
                self.cur.execute(sql2)
                self.cur.execute(sql3)
                self.cur.execute(sql4)
            else:
                self.cur.execute(eval("sql{}".format(mode)))
        except:
            print("Excel2DB.update1出现错误")
            self.db.rollback()
        else:
            self.db.commit()

    def update_mg_rate(self):
        """在续发国债招标发行中，Wind给出的边际利率其实是价格，需要转换为利率，借助BondYIM类可以做到将价格转换为收益率"""
        # 利用边际利率是否大于50来判断该字段的记录是价格还是利率，当大于50时一般对应的时价格，因为利率很难超过50%
        sql_select = """select t1.dt, t1.code, t1.term, t1.rate, t1.mg_rate, t2.dt, t2.code, t2.rate
                     from tb_pri t1, tb_pri t2 where t2.code = concat(left(t1.code, 6), ".IB") and t1.mg_rate > 50"""
        # TODO

    def close(self):
        self.cur.close()


def main():
    db = pymysql.connect("localhost", "root", "root", charset="utf8")
    data_path = r"f:\reports\my report\report1\数据"  # excel数据文件存放路径
    years = range(2013, 2019)
    e2db = Excel2DB(data_path, db)
    try:
        e2db.insert(years)
        e2db.update()
    finally:
        e2db.close()
        db.close()


# class Wind2DB(object):
#     """本类用于从Wind中读取数据并写入数据库"""
#     def __init__(self, db, bondtype, market="secondary"):
#         self.db = db
#         self.cur = self.db.cursor()
#         if market == "primary":
#             if bondtype == "国债":
#                 self.data = w.edb("M1001940,M1001942,M1001943,M1001944,M1001946", "2013-01-01", "2018-06-14")
#                 self.table = "tb_pri"
#             elif bondtype == "国开债":
#                 self.data = w.edb("M1004440,M1004441,M1004442,M1004443,M1004444", "2013-01-01", "2018-06-14")
#                 self.table = "cdb_pri"
#             else:
#                 raise IndexError("错误的bondtype参数类型")
#         elif market == "secondary":
#             if bondtype == "国债":
#                 self.data = w.edb("S0059744,S0059746,S0059747,S0059748,S0059749", "2013-01-01", "2018-06-14")
#                 self.table = "tb_sec"
#             elif bondtype == "国开债":
#                 self.data = w.edb("M1004263,M1004265,M1004267,M1004269,M1004271", "2013-01-01", "2018-06-14")
#                 self.table = "cdb_sec"
#             else:
#                 raise IndexError("错误的bondtype参数类型")
#         else:
#             raise IndexError("错误的market参数类型")
#
#     def insert(self):
#         sql = "insert into {} values %s".format(self.table)
#         try:
#             for dt, y1, y2, y3, y4, y5 in zip(self.data.Times, self.data.Data[0], self.data.Data[1], self.data.Data[2],
#                                               self.data.Data[3], self.data.Data[4]):
#                 s = []
#                 for y in (dt, y1, y2, y3, y4, y5):
#                     if isinstance(y, dtt.date):
#                         s.append(y)
#                     elif math.isnan(y):
#                         s.append(None)
#                     else:
#                         s.append(y)
#                 self.cur.execute(sql, (s,))
#             self.db.commit()
#         except Exception as e:
#             self.db.rollback()
#             print(s)
#             print(e)
#
#
# def create_table(db, bondtype, market="issue_result"):
#     if market == "issue_result":
#         excel2db = Excel2DB(db, bondtype)
#         excel2db.insert()
#         excel2db.close()
#     elif market == "primary":
#         wind2db = Wind2DB(db, bondtype, market="primary")
#         wind2db.insert()
#     elif market == "secondary":
#         wind2db = Wind2DB(db, bondtype)
#         wind2db.insert()
#     else:
#         raise IndexError("错误的market参数类型")
#
#
# class DBPlot(object):
#     """本类用于使用数据库的数据进行画图"""
#     def __init__(self, db):
#         self.db = db
#         self.cur = self.db.cursor()
#
#     def close(self):
#         self.cur.close()
#
#     def __enter__(self):
#         return self
#
#     def __exit__(self, exc_type, exc_val, exc_tb):
#         self.close()
#
#     def get_deltadata(self, term, bondtype):
#         if bondtype == "国债":
#             table1 = "tb_pri"
#             table2 = "tb_sec"
#         elif bondtype == "国开债":
#             table1 = "cdb_pri"
#             table2 = "cdb_sec"
#         else:
#             raise IndexError("错误的参数bondtype")
#         sql1 = "select dt, {0} from {1} where {0} is not null".format(term, table1)
#         sql2 = "select dt, {0} from {1} ".format(term, table2)
#         df1 = pd.read_sql(sql1, self.db, "dt")
#         df2 = pd.read_sql(sql2, self.db, "dt")
#         df3 = df2.diff()
#         index_num = np.array([i-1 for i in range(len(df2)) if df2.index[i] in df1.index])
#         df4 = pd.DataFrame(df1.iloc[:, 0].values-df2.iloc[index_num, 0].values, index=df1.index)
#         return df1, df2, df3, df4
#
#     def delta_plot(self, term, bondtype):
#         """本方法用于绘制三合一的图，分别是二级市场利率走势图，一级市场发行当日二级市场到期收益率变动，以及一级市场
#         发行结果相对上一交易日二级市场收盘价的变动"""
#         df1, df2, df3, df4 = self.get_deltadata(term, bondtype)
#         figure, axes = plt.subplots(3, 1, True)
#         axes[0].plot(df2)
#         axes[1].vlines(df3.loc[df4.index].index, 0, df3.loc[df4.index])
#         axes[2].vlines(df4.index, 0, df4)
#         plt.show()
#
#
# def create_db(db):
#     """创建数据库内的表格"""
#     # create_table(db, "国债")
#     create_table(db, "国开债")
#     # create_table(db, "国债", "secondary")
#     # create_table(db, "国开债", "secondary")
#     # create_table(db, "国债", "primary")
#     # create_table(db, "国开债", "primary")
#
#
# def figures(db):
#     """画图函数"""
#     with DBPlot(db) as dbplot:
#         dbplot.delta_plot("10Y", "国债")


if __name__ == "__main__":
    main()