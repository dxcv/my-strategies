# 本文件用于创建MySQL数据库
# 季俊男
# 创建日期：2018/10/22
# 更新时间：2019/2/14
import pymysql
import win32com.client
from WindPy import w
import datetime as dtt
import numpy as np
import pandas as pd
import scipy.optimize as optimize
import re, math


def create_database(cur, table=None):
    """本函数用于创建数据库与表"""
    _ = cur.execute("create database if not exists strategy1 character set UTF8MB4")
    _ = cur.execute("use strategy1")
    sql_tb_pri = """
    create table if not exists tb_pri(
    `dt` date NOT NULL COMMENT '发行日期',
    `code` char(15) NOT NULL PRIMARY KEY COMMENT '债券代码',
    `term` float(4,2) NOT NULL COMMENT '债券期限',
    `rate` float(6, 4) DEFAULT NULL COMMENT '中标利率',
    `price` float(7, 4) DEFAULT NULL COMMENT '中标价格',
    `mg_rate` float(7, 4) DEFAULT NULL COMMENT '边际中标利率',
    `mg_price` float(7, 4) DEFAULT NULL COMMENT '边际中标价格',
    `multiplier` float(5, 2) DEFAULT NULL COMMENT '中标倍数',
    `mg_multiplier` float(5, 2) DEFAULT NULL COMMENT '边际中标倍数',
    `bond_type` char(15) DEFAULT '国债' COMMENT '债券类型',
    `bid_way` char(15) COMMENT '招标方式',
    `target` char(15) COMMENT '招标标的',
    `pay_times` TINYINT DEFAULT NULL COMMENT '每年付息次数'
    )ENGINE=InnoDB DEFAULT CHARSET = utf8MB3 COMMENT = '从wind一级发行专题中下载的数据'
    """
    sql_appendix1 = """
    create table if not exists appendix1(
    `dt` date NOT NULL COMMENT '招标日期',
    `code` char(15) NOT NULL PRIMARY KEY COMMENT '债券代码',
    `term` float(4,2) NOT NULL COMMENT '债券期限',
    `amount` float(6, 2) DEFAULT NULL COMMENT '债券发行量',
    `rate` float(6, 4) NOT NULL COMMENT '加权利率',
    `mg_rate` float(6, 4) DEFAULT NULL COMMENT '边际利率',
    `multiplier` float(5, 2) DEFAULT NULL COMMENT '全场倍数',
    `mg_multiplier` float(5, 2) DEFAULT NULL COMMENT '边际倍数',
    `dt_pay` date DEFAULT NULL COMMENT '缴款日',
    `dt_list` date DEFAULT NULL COMMENT '上市日'
    )ENGINE=InnoDB DEFAULT CHARSET = utf8MB3 COMMENT = '从QB中下载的数据'
    """
    sql_tb_sec = """
    create table if not exists tb_sec(
    `dt` date NOT NULL COMMENT '日期',
    `code` char(15) NOT NULL COMMENT '续发债券代码',
    `code0` char(15) NOT NULL COMMENT '续发债对应首发债券代码',
    `term` float(4,2) NOT NULL COMMENT '债券期限',
    `yield` float(7,4) DEFAULT NULL COMMENT '交易日中债估值收益率',
    `net` float(7,4) DEFAULT NULL COMMENT '中债估值净价',
    `dirty` float(7,4) DEFAULT NULL COMMENT '中债估值全价',
    `seq` tinyint NOT NULL COMMENT '交易日顺序',
    primary key(code, dt)   
    )ENGINE=InnoDB DEFAULT CHARSET = utf8MB3 COMMENT = '一级发行对应区间的二级市场行情'
    """
    sql_tb_rate = """
    create table if not exists tb_rate(
    `dt` DATE NOT NULL COMMENT '日期',
    `term` float(4,2) NOT NULL COMMENT '期限',
    `bond_type` char(10) NOT NULL COMMENT '债券类型',
    `rate` float(7,4) NOT NULL COMMENT '中债估值收益率',    
    primary key(dt, term, bond_type)
    )ENGINE=InnoDB DEFAULT CHARSET = utf8MB3 COMMENT = '各期限国债国开债到期收益率'
    """
    sql_future = """
    CREATE TABLE IF NOT EXISTS future(
    `dt` DATE NOT NULL COMMENT '日期',
    `srate` FLOAT(7,4) NOT NULL COMMENT '结算收益率',
    `crate` FLOAT(7,4) NOT NULL COMMENT '收盘收益率',
    `settle` FLOAT(7,4) NOT NULL COMMENT '结算价',
    `close` FLOAT(7,4) NOT NULL COMMENT '收盘价',
    `term` TINYINT NOT NULL COMMENT '国债期货期限',
    `seq` INT NOT NULL COMMENT '按时间排序编号，5年期与10年期分开编号',
    CONSTRAINT pk PRIMARY KEY(`dt`, `term`)
    )ENGINE=InnoDB DEFAULT CHARSET = utf8MB3 COMMENT = '国债期货结算价与收盘价'
    """

    sql_tb_sec_delta = """
    create table if not exists tb_sec_delta(
    `dt` date NOT NULL COMMENT '日期',
    `code` char(15) NOT NULL COMMENT '续发债券代码',
    `code0` char(15) NOT NULL COMMENT '续发债对应首发债券代码',
    `term` float(4,2) NOT NULL COMMENT '债券期限',
    `delta` float(7,4) DEFAULT NULL COMMENT '交易日中债估值收益率变化',
    `dprice` float(7, 4) DEFAULT NULL COMMENT '交易日中债估值净价变化',
    `seq` tinyint NOT NULL COMMENT '交易日顺序',
    primary key(code, seq)   
    )ENGINE=InnoDB DEFAULT CHARSET = utf8MB3 COMMENT = '一级发行对应区间的二级市场行情变化'
    """
    sql_future_delta = """
    CREATE TABLE IF NOT EXISTS future_delta(
    `dt` DATE NOT NULL COMMENT '日期',    
    `dsettle` FLOAT(7,4) NOT NULL COMMENT '结算价价差',
    `dclose` FLOAT(7,4) NOT NULL COMMENT '收盘价价差',
    `dsrate` FLOAT(6,2) NOT NULL COMMENT '结算收益率差',
    `dcrate` FLOAT(6,2) NOT NULL COMMENT '收盘收益率差',
    `term` TINYINT NOT NULL COMMENT '国债期货期限',
    `seq` INT NOT NULL COMMENT '按时间排序编号，5年期与10年期分开编号',
    CONSTRAINT pk PRIMARY KEY(`dt`, `term`)
    )ENGINE=InnoDB DEFAULT CHARSET = utf8MB3 COMMENT = '国债期货结算价与收盘价'
    """
    sql_payment = r"""
    CREATE TABLE IF NOT EXISTS payment(
    `code` CHAR(15) NOT NULL COMMENT '债券代码（银行间）',
    `pmdt` DATE DEFAULT NULL COMMENT '债券付息日',
    `rate` FLOAT(6, 4) DEFAULT NULL COMMENT '债券利率或付息额',
    CONSTRAINT pk PRIMARY KEY(`code`)
    )ENGINE=InnoDB DEFAULT CHARSET=UTF8MB3 COMMENT = '债券付息日与付息额'
    """
    sql_money = r"""
    CREATE TABLE IF NOT EXISTS money(
    `dt` DATE NOT NULL COMMENT '交易日期',
    `code` CHAR(15) NOT NULL COMMENT '交易品种代码',
    `rate` FLOAT(7, 4) DEFAULT NULL COMMENT '收益率',
    CONSTRAINT pk PRIMARY KEY(`dt`, `code`)
    )ENGINE=InnoDB DEFAULT CHARSET=UTF8MB3 COMMENT = '货币市场利率'
    """
    sql_future_minute = """
    CREATE TABLE IF NOT EXISTS future_minute(
    `dtt` DATETIME NOT NULL COMMENT '交易时间',
    `term` TINYINT NOT NULL COMMENT '交易品种期限',
    `close` FLOAT(7, 4) DEFAULT NULL COMMENT '收盘价',
    `rate` FLOAT(6, 4) DEFAULT NULL COMMENT '收盘价对应的收益率',
    `seq` tinyint NOT NULL COMMENT '顺序',
    CONSTRAINT pk PRIMARY KEY(`dtt`, `term`)
    )ENGINE=InnoDB DEFAULT CHARSET=UTF8MB3 COMMENT = '国债期货价格5分钟序列'
    """
    sql_dts1 = """
    CREATE TABLE IF NOT EXISTS dts1(
    `dt` DATE NOT NULL COMMENT '交易所交易日期',
    `seq` INT NOT NULL COMMENT '日期顺序',
    CONSTRAINT pk PRIMARY KEY(`dt`)
    )ENGINE=InnoDB DEFAULT CHARSET=UTF8MB3 COMMENT = '交易所交易日序列'
    """
    sql_dts2 = """
    CREATE TABLE IF NOT EXISTS dts2(
    `dt` DATE NOT NULL COMMENT '银行间交易日期',
    `seq` INT NOT NULL COMMENT '日期顺序',
    CONSTRAINT pk PRIMARY KEY(`dt`)
    )ENGINE=InnoDB DEFAULT CHARSET=UTF8MB3 COMMENT = '银行间交易日序列'
    """
    sql_impact = """
    CREATE TABLE IF NOT EXISTS impact(
    `dt` DATE NOT NULL COMMENT  '日期',
    `code` CHAR(15) NOT NULL COMMENT '续发债券代码' PRIMARY KEY,
    `code0` CHAR(15) NOT NULL COMMENT '续发债对应首发债券代码',
    `term` FLOAT(4,2) NOT NULL COMMENT '债券期限',
    `delta` FLOAT(7,4) DEFAULT NULL COMMENT '加权利率发行冲击（BP）',
    `mg_delta` FLOAT(7, 4) DEFAULT NULL COMMENT '边际利率发行冲击（BP）',
    `bondtype` CHAR(10) DEFAULT NULL COMMENT '债券类型'
    )ENGINE=InnoDB DEFAULT CHARSET=UTF8MB3 COMMENT = '发行冲击'
    """
    if table is None:
        for sql in [sql_tb_pri, sql_appendix1, sql_tb_sec, sql_tb_rate, sql_future, sql_tb_sec_delta, sql_future_delta,
                    sql_payment, sql_money, sql_future_minute, sql_dts1, sql_dts2, sql_impact]:
            _ = cur.execute(sql)
    elif table == "pass":
        pass
    else:
        _ = cur.execute(eval("sql_{}".format(table)))


def dt_offset(cur, dt0, offset:int, table="dts2"):
    """从数据库中提取交易日的偏离值，默认使用银行间交易日（dts2)"""
    sql = """
             select t1.dt from {0} t1 inner join {0} t2 on t2.dt = %s and t1.seq = t2.seq + %s
             """.format(table)
    dt = Data(sql, cur, (dt0, offset)).data[0][0]
    return dt


def p2y_future(price, term):
    if term == 5:
        flow = [3, 3, 3, 3, 103]
    elif term == 10:
        flow = [3, 3, 3, 3, 3, 3, 3, 3, 3, 103]
    elif term == 2:
        flow = [3, 103]
    else:
        flow = None
    if isinstance(price, list):
        res = [100 * round(np.irr([-p, *flow]), 6) for p in price]
    else:
        res = 100 * round(np.irr([-price, *flow]), 6)
    return res


def get_freq(code):
    """从Wind中提取债券的年付息次数"""
    wdata = w.wss(code, "interestfrequency")
    res = wdata.Data[0][0]
    if res is None:
        res = 1
    return res


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

    def __init__(self, term, rate, dt0: dtt.date, freq = 1, par=100):
        """类初始化函数，terms代表债券年限，rate表示发行利率，dt0表示发行日期，par表示债券面值，默认100，付息频率支持
        1年1次或1年2次"""
        self.term = term
        self.rate = rate / 100
        self.dt0 = dt0
        self.year0 = self.dt0.year
        self.month0 = self.dt0.month
        self.day0 = self.dt0.day
        self.par = par
        self.freq = freq

    def get_ts(self, dt: dtt.date):
        """本方法用于计算利息与本金支付的时间点序列，本金在最后一个时间点支付，例如一个已经发行1.75年的5年期付息国债，
        本方法分别返回一个0.25的数值和一个[0, 1, 2, 3]的列表，0.25表示距离最近的一次付息时间长度（年），[0, 1, 2, 3]
        接下来剩四次付息，距离这四次付息时间点的时间分别为0.25，1.25、2.25和3.25（年），拥有这两个结果便可以根据价格
        计算到期收益率，或者根据到期收益率计算价格"""
        if self.freq == 1:
            # 年付息次数为1次时的计算相对简单
            if dt > dtt.date(dt.year, self.month0, self.day0) or dt == self.dt0:
                x = dt.year - self.year0
                y = (dtt.date(dt.year+1, self.month0, self.day0) - dt).days + 1
                year_days = (dtt.date(dt.year+1, self.month0, self.day0) - dtt.date(dt.year, self.month0, self.day0)).days
            else:
                x = dt.year - self.year0 - 1
                y = (dtt.date(dt.year, self.month0, self.day0) - dt).days + 1
                year_days = (dtt.date(dt.year, self.month0, self.day0) - dtt.date(dt.year - 1, self.month0, self.day0)).days
            t0 = y / year_days
            ts = [i for i in range(int(self.term) - x)]
        elif self.freq == 2:
            # 年付息次数为2次时的计算相对复杂一些，需要先计算出除了发行日次日的付息日外的另一个付息日（实际为发行日后的六个月）
            year1 = dt.year
            dt0 = dtt.date(dt.year, self.month0, self.day0)
            if self.month0 <= 6:
                month1 = self.month0 + 6
                try:
                    dt1 = dtt.date(year1, month1, self.day0)
                except ValueError as e:
                    dt1 = dtt.date(year1, month1+1, 1) - dtt.timedelta(1)
                if dt0 < dt <= dt1 or dt == self.dt0:
                    x = 2 * (dt.year - self.year0)
                    y = (dt1 - dt).days + 1
                    half_year_days = (dt1 - dt0).days
                elif dt <= dt0:
                    x = 2 * (dt.year - self.year0) - 1
                    y = (dtt.date(dt.year, self.month0, self.day0) - dt).days + 1
                    half_year_days = (dt0 - dtt.date(dt1.year -1, dt1.month, dt1.day)).days
                else:
                    dt2 = dtt.date(dt.year + 1, self.month0, self.day0)
                    x = 2 * (dt.year - self.year0) + 1
                    y = (dt2 - dt).days + 1
                    half_year_days = (dt2 - dt1).days
                t0 = y / half_year_days
                ts = [i for i in range(int(2 * self.term) - x)]
            else:
                month1 = self.month0 - 6
                try:
                    dt1 = dtt.date(year1, month1, self.day0)
                except ValueError as e:
                    dt1 = dtt.date(year1, month1+1, 1) - dtt.timedelta(1)
                if dt1 < dt <= dt0 or dt == self.dt0:
                    x = 2 * (dt.year - self.year0) - 1
                    y = (dt0 - dt).days + 1
                    half_year_days = (dt0 - dt1).days
                elif dt <= dt1:
                    dt2 = dtt.date(dt.year - 1, self.month0, self.day0)
                    x = 2 * (dt.year - self.year0) - 2
                    y = (dt1 - dt).days + 1
                    half_year_days = (dt1 - dt2).days
                else:
                    dt2 = dtt.date(dt.year+1, dt1.month, dt1.day)
                    x = 2 * (dt.year - self.year0)
                    y = (dt2 - dt).days + 1
                    half_year_days = (dt2 - dt0).days
                t0 = y / half_year_days
                ts = [i for i in range(int(2 * self.term) - x)]
        else:
            raise ValueError("不被接受的参数值self.freq")
        return t0, ts

    def bond_ytm(self, dt: dtt.date, price, mode=0, guess=0.03):
        """根据价格计算到期收益率，当mode=0时，不足1年的现金流仍以复利形式计算，当mode=1时，不足1年的现金流以单利按天数计算"""
        t0, ts = self.get_ts(dt)
        coup = self.par * self.rate/self.freq
        if mode == 0:
            ytm_func = lambda y: (sum([coup / (1 + y) ** t for t in ts]) + self.par / (1 + y) ** ts[-1]) / (
                        1 + y) ** t0 - price
            fprime = lambda y: sum([-(t + t0) * coup / (1 + y) ** (t + t0 + 1) for t in ts]) - (
                ts[-1] + t0) * self.par / (1 + y) ** (ts[-1] + t0 + 1)
        elif mode ==1:
            ytm_func = lambda y: (sum([coup / (1 + y) ** t for t in ts]) + self.par / (1 + y) ** ts[-1]) / (
                    1 + t0 * y) - price
            fprime = lambda y: (sum([-t * coup / (1 + y) ** (t + 1) for t in ts]) - ts[-1] * self.par / (1 + y) ** (
                    ts[-1] + 1)) / (1 + t0 * y) - t0 * (1 + t0 * y) ** (-2) * (
                                       sum([coup / (1 + y) ** t for t in ts]) + self.par / (1 + y) ** ts[-1])
        else:
            raise ValueError("不被接受的参数值mode")
        return self.freq * 100 * optimize.newton(ytm_func, guess, fprime=fprime)

    def bond_price(self, dt: dtt.date, rate, mode=0):
        """根据到期收益率计算价格，参数中dt表示增发债日期，rate表示到期收益率"""
        rate = rate / (100 * self.freq)
        coup = self.par * self.rate / self.freq
        t0, ts = self.get_ts(dt)
        if mode == 0:
            price = (sum([coup / (1 + rate) ** t for t in ts]) + self.par / (1 + rate) ** ts[-1])/(1 + rate) ** t0
        elif mode == 1:
            price = (sum([coup / (1 + rate) ** t for t in ts]) + self.par / (1 + rate) ** ts[-1])/(1 + t0 * rate)
        else:
            raise ValueError("不被接受的参数值mode")
        return price


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
            res = self.__data3()
        return res

    def __data1(self):
        """从国债招投标结果中提取附息国债的数据"""
        cont_pattern = re.compile(r"\d{2}00\d{2}x+", re.I)
        init_pattern = re.compile(r"\d{2}00\d{2}[^xX\d]+")
        data = self.ws.Range(self.ws.Cells(2,1), self.ws.Cells(2,31).End(4)).Value
        # 首发国债数据
        init = [([d[2].strftime("%Y-%m-%d"), d[0], d[4], d[29], d[28], d[10], None, self.multipliers(d[20], 2),
                 self.mg_multipliers(d[14], d[15]), d[30], d[5], d[6], get_freq(d[0])], )
                for d in data if re.match(init_pattern, d[0])]
        # 续发国债数据
        cont = [([d[2].strftime("%Y-%m-%d"), d[0], d[4], d[27], d[28], d[13], d[13], self.multipliers(d[20], 2),
                 self.mg_multipliers(d[14], d[15]), d[30], d[5], d[6], get_freq(d[0])], )
                for d in data if re.match(cont_pattern, d[0])]
        init.extend(cont)
        return init

    def __data2(self):
        """从利率债发行结果中提取需要的附息国债与国开债的数据"""
        p1 = re.compile(r"\d{2}附息国债")
        p2 = re.compile(r"\d{2}国开\d{2}")
        data = self.ws.Range(self.ws.Cells(3, 1), self.ws.Cells(3, 12).End(4)).Value
        res = [([self.cdt2dt(d[0]), self.name2code1(d[2]), self.term2int(d[3]), d[4], d[5], d[6], d[7],
                 self.qb_mg_multipliers(d[8]), self.cdt2dt(d[10], d[0]), self.cdt2dt(d[11], d[0])],)
               for d in data if re.match(p1, d[2])]
        res1 = [([self.cdt2dt(d[0]), self.name2code2(d[2]), self.term2int(d[3]), d[4], d[5], d[6], d[7],
                 self.qb_mg_multipliers(d[8]), self.cdt2dt(d[10], d[0]), self.cdt2dt(d[11], d[0])],)
                for d in data if re.match(p2, d[2])]
        res.extend(res1)
        return res

    def __data3(self):
        """从国开债招投标结果中提取国开债的数据"""
        init_pattern = re.compile(r"\d{2}02\d{2}[^ZH\d]+", re.I)
        cont_pattern = re.compile(r"\d{2}02\d{2}[ZH]+", re.I)
        data = self.ws.Range(self.ws.Cells(2, 1), self.ws.Cells(2, 31).End(4)).Value
        # 首发国开债数据
        init = [([d[2].strftime("%Y-%m-%d"), d[0], d[4], d[29], d[28], d[29], self.multipliers(d[20], 2),
                  self.mg_multipliers(d[14], d[15]), d[30], d[5], d[6]],)
                for d in data if re.match(init_pattern, d[0])]
        # 续发国开债数据
        cont = [([d[2].strftime("%Y-%m-%d"), d[0], d[4], d[27], d[28], d[27], self.multipliers(d[20], 2),
                  self.mg_multipliers(d[14], d[15]), d[30], d[5], d[6]],)
                for d in data if re.match(cont_pattern, d[0])]
        init.extend(cont)
        return init

    def cdt2dt(self, cdt, cdt0=None):
        """将中文的日期改为标准格式的日期，例如01月11日添加上年份成为2018-1-11"""
        p = re.compile(r"\d{2}")
        if cdt is None:
            return None
        else:
            res = re.findall(p, cdt)
            if cdt0:
                res0 = re.findall(p, cdt0)
                if res[0] == 1 and res0[1] ==12:
                    dt = [str(self.year+1), res[0], res[1]]
                else:
                    dt = [str(self.year), res[0], res[1]]
            else:
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
    def name2code1(name):
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
    def name2code2(name):
        """根据国开债名称获得债券代码"""
        ym_p = re.compile(r"\d{2}")
        x_p = re.compile(r"(X\d+)|H", re.I)
        res_ym = re.findall(ym_p, name)
        res_x = re.search(x_p, name)
        if res_x:
            if res_x.group() == "X1":
                res = res_ym[0] + "02" + res_ym[1] + "Z" + ".IB"
            elif res_x.group() == "H":
                res = res_ym[0] + "02" + res_ym[1] + "H" + ".IB"
            else:
                res = res_ym[0]+"02"+res_ym[1]+"Z"+res_x.group()[1:]+".IB"
        else:
            res = res_ym[0]+"02"+res_ym[1]+".IB"
        return res

    @staticmethod
    def term2int(term:str):
        """将字符串形式的期限转换为整数"""
        p = re.compile(r".+Y")
        res = re.match(p, term)
        if res:
            res = float(res.group()[:-1])
        return res


class Excel2DB(object):
    """本类用于从Excel中读取数据后写入数据库"""
    def __init__(self, data_path, db, cur):
        self.data_path = data_path  # 存放excel文件的路径
        self.xlapp = win32com.client.Dispatch("Excel.Application")
        self.db = db
        self.cur = cur

    def insert1(self, bondtype, year):
        """按表格类型与年份将单张excel表格写入数据库"""
        rd = ReadExcel(bondtype, year, self.data_path, self.xlapp)
        data = rd.extract()
        if bondtype == "国债":
            table = "tb_pri"
        elif bondtype == "QB补充":
            table = "appendix1"
        elif bondtype == "国开债":
            table = "tb_pri"
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
        """从wind数据库上下载的国债招投标结果缺失了2017年7月29日至11月21日之间的数据，可从QB的表格内补充该部分数据
        至tb_pri并且"""
        sql = """insert into tb_pri(dt, code, term, rate, mg_rate, multiplier, mg_multiplier) 
              select dt, code, term, rate, mg_rate, multiplier, mg_multiplier from appendix1 
              where dt between %s and %s and code regexp '[:alnum:]{2}00.*'"""
        sql1 = """update tb_pri set pay_times = %s where code = %s"""
        sql2 = """select code from appendix1 where dt between %s and %s"""
        self.cur.execute(sql, (dt1, dt2))
        codes = Data(sql2, self.cur, (dt1, dt2)).select_col(0)
        data = [(get_freq(code), code) for code in codes]
        self.cur.executemany(sql1, data)
        self.db.commit()

    def insert(self, years):
        for year in years:
            self.insert1("国债", year)
            self.insert1("QB补充", year)
            # self.insert1("国开债", year)
        if 2017 in years:
            self.insert2()
        upper_sql = """update tb_pri set code = upper(code)"""
        XX__sql = """update tb_pri set code = concat(left(code, 6), "X2.IB") where code regexp '.{6}XX.IB'"""
        self.cur.execute(upper_sql)  # 部分债券代码的X是小写，会对表格联结产生影响
        self.cur.execute(XX__sql)
        self.cur.execute("delete from tb_pri where term = 0.5") # 有一只国开债剩余期限0.5年，要删掉
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
            print("Excel2DB.update出现错误")
            self.db.rollback()
        else:
            self.db.commit()

    def update_mg_rate(self):
        """在续发国债招标发行中，Wind给出的边际利率其实是价格，需要转换为利率，借助BondYIM类可以做到将价格转换为收益率"""
        # 利用边际利率是否大于50来判断该字段的记录是价格还是利率，当大于50时一般对应的时价格，因为利率很难超过50%
        sql_update0 = """
                      update tb_pri t1
                      inner join appendix1 t2
                      on t1.code = t2.code
                      set t1.mg_rate = t2.mg_rate
                      where t1.mg_rate > 50 and t2.mg_rate is not null
                      """
        sql_select1 = """
                      select t1.dt, t1.code, t1.term, t1.rate, t1.mg_rate, t2.dt, t2.code, t2.rate,t2.pay_times, 
                      t3.dt_pay 
                      from tb_pri t1, tb_pri t2, appendix1 t3 
                      where t2.code = concat(left(t1.code, 6), ".IB")
                      and t1.code = t3.code 
                      and t1.mg_rate > 50
                      """
        data1 = Data(sql_select1, self.cur).data
        # for d in data:
        #     print(d[2], d[7], d[5], d[0], d[4], d[1], end="     ")
        #     print(BondYTM(d[2], d[7], d[5]).bond_ytm(d[0], d[4]))
        data_update1 = [[BondYTM(d[2], d[7], d[5], d[8]).bond_ytm(d[9], d[4]), d[1]] for d in data1]
        sql_update1 = """update tb_pri set mg_rate = %s where code = %s"""
        try:
            _ = self.cur.execute(sql_update0)
            _ = self.cur.executemany(sql_update1, data_update1)
        except:
            self.db.rollback()
        else:
            self.db.commit()

    def update_mg_price(self):
        """计算边际中标价格，原理类似于update_mg_rate，注意实际创建数据库时，该方法须在update_mg_rate运行之后使用"""
        sql_select2 = """
                      select t1.dt, t1.code, t1.term, t1.rate, t1.mg_rate, t2.dt, t2.code, t2.rate,t2.pay_times, 
                      t3.dt_pay 
                      from tb_pri t1, tb_pri t2, appendix1 t3 
                      where t2.code = concat(left(t1.code, 6), ".IB")
                      and t1.code = t3.code 
                      and t1.mg_price is null
                      and t1.mg_rate is not null
                      and t3.dt_pay is not null
                      """
        data2 = Data(sql_select2, self.cur).data
        data_update2 = [[BondYTM(d[2], d[7], d[5], d[8]).bond_price(d[9], d[4]), d[1]] for d in data2]
        sql_update2 = """update tb_pri set mg_price = %s where code = %s"""
        try:
            _ = self.cur.executemany(sql_update2, data_update2)
        except:
            self.db.rollback()
        else:
            self.db.commit()

    def update_price(self):
        """由appendix1补录的数据（2017/7/29-2017/11/21）缺少price信息，需要自己计算，计算由BondYTM负责"""
        sql_select = r"""
                      select t1.dt, t1.code, t1.term, t1.rate, t2.dt, t2.code, t2.rate, t2.pay_times, t3.dt_pay
                      from tb_pri t1 inner join tb_pri t2 inner join appendix1 t3
                      on t1.code = concat(left(t2.code, 6), ".IB") and t1.code = t3.code
                      where t2.price is null
                      """
        data = Data(sql_select, self.cur).data
        data_update = [[BondYTM(d[2], d[3], d[0], d[7]).bond_price(d[8], d[6]), d[5]] for d in data]
        sql_update = "update tb_pri set price = %s where code = %s"
        for d in data_update:
            print(d)
        try:
            _ = self.cur.executemany(sql_update, data_update)
        except:
            self.db.rollback()
        else:
            self.db.commit()


class Wind2DB(object):
    """Wind2DB类主要用于从Wind数据库中提取所需数据并写入数据库"""
    def __init__(self, db, cur):
        self.db = db
        self.cur = cur

    def get_data_tb_sec(self):
        """本方法用于提取续发债发行日的前一交易日至发行后四个交易日（共6个交易日）的债券二级市场中债估值收益率"""
        sql1 = """select concat(left(code, 6), ".IB") as code_init, count(*) as num from tb_pri 
            group by left(code, 6) having num>1"""
        codes_init = Data(sql1, self.cur).select_col(0)
        # 利用首发债代码筛选出对应的续发债的发行日期
        data = []
        for code_init in codes_init:
            p = code_init[0:6] + "[XZH]"
            sql2 = """select code, dt, term from tb_pri where code regexp %s"""
            datum = Data(sql2, self.cur, p).data
            for d in datum:
                # print(d)
                res = w.wsd(code_init, "yield_cnbd,net_cnbd,dirty_cnbd", dt_offset(self.cur, d[1], -1),
                            dt_offset(self.cur, d[1], 10),
                            "credibility=1;TradingCalendar=NIB")
                try:
                    ys, ns, ds = res.Data
                except ValueError as e:
                    print(d[0], d[1], e)
                    raise ValueError(e)
                seq = range(len(ys))
                dts = res.Times
                for y, n, dd, dt, s in zip(ys, ns, ds, dts, seq):
                    data.append(([dt, d[0], code_init, d[2], None if math.isnan(y) else y,
                                  None if math.isnan(n) else n, None if math.isnan(dd) else dd, s],))
        return data

    @staticmethod
    def get_data_tb_rate(dt1="2013-1-1", dt2="2019-1-31"):
        """本方法用于从WIND获取国债与国开债的期限利率的中债估值，共9个期限，分别是1Y 2Y 3Y 5Y 7Y 10Y 20Y 30Y 50Y"""
        code1 = ["S0059744", "S0059745", "S0059746", "S0059747", "S0059748", "S0059749", "S0059751",
                 "S0059752", "M1000170"]
        code2 = ["M1004263", "M1004264", "M1004265", "M1004267", "M1004269", "M1004271", "M1004273",
                 "M1004274", "M1004275"]
        codes = [code1, code2]
        bond_type = ["国债", "国开债"]
        data = []
        terms = [1, 2, 3, 5, 7, 10, 20, 30, 50]
        for m in range(len(codes)):
            res_w = w.edb(codes[m], dt1, dt2, "Fill=Previous")
            for n in range(len(terms)):
                d = [([res_w.Times[i], terms[n], bond_type[m], res_w.Data[n][i]],) for i in range(len(res_w.Times))]
                data.extend(d)
        return data

    @staticmethod
    def get_data_future(dt="2019-1-31"):
        """本函数用于从WIND获取建立国债期货结算价与收盘价表格所需的数据,包括TF与T合约"""
        wd_tf = w.wsd("TF.CFE", "settle,close", "2013-9-6", dt, "")
        wd_t =  w.wsd("TF.CFE", "settle,close", "2015-3-20", dt, "")
        wds = [wd_tf, wd_t]
        terms = [5, 10]
        data = []
        for m in range(len(terms)):
            wd = list(zip(wds[m].Times, p2y_future(wds[m].Data[0], terms[m]), p2y_future(wds[m].Data[1], terms[m]),
                          wds[m].Data[0], wds[m].Data[1]))
            d=[([*wd[i], terms[m], i],) for i in range(len(wd))]
            data.extend(d)
        return data

    def get_data_payment(self):
        sql = r"select distinct code0 from tb_sec"
        codes = Data(sql, self.cur).select_col(0)
        wdata = w.wss(codes, "maturitydate,couponrate", "N=0")
        data = [(d,) for d in list(zip(wdata.Codes, wdata.Data[0], wdata.Data[1]))]
        return data

    @staticmethod
    def get_data_money(dt1=dtt.date(2013, 1, 1), dt2=dtt.date(2019, 1, 31),
                       codes=("FR007.IR", "SHIBOR3M.IR", "SHIBORON.IR")):
        """从Wind提取货币市场利率数据"""
        res = []
        data = w.wsd(codes, "close", dt1, dt2, "TradingCalendar=NIB")
        for i in range(len(data.Codes)):
            code = data.Codes[i]
            for j in range(len(data.Times)):
                d = None if math.isnan(data.Data[i][j]) else data.Data[i][j]
                res.append(([data.Times[j], code, d],))
        return res

    @staticmethod
    def get_data_future_minute(codes=("TF.CFE", "T.CFE"), barsize=5,
                               dt=dtt.datetime(2019, 1, 31, 15, 16, 00)):
        """从Wind提取分钟序列"""
        res = list()
        for code in codes:
            if code == "T.CFE":
                dt1 = dtt.datetime(2013, 9, 6, 9, 15, 00)
                term = 10
            elif code == "TF.CFE":
                dt1 = dtt.datetime(2015, 3, 20, 9, 15, 00)
                term = 5
            else:
                raise ValueError("错误的codes参数类型")
            BarSize = "BarSize={}".format(barsize)
            wdata = w.wsi(code, "close", dt1, dt, BarSize)
            seq = 0
            data = []
            for d in zip(wdata.Times, wdata.Data[0]):
                data.append(([d[0], term, d[1], p2y_future(d[1], term), seq],))
                seq += 1
                if seq == 54:
                    seq = 0
            res.extend(data)
        return res

    @staticmethod
    def get_data_dts1(dt1="2013-1-1", dt2="2019-1-31"):
        """从Wind提取交易所的交易日序列"""
        wdata = w.tdays(dt1, dt2, "")
        d = wdata.Data[0]
        n = len(d)
        data = [([d[i].date(), i],) for i in range(n)]
        return data

    @staticmethod
    def get_data_dts2(dt1="2013-1-1", dt2="2019-1-31"):
        """从Wind提取银行间的交易日序列"""
        wdata = w.tdays(dt1, dt2, "TradingCalendar=NIB")
        d = wdata.Data[0]
        n = len(d)
        data = [([d[i].date(), i],) for i in range(n)]
        return data

    def insert(self, table=None):
        if table:
            data = eval("self.get_data_{}()".format(table))
            self.cur.executemany(r"insert into {} values %s".format(table), data)
        else:
            for t in ["tb_sec", "tb_rate", "future", "payment", "money", "future_minute", "dts1", "dts2"]:
                data = eval("self.get_data_{}()".format(t))
                self.cur.executemany(r"insert into {} values %s".format(t), data)
        self.db.commit()


class DB2self(object):
    """本类用于创建基于数据库自身而创建的对象，例如表格、函数、过程，不需依赖外部数据源"""
    def __init__(self, db, cur):
        self.db = db
        self.cur = cur

    def create_function(self, funcname=None):
        """在数据库中创建函数"""
        sql_imp_delta = r"""
        create function imp_delta(ccode char(15), sseq tinyint)
        returns float(6, 2)
        language sql deterministic 
        begin
          declare y0 float(7, 4);
          declare y1 float(7, 4);          
          if sseq = 0 then
            select rate into y1 from tb_pri where code = ccode;
            select yield into y0 from tb_sec where code = ccode and seq = 0;
          else
            select yield into y1 from tb_sec where code = ccode and seq = sseq;
            select yield into y0 from tb_sec where code = ccode and seq = sseq-1;
          end if;
          return (100 * (y1-y0));
        end;              
        """
        sql_imp_dprice = r"""
        create function imp_dprice(ccode char(15), sseq tinyint)
        returns float(6, 4)
        language sql deterministic
        begin
          declare p0 float(7, 4);
          declare p1 float(7, 4);
          if sseq = 0 then
            select price into p1 from tb_pri where code = ccode;
            select net into p0 from tb_sec where code = ccode and seq = 0;
          else
            select net into p1 from tb_sec where code = ccode and seq = sseq;
            select net into p0 from tb_sec where code = ccode and seq = sseq - 1; 
          end if;
          return (p1 -p0);
        end;
        """
        if funcname:
            self.cur.execute(eval("sql_{}".format(funcname)))
        else:
            for name in ["imp_delta", "imp_dprice"]:
                self.cur.execute(eval("sql_{}".format(name)))

    def insert_tb_sec_delta(self):
        sql1 = r"""
        insert into tb_sec_delta(dt, code, code0, term, seq)
        select dt, code, code0, term, seq from tb_sec
        """
        sql2 = r"""update tb_sec_delta set delta = imp_delta(code, seq), dprice = imp_dprice(code, seq)"""
        try:
            self.cur.execute(sql1)
            self.cur.execute(sql2)
        except:
            self.db.rollback()
        else:
            self.db.commit()

    def insert_future_delta(self):
        sql = r"""
        insert into future_delta 
        select t1.dt, t1.settle-t2.settle, t1.close-t2.close, 100*(t1.srate-t2.srate), 100*(t1.crate-t2.crate),
        t1.term, t1.seq-1
        from future t1 inner join future t2 on t1.term = t2.term and t1.seq= t2.seq+1"""
        self.cur.execute(sql)

    def insert_impact(self):
        """向数据库中的impact表插入数据"""
        sql1 = """
        select t1.dt, t1.code, t1.term, t1.price, t1.mg_price, t1.pay_times, t2.code, t2.dt, t2.rate, t3.yield, 
        t4.dt_pay,t1.bond_type 
        from tb_pri t1 inner join tb_pri t2 inner join tb_sec t3 inner join appendix1 t4
        on t1.code = t3.code and t2.code = t3.code0 and t1.code = t4.code and t3.seq = 0
        where t1.bond_type = '国债' and t1.mg_price is not null
        """
        data1 = Data(sql1, self.cur).data
        data = []
        for d in data1:
            if d[3] is None:
                continue
            bond = BondYTM(d[2], d[8], d[7], d[5])
            price = d[3]
            mg_price = d[4]
            if d[2] in [3]:
                a = 0.05  # 2年期国债返费为5分钱
            elif d[2] in [5, 7, 10, 30]:
                a = 0.1  # 5、7、10、30年期国债返费为0.1元
            else:
                a = 0
            ytm = bond.bond_ytm(d[10], price - a)
            mg_ytm = bond.bond_ytm(d[10], mg_price - a)
            dd = ([d[0], d[1], d[6], d[2], 100 * (ytm - d[9]), 100 * (mg_ytm - d[9]), d[11]],)
            data.append(dd)
        sql2 = "insert into impact values %s"
        self.cur.executemany(sql2, data)

    def insert(self, table=None):
        if table:
            eval(r"self.insert_{}()".format(table))
        else:
            for t in ["tb_sec_delta", "future_delta", "impact"]:
                eval(r"self.insert_{}()".format(t))
        self.db.commit()


def main():
    data_path = r"f:\reports\my report\report1\数据"  # excel数据文件存放路径
    # data_path = r"C:\Users\daidi\Documents\我的研究报告\利率债一级市场与二级市场关系研究\数据"  # excel数据文件存放路径
    db = pymysql.connect("localhost", "root", "root", charset="utf8")
    cur = db.cursor()
    # w.start()
    create_database(cur, "impact")
    # e2db = Excel2DB(data_path, db, cur)
    # years = range(2013, 2020)
    db2self = DB2self(db, cur)
    # w2db = Wind2DB(db, cur)

    try:
        # e2db.insert(years)
        # e2db.update()
        # e2db.update_mg_rate()
        # e2db.update_price()
        # e2db.update_mg_price()
        # w2db.insert("future_minute")
        # db2self.create_function("imp_dprice")
        db2self.insert("impact")
    finally:
        cur.close()
        db.close()


if __name__ == "__main__":
    main()