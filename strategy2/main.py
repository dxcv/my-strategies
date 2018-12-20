# 项目strategy2用于进行利率互换做市报价，依据市场最新的报价或者成交信息来获得各期限利率互换的合适报价
# 创建者：季俊男
# 创建日期：2018/12/20

import pymysql
import numpy as np
import pandas as pd
from scipy import optimize
import datetime as dtt
from WindPy import w
import statsmodels.api as sm
import matplotlib.pyplot as plt


class WindIrsData(object):
    """从WIND中提取利率互换收盘价数据"""
    def __init__(self, type):
        """存储不同种类的IRS代码，主要期限为9M, 1Y, 2Y, 3Y, 4Y, 5Y"""
        if type == "FR007":
            self.codes = ["M0218007", "M0218008", "M0218009", "M0218010", "M0218011", "M0218012"]
            self.columns = ["FR007S9M", "FR007S1Y", "FR007S2Y", "FR007S3Y", "FR007S4Y", "FR007S5Y"]
            self.outliers = [dtt.date(2015, 5, 22)]
        elif type == "SHIBOR3M":
            self.codes = ["M0218016", "M0218017", "M0218018", "M0218019", "M0218020", "M0218021"]
            self.columns = ["SHI3MS9M", "SHI3MS1Y", "SHI3MS2Y", "SHI3MS3Y", "SHI3MS4Y", "SHI3MS5Y"]
            self.outliers = [dtt.date(2015, 5, 6), dtt.date(2015, 6, 4), dtt.date(2015, 6, 10), dtt.date(2015, 9, 21)]
        else:
            raise ValueError("不被接受的参数值type")
        self.wdata = None
        self.times = None
        self.data = None
        self.df = None

    def extract(self, dt1=dtt.date(2015, 1, 1), dt2=dtt.date(2018, 12, 17)):
        "从Wind中提取相应品种利率互换的日收盘价数据"
        wData = w.edb(self.codes, dt1, dt2)
        if wData.ErrorCode == 0:
            self.wdata = wData
            self.times = wData.Times
            self.data = wData.Data
            self.df = pd.DataFrame(list(zip(*self.data)), index=pd.to_datetime(self.times), columns=self.columns)
            self.df.drop(self.outliers, inplace=True)
        else:
            raise ValueError("{}".format(wData.ErrorCode))

    def spreads(self):
        """分别返回9M*1Y，1*5Y，2*5Y，3*5Y，4*5Y的利差数据"""
        spreads = [self.df[self.columns[1]] - self.df[self.columns[0]],
                   self.df[self.columns[5]] - self.df[self.columns[1]],
                   self.df[self.columns[5]] - self.df[self.columns[2]],
                   self.df[self.columns[5]] - self.df[self.columns[3]],
                   self.df[self.columns[5]] - self.df[self.columns[4]]]
        res = pd.concat(spreads, axis=1)
        res.columns = ["9M*1Y", "1*5Y", "2*5Y", "3*5Y", "4*5Y"]
        return res


class Stats(object):
    """本类用于对利率互换期限利差之间的相互关系进行统计"""
    def __init__(self, type="FR007", dt1=dtt.date(2015, 1, 1), dt2=dtt.date(2018, 12, 17)):
        """初始化，type为利率互换基础利率品种，dt1为样本开始日期，dt2为样本结束日期"""
        self.type = type
        self.dt1 = dt1
        self.dt2 = dt2
        self.wid = WindIrsData(self.type)
        self.wid.extract(dt1, dt2)
        self.spreads = self.wid.spreads()

    def get_params(self, dt1, dt2):
        """以1*5Y为解释变量，分别以9M*1Y，2*5Y，3*5Y，4*5Y为被解释变量进行单元线性回归（不带常数项），返回解释变量的系数"""
        params = []
        spreads = self.spreads.loc[(self.spreads.index > dt1) & (self.spreads.index <= dt2)]
        fields = ["9M*1Y", "2*5Y", "3*5Y", "4*5Y"]  # 因变量字段名
        field = "1*5Y"  # 自变量字段名
        for f in fields:
            data = spreads[[field, f]].dropna()
            x = data.iloc[:, 0]
            y = data.iloc[:, 1]
            model = sm.OLS(y, x)
            res = model.fit()
            params.append(res.params[0])
        return params

    def roll_params(self, roll_period):
        """使用滚动的方式计算单元线性回归参数，样本期为roll_period"""
        res = []
        dts = self.spreads.index
        for i in range(len(dts) - roll_period):
            dt1 = dts[i]
            dt2 = dts[i + roll_period]
            params = self.get_params(dt1, dt2)
            res.append(params)
        res = pd.DataFrame(res, index=pd.to_datetime(dts[roll_period:]), columns=["9M*1Y", "2*5Y", "3*5Y", "4*5Y"])
        return res


class IrsModel(object):
    """IrsModel类用于创建对利率互换进行定价的模型，模型的主要原理是依据某个期限点的最新的市场合理报价与成交来确定其他
    期限利率互换的合理报价，模型藉由一个维度N来进行初始化，N表示模型应当包括的期限个数，例如模型只对1Y, 2Y, 3Y, 4Y, 5Y
    这5个期限进行报价，那么N=5。初始化类时还可以提供参数矩阵B，如果不提供则需要调用estimate_B方法来对B进行估计"""
    def __init__(self, N, R0=None, B=None):
        self.N = N
        self.B = B
        self.R = R0

    def reset_R(self, R):
        """重置R，设置该方法的目的是为了能够随时按照需求来重置实例的R值，方便使用"""
        self.R = R

    def B_Matrix(self, x):
        """本方法生成一个函数，接受(N)*(N-1)个参数(即x），生成一个N*N矩阵，其对角元素均为1，该矩阵其实就是参数矩阵B，创建该
        函数的原因是为了进一步构建最小二乘函数用来估计B中的(N)*(N-1)个参数"""
        B = np.eye(self.N)
        i = 0
        for j in range(self.N):
            for k in range(self.N):
                if j == k:
                    continue
                else:
                    B[j, k] = x[i]
                    i += 1
        return B

    def receive_X(self, Xt):
        """receive方法用于接收最新的价格信息Xt，并根据Rt-1计算出价格冲击It"""
        It = np.matrix(np.zeros(self.N)).T
        index = Xt != 0
        it = Xt[index] - self.R[index]
        It[index] = it
        return It

    def It2Rt(self, It, output=False):
        """根据t时刻的价格冲击It计算各期限估值Rt"""
        self.R = self.R + np.dot(self.B, It)
        if output:
            return self.R

    def Xt2Rt(self, Xt):
        """直接由观测值Xt计算Rt"""
        It = self.receive_X(Xt)
        self.It2Rt(It)

    def estimate_B(self, R0, sample):
        """estimate_B方法用于估计参数矩阵，首先根据最小二乘法构建目标函数，目标函数是一个多元二次函数，可使用牛顿迭代法
        来计算最小值，参数R0表示初始利率值，sample表示样本，即历史的价格信息流，{Xt}的时间序列"""
        self.R = R0 # 用初始值重置R
        def f(x, sample):
            res = 0
            B = self.B_Matrix(x)
            for X in sample:
                I = self.receive_X(X)
                self.It2Rt(I)
                res += I.T * I[0, 0]
            return res






