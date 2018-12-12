# 本文件用于对数据进行统计处理
# 作者：季俊男
# 创建日期： 2018/11/14

import pymysql
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from database import Data
import statsmodels.api as sm
from pylab import mpl


def imp_select_code(imp:list, cur, column="delta"):
    res = list()
    res.append(Data(r"select code from tb_sec_delta where seq=0 and {} <= %s".format(column),
                    cur, (imp[0],)).select_col(0))
    for i in range(len(imp) - 1):
        res.append(Data(r"select code from tb_sec_delta where seq=0 and {} > %s and delta <= %s".format(column),
                        cur, (imp[i], imp[i+1])).select_col(0))
    res.append(Data(r"select code from tb_sec_delta where seq=0 and {} > %s".format(column),
                    cur, (imp[-1],)).select_col(0))
    return res


class ImpSat(object):
    """本类用于对一级市场冲击统计"""
    def __init__(self, db, cur):
        self.db = db
        self.cur = cur

    def get_avg_std_by_term(self, terms, column="delta"):
        """计算不同期限的发行冲击的均值与标准差"""
        res = []
        sql = "select count(*), avg({0}), stddev_samp({0}) from tb_sec_delta " \
              "where seq = 0 and term = %s".format(column)
        for t in terms:
            num, avg, std = Data(sql, self.cur, (t,)).data[0]
            res.append([num, avg, std])
        return res

    def imp_seq(self, imp: list, seq: list, column="delta"):
        """返回根据冲击大小与seq先后排序的二维收益率差"""
        res = list()
        sql = r"""select avg({}) from tb_sec_delta where code in %s and seq = %s""".format(column)
        codes = imp_select_code(imp, self.cur, column=column)
        for code in codes:
            num = len(code)
            d = list([num])
            for s in seq:
                d.append(Data(sql, self.cur, (code, s)).data[0][0])
            res.append(d)
        return res

    def imp_hist_plot(self):
        """绘制发行冲击的直方分布图，分别使用收益率变动与净价变动来衡量"""
        fig, axes = plt.subplots(2, 2, figsize=(9.6, 4.8))
        fig.subplots_adjust(hspace=0.5)
        i = 0
        for bondtype in ["00", "02"]:
            j = 0
            for column in ["delta", "dprice"]:
                sql = r"""select {0} from tb_sec_delta where seq = 0 and {0} is not null and 
                           code0 regexp '[:alnum:]{{2}}{1}.*'""".format(column, bondtype)
                imp = np.array(Data(sql, self.cur).select_col(0))
                if column == "delta":
                    if bondtype == "00":
                        bins = np.arange(-40, 30, 1)
                        title = "国债发行冲击(BP)分布图"
                    elif bondtype == "02":
                        bins = np.arange(-40, 30, 1)
                        title = "国开债发行冲击(BP)分布图"
                    else:
                        raise ValueError("错误的参数值bondtype")
                elif column == "dprice":
                    if bondtype == "02":
                        bins = np.arange(-2, 4, 0.01)
                        title = "国开债发行冲击(元)分布图"
                    elif bondtype == "00":
                        bins = np.arange(-2, 4, 0.01)
                        title = "国债发行冲击(元)分布图"
                    else:
                        raise ValueError("错误的参数值bondtype")
                else:
                    raise ValueError("错误的参数值column")
                axes[i, j].hist(imp, bins=bins)
                axes[i, j].set_title(title, fontproperties="SimHei", fontsize=20)
                j += 1
            i += 1
        fig.show()

    def imp_future(self, imp:list, seq:list, term=10, delta="dsrate"):
        """"发行冲击对国债期货市场影响"""
        res = list()
        sql0 = """
        select count(*), avg(t1.delta) from tb_sec_delta t1 inner join future_delta t2
        on t1.dt = t2.dt
        where t1.code in %s and t2.term = %s and t1.seq = 0
        """
        sql = """
        select avg(t2.{})
        from tb_sec_delta t1 left outer join future_delta t2
        on t1.dt = t2.dt
        where t1.code in %s and t2.term = %s and t1.seq = %s
        """.format(delta)
        codes = imp_select_code(imp, self.cur)
        for code in codes:
            data_imp = Data(sql0, self.cur, (code, term)).data
            num = data_imp[0][0]
            avg_imp = data_imp[0][1]
            d = list([num, avg_imp])
            for s in seq:
                d.append(Data(sql, self.cur, (code, term, s)).data[0][0])
            res.append(d)
        return res

    def imp_delta_plot(self):
        sql = r"""
        select t1.delta, t2.delta from tb_sec_delta t1 inner join tb_sec_delta t2
        on t1.code = t2.code and t2.seq = t1.seq + %s
        where t1.seq = 0 and t1.delta is not null and t2.delta is not null
        and t1.delta between -30 and 20
        """
        sql0 = r"select delta from tb_sec_delta where seq = 0 and delta is not null"
        imp = np.array(Data(sql0, self.cur).select_col(0))
        fig = plt.figure(figsize=(7.2, 9.6))
        plt.subplot(321)
        plt.hist(imp, bins=np.arange(-40, 30, 1), normed=True)
        plt.title("一级发行冲击分布图", fontproperties="SimHei")
        plt.xlabel("发行冲击（bp)", fontproperties="SimHei")
        for i in range(2, 7):
            data = np.array(Data(sql, self.cur, (i-1,)).data)
            eval("plt.subplot(32{})".format(i))
            X = sm.add_constant(data[:, 0])
            Y = data[:, 1]
            model = sm.OLS(Y, X)
            res = model.fit()
            formula_string = "y = {:.4f} + {:.4f}*x".format(*res.params)
            line_x = [-30, 20]
            line_y = res.predict(sm.add_constant(line_x))
            plt.scatter(data[:, 0], data[:, 1], s=0.5)
            plt.plot(line_x, line_y, color='black')
            plt.text(line_x[0], 6, formula_string)
            plt.title("发行第{}天的收益变动散点图".format(i-1), fontproperties="SimHei")
            plt.xlabel("发行冲击（bp）", fontproperties="SimHei")
            plt.ylabel("二级市场收益率变动（bp)", fontproperties="SimHei")
            plt.xlim(*line_x)
            plt.ylim(-7, 7)
        plt.show()

    def imp_and_trend(self):
        sql1 = r"""select dt, rate from tb_rate where term = 10 and bond_type ='国债'"""
        data = Data(sql1, self.cur)
        dt = np.array(data.select_col(0))
        rate = np.array(data.select_col(1))
        fig, axes = plt.subplots(1, 1, figsize=(12, 3), sharex=True)
        axes.spines["top"].set_color('none')
        axes.spines["right"].set_color("none")
        axes.xaxis.set_ticks_position("bottom")
        axes.yaxis.set_ticks_position("left")
        axes.spines["bottom"].set_position(('data', 3))
        axes.spines["left"].set_position(('outward', 0))
        axes.plot(dt, rate)
        fig.show()


def main():
    mpl.rcParams['font.sans-serif'] = ['SimHei']
    db = pymysql.connect("localhost", "root", "root", "strategy1", charset="utf8")
    cur = db.cursor()
    imp_sat = ImpSat(db, cur)
    imp_sat.imp_and_trend()
    # res = imp_sat.imp_seq(list(range(-19, 16, 5)), list(range(6)))
    # res = imp_sat.imp_future(list(range(-19, 16, 5)), list(range(1, 6)), term=5)
    # for rs in res:
    #     print()
    #     for r in rs:
    #         print(r, end=", ")
    # imp_sat.imp_delta_plot()


if __name__ == "__main__":
    main()
