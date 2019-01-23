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
from matplotlib.ticker import  MultipleLocator
from matplotlib.ticker import  FormatStrFormatter


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
        sql0 = r"""select dt, rate from tb_rate where term = 10 and bond_type ='国债'"""
        data0 = Data(sql0, self.cur)
        dt = np.array(data0.select_col(0))
        rate = np.array(data0.select_col(1))
        fig, axes = plt.subplots(3, 1, figsize=(12, 6), sharex="all", gridspec_kw={'height_ratios': [3, 1.5, 1.5]})
        # 图1，十年国债到期收益率
        axes[0].spines["top"].set_color('none')
        axes[0].spines["right"].set_color("none")
        axes[0].xaxis.set_ticks_position("bottom")
        axes[0].yaxis.set_ticks_position("left")
        axes[0].plot(dt, rate, label="十年国债收益率")
        axes[0].spines["bottom"].set_position(('data', 3))
        axes[0].legend(fontsize=15)
        # 图2，国债发行冲击
        sql1 = r"""select dt, delta from tb_sec_delta where code regexp '[:alnum:]{2}00.*' and seq = 0"""
        data1 = Data(sql1, self.cur)
        dt1 = np.array(data1.select_col(0))
        delta1 = np.array(data1.select_col(1))
        axes[1].bar(dt1, delta1, label="续发国债冲击（BP)")
        axes[1].set_ylim(-10, 10)
        axes[1].legend(fontsize=10, loc="upper left")
        # 图3，国开债发行冲击
        sql2 = r"""select dt, delta from tb_sec_delta 
        where code regexp '[:alnum:]{2}02.*' and seq = 0
        and delta is not null"""
        data2 = Data(sql2, self.cur)
        dt2 = np.array(data2.select_col(0))
        delta2 = np.array(data2.select_col(1))
        axes[2].bar(dt2, delta2, label="续发国开债冲击（BP）")
        axes[2].set_ylim(-20, 10)
        axes[2].legend(fontsize=10, loc="upper left")
        fig.show()


class ImpFuture(object):
    """用于统计发行冲击后的国债期货表现"""
    def __init__(self, cur, db):
        self.cur = cur
        self.db = db

    def imp_days(self, bond_type, future_type):
        """将发行冲击五等分，计算之后4日的国债期货收益均值，参数bond_type为续发债类型，分别为
        国债和国开债，future_type为国债期货合约类型("TF"或者"T")"""
        if future_type == "TF":
            future_term = 5
        elif future_type == "T":
            future_term = 10
        else:
            raise ValueError("不被接受的参数值future_term")
        sql1 = """select t1.delta, t2.dsrate, t3.dsrate, t4.dsrate, t5.dsrate, t6.dsrate, t7.dsrate, t8.dsrate
        from impact t1 inner join future_delta t2 inner join future_delta t3 inner join future_delta t4
        inner join future_delta t5 inner join future_delta t6 inner join future_delta t7 inner join future_delta t8
        on t1.dt = t5.dt and t2.seq = t5.seq - 3 and t3.seq = t5.seq - 2 and t4.seq = t5.seq - 1
        and t6.seq = t5.seq+1 and t7.seq = t5.seq+2 and t8.seq = t5.seq +3
        and t2.term = t5.term and t3.term = t5.term and t4.term = t5.term and t6.term = t5.term  
        and t7.term = t5.term and t8.term = t5.term
        and t1.bondtype = %s and t5.term = %s
        order by t1.delta
        """
        data = Data(sql1, self.cur, (bond_type, future_term)).data
        data = pd.DataFrame(np.array(data))
        # 依据delta将data五等分
        n = 5
        res = []
        l = int(len(data)/n)
        for i in range(n):
            a = i*l
            b = (i+1)*l-1
            if i == 4:
                b = -1
            d = list(data[a:b].mean())
            d.insert(1, len(data[a:b]))
            res.append(d)
        return res

    def imp_minutes(self, bond_type, future_type, day=0):
        """计算发行冲击当日的五分钟级的市场走势"""
        if future_type == "TF":
            future_term = 5
        elif future_type == "T":
            future_term = 10
        else:
            raise ValueError("不被接受的参数值future_type")
        # 获得delta五等分点
        sql1 = """select t1.delta from impact t1 inner join future_minute t2
                  on t1.dt = date(t2.dtt) and t2.seq=0
                  where t1.bondtype = %s and t2.term = %s
                  """
        delta = np.array(Data(sql1, self.cur, (bond_type, future_term)).data)
        delta = pd.DataFrame(delta, columns=["delta"]).dropna()
        per_delta = [float(delta.min()-1)]
        for p in range(20, 120, 20):
            per_delta.append(float(np.percentile(delta, p)))
        # 根据五等分点（per_delta)从数据库中选出每个分位的
        data = []
        if day == 0:
            sql2 = r"""select date_format(t2.dtt, '%%H:%%i'), avg(t2.rate) from impact t1 
            inner join future_minute t2
            on t1.dt = date(t2.dtt) and t1.bondtype = %s and t2.term = %s 
            and t1.delta > %s and t1.delta <= %s
            group by date_format(t2.dtt, '%%H:%%i')"""
        elif day > 0:
            sql2 = r"""select date_format(t2.dtt, '%%H:%%i'), avg(t2.rate) from impact t1 
            inner join future_minute t2 inner join dts1 t3 inner join dts1 t4
            on t1.dt = t3.dt and t4.seq = t3.seq +{} and t4.dt = date(t2.dtt)
            where t1.bondtype = %s and t2.term = %s and t1.delta > %s and t1.delta <= %s
            group by date_format(t2.dtt, '%%H:%%i')
            """.format(day)
        else:
            sql2 = r"""select date_format(t2.dtt, '%%H:%%i'), avg(t2.rate) from impact t1 
            inner join future_minute t2 inner join dts1 t3 inner join dts1 t4
            on t1.dt = t3.dt and t4.seq = t3.seq - {} and t4.dt = date(t2.dtt)
            where t1.bondtype = %s and t2.term = %s and t1.delta > %s and t1.delta <= %s
            group by date_format(t2.dtt, '%%H:%%i')
            """.format(abs(day))
        for i in range(len(per_delta)-1):
            a = per_delta[i]
            b = per_delta[i+1]
            da = Data(sql2, self.cur, (bond_type, future_term, a, b))
            time_index = da.select_col(0)
            rate = da.select_col(1)
            data.append(rate)
        data = np.array(data).T
        res = []
        for k in range(1, len(data), 1):
            res.append(100*(data[k] - data[0]))
        res = pd.DataFrame(res, index=time_index[1:], columns=["一", "二", "三", "四", "五"])
        return res

    def imp_minutes_plot(self, day=0):
        """将利率债发行对国债期货市场的影响可视化，即分别以国债-TF、国债-T、国开债-TF、国开债-T作为参数
        计算imp_minutes，并将结果放入一张4×1的图中"""
        imp_minutes_params = [("国债", "TF"), ("国债", "T"), ("国开债", "TF"), ("国开债", "T")]
        fig, axes = plt.subplots(4, 1, figsize=(8,12), sharex="all", )
        xmajorLocator = MultipleLocator(4)
        for params, ax in zip(imp_minutes_params, axes):
            data =  self.imp_minutes(*params, day)
            ax.spines["top"].set_color("none")
            ax.spines["right"].set_color("none")
            ax.xaxis.set_ticks_position("bottom")
            ax.yaxis.set_ticks_position("left")
            labels = ["一", "二", "三", "四", "五"]
            for i in range(len(labels)):
                ax.plot(data.index, data.iloc[:, i], label=labels[i])
            ax.spines["bottom"].set_position(('data', 0))
            ax.xaxis.set_major_locator(xmajorLocator)
            ax.set_title("{}-{}".format(*params))
            ax.legend(loc="best")
        fig.show()

def main():
    mpl.rcParams['font.sans-serif'] = ['SimHei']
    plt.rcParams['axes.unicode_minus'] = False
    db = pymysql.connect("localhost", "root", "root", "strategy1", charset="utf8")
    cur = db.cursor()
    imp_future = ImpFuture(cur, db)
    res = imp_future.imp_days("国债", "TF")
    # imp_future.imp_minutes_plot(3)
    # imp_sat = ImpSat(db, cur)
    # imp_sat.imp_and_trend()
    # res = imp_sat.imp_seq(list(range(-19, 16, 5)), list(range(6)))
    # res = imp_sat.imp_future(list(range(-19, 16, 5)), list(range(1, 6)), term=10)
    for rs in res:
        print()
        for r in rs:
            print(round(r, 4), end=" ")
    # imp_sat.imp_delta_plot()


if __name__ == "__main__":
    main()
