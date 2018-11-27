# 本文件用于对数据进行统计处理
# 作者：季俊男
# 创建日期： 2018/11/14

import pymysql
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from database import Data
from pprint import pprint


def imp_select_code(imp:list, cur):
    res = list()
    res.append(Data(r"select code from tb_sec_delta where seq=0 and delta <= %s", cur, (imp[0],)).select_col(0))
    for i in range(len(imp) - 1):
        res.append(Data(r"select code from tb_sec_delta where seq=0 and delta > %s and delta <= %s", cur,
                         (imp[i], imp[i+1])).select_col(0))
    res.append(Data(r"select code from tb_sec_delta where seq=0 and delta > %s", cur, (imp[-1],)).select_col(0))
    return res


class ImpSat(object):
    """本类用于对"""
    def __init__(self, db, cur):
        self.db = db
        self.cur = cur

    def imp_seq(self, imp: list, seq: list):
        """返回根据冲击大小与seq先后排序的二维收益率差"""
        res = list()
        sql = r"""select avg(delta) from tb_sec_delta where code in %s and seq = %s"""
        codes = imp_select_code(imp, self.cur)
        for code in codes:
            num = len(code)
            d = list([num])
            for s in seq:
                d.append(Data(sql, self.cur, (code, s)).data[0][0])
            res.append(d)
        return res

    def imp_dst_plot(self):
        """绘制发行冲击的直方分布图"""
        imp = np.array(Data(r"select delta from tb_sec_delta where seq = 0 and delta is not null",
                            self.cur).select_col(0))
        plt.hist(imp, bins=np.arange(-40, 30, 1), normed=True)
        plt.show()

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
            num = len(code)
            d = list([num])
            for s in seq:
                d.append(Data(sql, self.cur, (code, term, s)).data[0][0])
            res.append(d)
        return res




def main():
    db = pymysql.connect("localhost", "root", "root", "strategy1", charset="utf8")
    cur = db.cursor()
    imp_sat = ImpSat(db, cur)
    # res = imp_sat.imp_seq(list(range(-19, 16, 5)), list(range(6)))
    # res = imp_sat.imp_future(list(range(-19, 16, 5)), list(range(1, 6)))
    # for rs in res:
    #     print()
    #     for r in rs:
    #         print(r, end=", ")
    imp_sat.imp_dst_plot()


if __name__ == "__main__":
    main()
