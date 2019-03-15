# 本文件用于从中金所官网、中国货币网、Wind等提取周报所需数据并写入数据库
# 创建者：季俊男
# 创建日期：20190308
# 更新日期：20190308

import requests
import pymysql
from bs4 import BeautifulSoup as bs
from WindPy import w
import datetime as dtt


def create_tables(cur: pymysql.cursors, table):
    """"创建数据库内的各种表"""
    sql_his_shibor3ms = """
    create table if not exists his_shibor3ms(
    `dtt` datetime not null comment '时间',
    `curveType` char(15) not null comment '曲线类型，包括定盘曲线（fix），行情曲线（quotes)，收盘曲线（close)',
    `askBidType` char(15) not null comment '价格类型，包括均价（avg），报买（ask），报卖（bid）',
    `6M` float(6, 4) not null,
    `9M` float(6, 4) not null,
    `1Y` float(6, 4) not null, 
    `2Y` float(6, 4) not null,
    `3Y` float(6, 4) not null,
    `4Y` float(6, 4) not null,
    `5Y` float(6, 4) not null,
    `7Y` float(6, 4) not null,
    `10Y` float(6, 4) not null,
    constraint pk primary key(dtt, curveType, askBidType)
    )ENGINE=InnoDB DEFAULT CHARSET = utf8MB3 COMMENT = 'Shibor 3M互换历史数据，从中国货币网爬取'
    """
    sql_his_fr007s = """
    create table if not exists his_fr007s(
    `dtt` datetime not null comment '时间',
    `curveType` char(15) not null comment '曲线类型，包括定盘曲线（fix），行情曲线（quotes)，收盘曲线（close)',
    `askBidType` char(15) not null comment '价格类型，包括均价（avg），报买（ask），报卖（bid）',
    `1M` float(6, 4) not null,
    `3M` float(6, 4) not null,
    `6M` float(6, 4) not null,
    `9M` float(6, 4) not null,
    `1Y` float(6, 4) not null, 
    `2Y` float(6, 4) not null,
    `3Y` float(6, 4) not null,
    `4Y` float(6, 4) not null,
    `5Y` float(6, 4) not null,
    `7Y` float(6, 4) not null,
    `10Y` float(6, 4) not null,
    constraint pk primary key(dtt, curveType, askBidType)
    )ENGINE=InnoDB DEFAULT CHARSET = utf8MB3 COMMENT = 'Shibor 3M互换历史数据，从中国货币网爬取'
    """
    sql_dts1 = """
        create table if not exists dts1(
        `dt` date not null primary key comment '中金所交易日期',
        `seq` int not null comment '交易日期顺序'
        )ENGINE=InnoDB DEFAULT CHARSET = utf8MB3 COMMENT = '中金所交易日期'
        """
    sql_positions = """
        create table if not exists positions(
        `dt` date not null comment '中金所交易日期',
        `name` char(10) not null comment '期货公司',
        `contract` char(10) not null comment '期货合约',
        `volume` int default null comment '成交量',
        `volume_delta` int default null comment '成交量较上一日变化',
        `volume_rank` int default null comment '成交量排名',
        `buy` int default null comment '持买单量',
        `buy_delta` int default null comment '持买单量较上一日变化',
        `buy_rank` int default null comment '持买单量排名',
        `sell` int default null comment '持卖单量',
        `sell_delta` int default null comment '持卖单量较上一日变化',
        `sell_rank` int default null comment '持卖单量排名',
        `net` int default null comment '净持仓',
        `net_delta` int default null comment '净持仓较上一日变化',
        `net_rank` int default null comment '净持仓排名',
        constraint pk primary key(dt, name, contract)
        ) ENGINE=InnoDB DEFAULT CHARSET = utf8MB3 COMMENT = '期货公司持仓信息表'
        """
