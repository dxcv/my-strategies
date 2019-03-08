# 本文件用于从中金所官网、中国货币网、Wind等提取周报所需数据并写入数据库
# 创建者：季俊男
# 创建日期：20190308
# 更新日期：20190308

import requests
import pymysql
from bs4 import BeautifulSoup as bs
from WindPy import w

