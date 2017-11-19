#!/usr/bin/python
# -*- coding: UTF-8 -*-
from WindPy import *
import datetime,time

class WindStock:
    def getCurrentTime(self):
        # 获取当前时间
        return time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))

    def getAStockCodesWind(end_date = time.strftime('%Y%m%d',time.localtime(time.time()))):
	    '''''
	    通过wset数据集获取所有A股股票代码，深市代码为股票代码+SZ后缀，沪市代码为股票代码+SH后缀。
	    如设定日期参数，则获取参数指定日期所有A股代码，不指定日期参数则默认为当前日期
	    :return: 指定日期所有A股代码，不指定日期默认为最新日期
	    '''
	    w.start()
	    #加日期参数取最指定日期股票代码
	    #stockCodes=w.wset("sectorconstituent","date="+end_date+";sectorid=a001010100000000;field=wind_code")
	    #不加日期参数取最新股票代码
	    stockCodes = w.wset("sectorconstituent","sectorid=a001010100000000;field=wind_code")
	    return stockCodes.Data[0]
	    #return stockCodes