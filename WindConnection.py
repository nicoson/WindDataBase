#!/usr/bin/python
# -*- coding: UTF-8 -*-
from WindPy import *
import datetime,time,re

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

    # get all of the contracts in the history
    # get history data
    def getFutureCodesWindAll(self, end_date = time.strftime('%Y%m%d',time.localtime(time.time()))):
        w.start()
        # futureCodes = w.wset("sectorconstituent","sector=全部期货连续合约;field=wind_code")
        # futureCodes = w.wset("sectorconstituent","sector=全部国内商品期货合约;field=wind_code")
        futureCodes = w.wset("sectorconstituent","sector=全部国内期货合约(含已摘牌);field=wind_code")
        futureCodes = futureCodes.Data[0]
        # indexCodes = w.wset("sectorconstituent","sector=WIND商品品种指数;field=wind_code")
        # indexCodes = indexCodes.Data[0]
        # futureCodes += indexCodes
        mainCode = list(set(list(map(self.getMainCode, futureCodes))))
        return futureCodes + mainCode

    # get all of the contracts on the market now
    # for maintaining the database
    def getFutureCodesWindOnMarket(self, end_date = time.strftime('%Y%m%d',time.localtime(time.time()))):
        w.start()
        futureCodes = w.wset("sectorconstituent","sector=全部国内商品期货合约;field=wind_code")
        futureCodes = futureCodes.Data[0]
        return futureCodes

    # get all of the categories on the market
    # for maintaining the active contract
    def getCateFutureCodes(self, end_date = time.strftime('%Y%m%d',time.localtime(time.time()))):
        w.start()
        futureCodes = w.wset("sectorconstituent","sector=全部中国商品(CN);field=wind_code")
        futureCodes = futureCodes.Data[0]
        return futureCodes

    def getMainCode(self, code):
        res, num = re.subn('\d','',code)
        return res

    def getAStockData(self, symbol, start_date):
        w.start()
        try:
            stock = w.wsd(symbol, """open,high,low,close,pre_close,volume,amt,dealnum,chg,pct_chg,
                vwap,close2,turn,free_turn,oi,oi_chg,pre_settle,settle,chg_settlement,pct_chg_settlement, 
                lastradeday_s,last_trade_day,rel_ipo_chg,rel_ipo_pct_chg,susp_reason,close3,contractmultiplier,changelt,mfprice,pe_ttm,
                val_pe_deducted_ttm,pe_lyr,pb_lf,ps_ttm,ps_lyr,dividendyield2,ev,mkt_cap_ard,pb_mrq,
                pcf_ocf_ttm,pcf_ncf_ttm,pcf_ocflyr,trade_status""", start_date, (datetime.datetime.today()-datetime.timedelta(1)).strftime("%Y-%m-%d"))
            if stock.ErrorCode == 0:
                return(stock.Data)
            else:
                return None

        except Exception as e:
            print ( "XXXXXXXXXXXXXXXXXXXX    ", symbol )
            print ( "XXXXXXXXXXXXXXXXXXXX    ", datetime.datetime.now().strftime("%Y-%m-%d-%H"), ": SQL Exception :%s" % (e) )
            return None

    def getFutureData(self, symbol, start_date):
        w.start()
        end_date = self.getEndDate(symbol)
        end_date = min(datetime.datetime.today()-datetime.timedelta(1), datetime.datetime.strptime(end_date,'%Y-%m-%d'))
        print(start_date, end_date)
        try:
            stock = w.wsd(symbol, """lastradeday_s,last_trade_day,open,high,low,close,volume,amt,oi,oi_chg,
                pre_settle,settle,susp_reason,close3,contractmultiplier,changelt,
                mfprice""", start_date, end_date.strftime("%Y-%m-%d"))
            if stock.ErrorCode == 0:
                return(stock.Data)
            else:
                return None

        except Exception as e:
            print ( "XXXXXXXXXXXXXXXXXXXX    ", symbol )
            print ( "XXXXXXXXXXXXXXXXXXXX    ", datetime.datetime.now().strftime("%Y-%m-%d-%H"), ": SQL Exception :%s" % (e) )
            return None

    def getEndDate(self, symbol):
        year = re.sub(r'[a-zA-Z.]+', '', symbol)[:2]
        if year[0] == '9':
            year = int(year) + 1900
        else:
            year = int(year) + 2000
        year = str(year) + '-12-31'
        return year
