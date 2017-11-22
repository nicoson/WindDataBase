#!/usr/bin/python
# -*- coding: UTF-8 -*-
from WindPy import *
from DBConnection import *
from WindConnection import *

def main():
	ws = WindStock()
	# get category list
	symbols = ws.getAStockCodesWind()
	symbols = ['000001.SZ','000003.SZ','600001.SH']

	# create tables for new category
	db = DBConnect("localhost","root","root","astock")
	db.createTables(symbols)
	db.destroy()

	# update database by using [updatelog] table
	print(self.getCurrentTime(),": Download A Stock Starting:")  
    for symbol in symbols:  
        w.start()  
        try:
        	stock=w.wsd(symbol, "open,high,low,close,pre_close,volume,amt,dealnum,chg,pct_chg,vwap, adjfactor,close2,turn,free_turn,oi,oi_chg,pre_settle,settle,chg_settlement,pct_chg_settlement, lastradeday_s,last_trade_day,rel_ipo_chg,rel_ipo_pct_chg,susp_reason,close3, pe_ttm,val_pe_deducted_ttm,pe_lyr,pb_lf,ps_ttm,ps_lyr,dividendyield2,ev,mkt_cap_ard,pb_mrq,pcf_ocf_ttm,pcf_ncf_ttm,pcf_ocflyr,pcf_nflyr,trade_status", start_date,end_date)  
            
        except Exception as e:
        	print ( "XXXXXXXXXXXXXXXXXXXX    ", symbol )
        	print ( "XXXXXXXXXXXXXXXXXXXX    ", datetime.now().strftime("%Y-%m-%d-%H"), ": SQL Exception :%s" % (e) )

if __name__ == "__main__":
    main()