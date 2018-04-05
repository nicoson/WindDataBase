#!/usr/bin/python
# -*- coding: UTF-8 -*-
# for downloading contracts from wind platform
#
# sample:
# > python3 main_contract.py --history=true
# > python3 main_contract.py --fixdata=true
# ==============================================================================

import argparse
from WindPy import *
from DBConnection import DBConnect
from WindConnection import *
import datetime,time,re

def currentTime():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def main(isHistory = False, isfix = False):
    # connect Wind service
    ws = WindStock()

    # connect Database
    db = DBConnect("localhost","root","root","future_l2")   # database for level 2 data for future

    # get category list
    if isHistory or isfix:
        symbols = ws.getFutureCodesWindAll()    # for history data codes
    else:
        symbols = ws.getFutureCodesWindOnMarket()    # for current trading data codes

    if isfix:
        symbols = filterSymbols(symbols, db)

    # symbols = symbols[0:3]    # test case

    # create tables for new category
    db.createUpdateLogTable()
    db.createFutureTables(list(map(translate, symbols)))

    # update database by using [updatelog] table
    print(currentTime(),": Download Future Index Starting:")
    for symbol in symbols:
        windsymbol = symbol
        symbol = translate(symbol) # convert pm803.czc to pm1803.czc

        start_date = db.getUpdateDate(symbol)
        print(currentTime(), "===========> Downloading for ", symbol, ": ")
        print("last update date: ", start_date[0].strftime("%Y-%m-%d"))
        if start_date != None:
            data = ws.getFutureData(windsymbol, start_date[0].strftime("%Y-%m-%d"))

            if data != None:
                print(currentTime(), "===========> Inserting Data into DB for ", symbol, ": ")
                db.insertFutureData(symbol, data)
                print("Data insert complete successfully!")
            else:
                print(symbol, " has no new data")
        else:
            print(symbol, " does not EXIST or already updated today")

    # job finished, close the db connection
    db.destroy()


# convert ag803.czc to ag1803.czc
def translate(symbol):
    if re.match(r'[a-zA-Z]{1,3}\d{3}\.\w{2,3}', symbol) != None:
        ind = symbol.find('.')
        symbol = symbol[:ind-3] + '1' + symbol[ind-3:]
    return symbol

def filterSymbols(symbols, db):
    updateloglist = [list(data) for data in db.getUpdatelogList()]
    fixlist = []
    for sym in symbols:
        temp = list(filter(lambda x:x[0] == translate(sym), updateloglist))
        if re.match(r'[a-zA-Z]{1,3}\d{3,4}\.\w{2,3}', sym) == None:
            continue
        elif len(temp) == 0:
            fixlist += [sym]
            print('missing data: ', sym)
        else:
            tempdate = db.getLastDate(translate(sym))
            if tempdate == None or tempdate[0] == None:
                fixlist += [sym]
                print('no data: ', sym)
            # else:
            #     yd = re.search(r'\d{4}',sym).group()
            #     yddata = str(tempdate)[2:] + str(tempdate.month if tempdate.month >= 10 else '0' + str(tempdate.month))
            #     if yd != yddata:
            #         fixlist += [sym]
        
    print('Number of tables to be complement: ', len(fixlist))
    return fixlist

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='choose to load history/current data')
    parser.add_argument('--history', type=str, default = 'false')
    parser.add_argument('--fixdata', type=str, default = 'false')
    args = parser.parse_args()
    main(args.history == 'true', args.fixdata == 'true')