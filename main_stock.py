#!/usr/bin/python
# -*- coding: UTF-8 -*-
from WindPy import *
from DBConnection import *
from WindConnection import *
import datetime,time

def currentTime():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def main():
    ws = WindStock()
    # get category list
    symbols = ws.getAStockCodesWind()
    # symbols = ['000001.SZ','000003.SZ','600001.SH']

    # create tables for new category
    db = DBConnect("localhost","root","root","astock")
    db.createUpdateLogTable()
    db.createTables(symbols)

    # update database by using [updatelog] table
    print(currentTime(),": Download A Stock Starting:")
    for symbol in symbols:
        start_date = db.getUpdateDate(symbol)
        print(currentTime(), "===========> Downloading for ", symbol, ": ")
        print("last update date: ", start_date[0].strftime("%Y-%m-%d"))
        if start_date != None:
            data = ws.getAStockData(symbol, start_date[0].strftime("%Y-%m-%d"))

            if data != None:
                print(currentTime(), "===========> Inserting Data into DB for ", symbol, ": ")
                db.insertStockData(symbol, data)
            else:
                print(symbol, " has no new data")
        else:
            print(symbol, " does not EXIST or already updated today")

    # job finished, close the db connection
    db.destroy()

if __name__ == "__main__":
    main()