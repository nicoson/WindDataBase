#!/usr/bin/python
# -*- coding: UTF-8 -*-
#
# sample:
# > python3 main_future.py --history=true
# ==============================================================================

import argparse
from WindPy import *
from DBConnection import *
from WindConnection import *
import datetime,time,re

def currentTime():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def main():
    ws = WindStock()
    # get category list
    symbols = ws.getCateFutureCodes()    # for history data codes
    symbols = list(filter(lambda sym:sym.find('(') == -1, symbols))
    # symbols = symbols[0:3]    # test case

    # create tables for new category
    db = DBConnect("localhost","root","root","future_l2")   # database for level 2 data for future

    # update database by using [updatelog] table
    print(currentTime(),"==================> start generating main contract: ")
    for symbol in symbols:
        print(symbol.find('.'))
        prefix = symbol[:symbol.find('.')].lower()
        tablelist = db.getTableListByName(prefix)
        tablelist = list(filter(lambda x : re.match(r'%s\d{3,4}\.\w{2,3}' %prefix, x[0].lower()) != None, tablelist))
        print(tablelist)
        break

    # job finished, close the db connection
    db.destroy()

if __name__ == "__main__":
    main()