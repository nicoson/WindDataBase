#!/usr/bin/python
# -*- coding: UTF-8 -*-
# use for get index for contract
#
# sample:
# > python3 main_futureindex.py
# ==============================================================================

import argparse
from WindPy import *
from DBConnection import *
from WindConnection import *
import datetime,time,re
# import numpy as np

def currentTime():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# main process
def main():
    ws = WindStock()
    # get category list
    symbols = ws.getCateFutureCodes()    # for history data codes
    symbols = list(filter(lambda sym:sym.find('(') == -1, symbols))
    # symbols = symbols[0:3]    # test case

    # create tables for new category
    db = DBConnect("localhost","root","root","future_l2")   # database for level 2 data for future
    db_mc = DBConnect("localhost","root","root","securityindex")   # database for main contract
    #db_mc.createUpdateLogTable4MainContract()
    db_mc.createContractIndexTables(symbols)

    # update database by using [updatelog] table
    print(currentTime(),"==================> start generating main contract: ")
    for symbol in symbols:
        generateindex(symbol, db, db_mc)

    # job finished, close the db connection
    db.destroy()
    db_mc.destroy()

def sortTableList(tlist):
    tlist.sort()
    ind = 0
    for i in range(len(tlist)):
        if re.match(r'[a-zA-Z]{1,3}[9]\d{3}\.\w{2,3}', tlist[i]) == None:
            continue
        ind = i
        break
    tlist = tlist[ind:] + tlist[:ind]
    return tlist

def getConvertTable(symbol, db):
    data = db.getContractDataBySymbol(symbol)
    if len(data) > 0:
        # [date, price, volumn, turnover amount, open interest]
        data = list(map(lambda x : [x[1]] + [x[5]] + [x[6] if x[6] != None else 0] + [x[7] if x[7] != None else 0] + [x[8] if x[8] != None else 0], data))
        datum = 0
        for i in data:  # auto fill close price
            if i[1] == None:
                i[1] = datum
            else:
                datum = i[1]
    else:
        data = None
    return data

def generateindex(symbol, db, db_mc):
    # =========================================
    # step 1: 
    #   get the related contracts from L2 db
    # =========================================
    prefix = symbol[:symbol.find('.')].lower()
    print(prefix)

    # get the table list from l2 database based on the symbol
    tablelist = db.getTableListByName(prefix)
    # filter the case 'a' from 'ag' or 'au'
    tablelist = list(map(lambda y : y[0], filter(lambda x : re.match(r'%s\d{3,4}\.\w{2,3}' %prefix, x[0].lower()) != None, tablelist)))
    if tablelist == None:
        return
    tablelist = sortTableList(tablelist)


    # =========================================
    # step 2:
    #   generate/update contract index table
    # =========================================
    # generate main contract data list
    indexList = []
    for tb in tablelist:
        print('==========>   ', tb)
        data = getConvertTable(tb, db)
        if data == None:
            pass
        elif len(indexList) == 0:
            indexList += data
        else:
            indall = 0
            maxall = len(indexList)
            indnew = 0
            maxnew = len(data)
            flag = True
            
            while flag:
                if indexList[indall][0] < data[indnew][0]:
                    indall += 1
                elif indexList[indall][0] == data[indnew][0]:
                    if indexList[indall][1] == None:
                        indexList[indall][1] = indexList[indall-1][1]
                    if data[indnew][1] == None:
                        data[indnew][1] = data[indnew-1][1]

                    w1 = 2/3 * indexList[indall][2] + 1/3 * indexList[indall][4]
                    w2 = 2/3 * data[indnew][2] + 1/3 * data[indnew][4]

                    if (w1 + w2) != 0:
                        indexList[indall][1] = indexList[indall][1] * w1 / (w1 + w2) + data[indnew][1] * w2 / (w1 + w2)   # setup price
                    indexList[indall][2] = indexList[indall][2] + data[indnew][2]
                    indexList[indall][3] = indexList[indall][3] + data[indnew][3]
                    indexList[indall][4] = indexList[indall][4] + data[indnew][4]

                    indall += 1
                    indnew += 1
                else:
                    if indall <= maxall:
                        indexList = indexList[:indall] + [data[indnew]] + indexList[indall:]
                        indall += 1
                        indnew += 1
                        maxall += 1
                    pass

                if indall == maxall:
                    if indnew < maxnew:
                        indexList += data[indnew:]
                    flag = False
                elif indnew == maxnew:
                    flag = False


    print("step last: insert data")
    if len(indexList) > 0:
        print("Inserting Data for: ", symbol)
        db_mc.updateContractIndex(symbol, indexList)
        print("Insert complete")
    else:
        print("No data inserted")



if __name__ == "__main__":
    main()