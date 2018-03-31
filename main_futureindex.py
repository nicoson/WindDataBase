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
    symbols = symbols[0:3]    # test case

    # create tables for new category
    db = DBConnect("localhost","root","root","future_l2")   # database for level 2 data for future
    db_mc = DBConnect("localhost","root","root","securityindex")   # database for main contract
    db_mc.createUpdateLogTable4MainContract()
    db_mc.createMainContractTables(symbols)

    # update database by using [updatelog] table
    print(currentTime(),"==================> start generating main contract: ")
    for symbol in symbols:
        generateMainContract(symbol, db, db_mc)

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
        data = list(map(lambda x : list(x[0]) + list(x[5:9]), data))
    else:
        data = None
    return data

def generateMainContract(symbol, db, db_mc):
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
    print(tablelist)

    # =========================================
    # step 2:
    #   get the history merge info from maincontract database
    # =========================================
    # get the info from updatelog to merge the main contract table
    
    # cinfo = db_mc.getCurrentMainContractInfoBySymbol(symbol)
    # if cinfo == None:
    #     ccode, lastdate = [None, None]
    # else:
    #     ccode, lastdate = cinfo
    #     try:
    #         ind = tablelist.index(ccode)
    #         tablelist = tablelist[ind:]
    #     except:
    #         print(ccode)
    #         return

    # =========================================
    # step 3:
    #   generate/update main contract table
    # =========================================
    # generate main contract data list
    indexList = []
    for tb in tablelist:
        print('==========>   ', tb)
        data = getConvertTable(tb, db)
        if len(indexList) == 0:
            indexList += data
        else:
            indall = 0
            maxall = len(indexList)
            indnew = 0
            maxnew = len(data)
            flag = True
            
            while flag:
                print(indexList[indall][0], data[indnew][0])

                if indexList[indall][0] < data[indnew][0]:
                    indall = (indall + 1) if (indall + 1) <= maxall else maxall
                elif indexList[indall][0] == data[indnew][0]:
                    w1 = 2/3 * indexList[indall][2] + 1/3 * indexList[indall][4]
                    w2 = 2/3 * data[indnew][2] + 1/3 * data[indnew][4]

                    indexList[indall][1] = indexList[indall][1] * w1 / (w1 + w2) + data[indnew][1] * w2 / (w1 + w2)   # setup price
                    indexList[indall][2] = indexList[indall][2] + data[indnew][2]
                    indexList[indall][3] = indexList[indall][3] + data[indnew][3]
                    indexList[indall][4] = indexList[indall][4] + data[indnew][4]

                    indall = (indall + 1) if (indall + 1) <= maxall else maxall
                    indnew = (indnew + 1) if (indnew + 1) <= maxnew else maxnew
                else:
                    pass

                print(indall, indnew)
                input("input:")
                if indall == maxall and indnew == maxnew:
                    flag = False

            


    #     singlebase = singlenext
    #     singlenext = getConvertTable(tb, db)

    #     if singlebase == -1:
    #         break
    #     elif singlebase == None:
    #         continue
    #     elif singlenext == None:
    #         singlenext = singlebase
    #         singlebase = None
    #     else:
    #         # todo
    #         print("step 3:")
    #         max_base = len(singlebase)
    #         max_next = len(singlenext)
    #         if lastdate == None:
    #             lastdate = singlebase[0][0]
    #             pt_base = 0
    #         else:
    #             temp = list(map(lambda x : x[0], singlebase))
    #             try:
    #                 pt_base = temp.index(lastdate) + 1 # start from the next day
    #             except:
    #                 # continue
    #                 break
    #             if pt_base >= max_base:
    #                 continue

    #         print(pt_base, max_base)
    #         nextdate = list(map(lambda x : x[0], singlenext))
    #         for i in range(pt_base, max_base):
    #             td = singlebase[i][0]
    #             try:
    #                 ind = nextdate.index(td)
    #                 if singlebase[i][7] > singlenext[ind][7]:
    #                     if i != max_base - 1:
    #                         continue
    #                 print('======>    ',singlebase[i][7],singlenext[ind][7])
    #             except:
    #                 if i != max_base-1:
    #                     continue
                
    #             if i+1 < max_base:
    #                 # main contract changed to another contract
    #                 lastdate = singlenext[ind+1][0]
    #                 maincontract += singlebase[pt_base:i+1]
    #             else:
    #                 # main contract not finished, need jump out the whole outer loop
    #                 # here is one little concern:
    #                 #   if the next contract won't be the main, and the next next one will be the main, then
    #                 #   in this case, the process will be collapsed
    #                 maincontract += singlebase[pt_base:]
    #                 singlenext = -1
    #             break

    # print("step last: insert data")
    # if len(maincontract) > 0:
    #     print("Inserting Data for: ", symbol)
    #     db_mc.updateMainContract(symbol, maincontract)
    #     print("Insert complete")
    # else:
    #     print("No data inserted")
    # # print(maincontract)
    # # break

if __name__ == "__main__":
    main()