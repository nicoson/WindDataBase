#!/usr/bin/python
# -*- coding: UTF-8 -*-
# Used for merging the contracts for get main contract
#
# sample:
# > python3 maincontract_main.py
# ==============================================================================

import argparse
from WindPy import *
from DBConnection import *
from WindConnection import *
import datetime,time,re

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
    db_mc = DBConnect("localhost","root","root","maincontract")   # database for main contract
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
    singlebase = db.getContractDataBySymbol(symbol)
    if len(singlebase) > 0:
        singlebase = list(map(lambda x : list(x[:2]) + [symbol] + list(x[2:]), singlebase))
    else:
        singlebase = None
    return singlebase

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
    cinfo = db_mc.getCurrentMainContractInfoBySymbol(symbol)
    if cinfo == None:
        ccode, lastdate = [None, None]
    else:
        ccode, lastdate = cinfo
        try:
            ind = tablelist.index(ccode)
            tablelist = tablelist[ind:]
        except:
            print(ccode)
            return

    # =========================================
    # step 3:
    #   generate/update main contract table
    # =========================================
    # generate main contract data list
    maincontract = []
    baseset = None
    nextset = None
    print('======> ', ccode, lastdate)
    for tind, tb in enumerate(tablelist):
        print('==========>   ', tb)
        nextset = getConvertTable(tb, db)

        if nextset == None:
            continue
        elif baseset == None:
            baseset = nextset
            lastindex = 0

            if lastdate != None:
                lastindex = [index for index,x in enumerate(baseset) if x[1] > lastdate]
                if len(lastindex) == 0:
                    baseset = None  # if not found then reset the baseset as None
                    continue
                else:
                    lastindex = lastindex[0]
                    baseset = baseset[lastindex:]
                    if tind+1 == len(tablelist):
                        maincontract += baseset
                    lastdate == None
            
        else:
            flag = True
            indbase = 0
            indnext = 0
            maxbase = len(baseset)
            maxnext = len(nextset)
            while flag:
                #print(indbase,maxbase,indnext,maxnext)
                #print(baseset[indbase][1], nextset[indnext][1])
                if baseset[indbase][1] < nextset[indnext][1]:
                    indbase += 1
                elif baseset[indbase][1] == nextset[indnext][1]:
                    #print(baseset[indbase][7], nextset[indnext][7])
                    if baseset[indbase][7] == None or nextset[indnext][7] == None or baseset[indbase][7] >= nextset[indnext][7]:
                        indbase += 1
                        indnext += 1
                    else:
                        maincontract += baseset[:indbase]
                        baseset = nextset[indnext:]
                        flag = False
                else:
                    indnext += 1

                if flag and indbase == maxbase:
                    maincontract += baseset
                    if indnext < maxnext:
                        baseset = nextset[indnext:]
                    elif indnext == maxnext:
                        lastdate = baseset[-1][1]
                        baseset = None
                    flag = False
                elif flag and indnext == maxnext:
                    if tind+1 == len(tablelist):
                        maincontract += baseset
                    flag = False



    print("step last: insert data")
    if len(maincontract) > 0:
        print("Inserting Data for: ", symbol)
        db_mc.updateMainContract(symbol, maincontract)
        print("Insert complete")
    else:
        print("No data inserted")
    # print(maincontract)
    # break

if __name__ == "__main__":
    main()