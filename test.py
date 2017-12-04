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
    symbols = ws.getFutureCodesWind()
    print(symbols)

if __name__ == "__main__":
    main()