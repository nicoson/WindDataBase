#!/usr/bin/python
# -*- coding: UTF-8 -*-
from WindPy import *
from DBConnection import *
from WindConnection import *

def main():
	ws = WindStock()
	symbles = ws.getAStockCodesWind()
	# symbles = ['000001.sz','600000.sh']

	db = DBConnect("localhost","root","root","astock")
	db.createTables(symbles)
	db.createUpdateLogTable()
	db.destroy()

if __name__ == "__main__":
    main()