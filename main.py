#!/usr/bin/python
# -*- coding: UTF-8 -*-
from WindPy import *
from DBConnection import *
from WindConnection import *

def main():
	ws = WindStock()
	# symbles = ws.getAStockCodesWind()
	symbles = ['000001.SZ','000003.SZ','600001.SH']
	symbles.append('A000000')

	db = DBConnect("localhost","root","root","astock")
	db.createTables(symbles)
	db.destroy()

if __name__ == "__main__":
    main()