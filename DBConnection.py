#!/usr/bin/python
# -*- coding: UTF-8 -*-
import pymysql
import datetime,time

class DBConnect:
	def __init__(self, server, user, psd, database):
		self.server		= server
		self.user		= user
		self.psd		= psd
		self.database	= database
		self.logTable	= 'updateLog'
		self.db = pymysql.connect(server, user, psd, database, charset='utf8mb4')
		# 使用 cursor() 方法创建一个游标对象 cursor
		self.cursor = self.db.cursor()
		# create log table for update history
		self.createUpdateLogTable()

	def createSingleTable(self, symbol):
		# symbol = symbol
		# symbol = symbol.split('.')
		# symbol.reverse()
		# symbol = "".join(symbol)
		# 使用预处理语句创建表
		sql = "CREATE TABLE IF NOT EXISTS `" + symbol + """` (
			ID bigint(20) primary key NOT NULL auto_increment,
			trade_date date NULL COMMENT '交易日期',
			open double DEFAULT NULL COMMENT '开盘价',
			high double DEFAULT NULL COMMENT '最高价',
			low double DEFAULT NULL COMMENT '最低价',
			close double DEFAULT NULL COMMENT ' 收盘价,证券在交易日所在指定周期的最后一条行情数据中的收盘价',
			volume double DEFAULT NULL COMMENT '成交量',
			amt double DEFAULT NULL COMMENT '成交金额',
			pct_chg double DEFAULT NULL COMMENT '涨跌幅=(收盘价/前收价-1)*100%',
			dealnum double DEFAULT NULL COMMENT '成交笔数',
			pre_close double DEFAULT NULL COMMENT '证券在交易日所在指定周期的首个前收盘价',
			chg double DEFAULT NULL COMMENT '涨跌=收盘价-前收价',
			swing double DEFAULT NULL COMMENT '振幅=[(最高价-最低价)／前收盘价]*100%',
			vwap double DEFAULT NULL COMMENT '均价=成交金额/成交量',
			adj_factor double DEFAULT NULL COMMENT 'Ext=X0*X1*...*Xt-1*Xt	Ext为T日分红复权因子	X0=1	Xt=Pt-1/Pext,其中Pext为T日前收盘价，Pt-1为T日前一个交易日收盘价。',
			close2 double DEFAULT NULL COMMENT ' 收盘价，该指标支持定点复权，当复权方式选择为定点复权时，复权基期传入有效；否则无效。',
			turn double DEFAULT NULL COMMENT '换手率=成交量/流通股本*100%',
			free_turn double DEFAULT NULL COMMENT ' 换手率(自由流通股本)=成交量/自由流通股本*100%',
			ev double DEFAULT NULL COMMENT '上市公司的股权公平市场价值。对于一家多地上市公司，区分不同类型的股份价格和股份数量分别计算类别市值，然后加总',
			mkt_cap_ard double DEFAULT NULL COMMENT '按指定证券价格乘指定日总股本计算上市公司在该市场的估值。该总市值为计算PE、PB等估值指标的基础指标。暂停上市期间或退市后该指标不计算。',
			pe_ttm double DEFAULT NULL COMMENT '分子=最近交易日收盘价*最新普通股总股数分母=归属母公司股东的净利润(TTM)*最近交易日转换汇率(记帐本位币转换为交易币种)返回=分子／分母',
			val_pe_deducted_ttm double DEFAULT NULL COMMENT '扣非后的市盈率(TTM)=总市值/前推12个月扣除非经常性损益后的净利润',
			pe_lyr double DEFAULT NULL COMMENT ' 每股股价为每股收益的倍数。可回测的估值指标。	总市值2/归属母公司股东净利润（LYR) 注： 1、总市值2=指定日证券收盘价*指定日当日总股本2、B股涉及汇率转换',
			pb_lf double DEFAULT NULL COMMENT '普通股每股市价为每股净资产的倍数。 总市值2／指定日最新公告股东权益(不含少数股东权益)注： 1、总市值2=指定日证券收盘价*指定日当日总股本2、B股涉及汇率转换',
			pb_mrq double DEFAULT NULL COMMENT '每股股价为每股净资产的倍数。可回测的估值指标。总市值2/归属母公司股东的权益（MRQ)如财务报表币种与市值币种不同，则财务报表数据按报告期截止日汇率转换为市值币种。',
			ps_ttm double DEFAULT NULL COMMENT '分子=最近交易日收盘价*最新普通股总股数 ;分母=销售收入(TTM)*最近交易日转换汇率(记帐本位币转换为交易币种); 返回=分子／分母',
			ps_lyr double DEFAULT NULL COMMENT '每股股价为每股营业收入(LYR)的倍数。可回测的估值指标。总市值2/营业收入(LYR) 注：1、总市值2=指定日证券收盘价*指定日当日总股本 2、B股涉及汇率转换',
			dividendyield2 double DEFAULT NULL COMMENT '股息率，也称股票获利率，是近12个月分配给股东的股息占股价的百分比。',
			pcf_ocf_ttm double DEFAULT NULL COMMENT '每股股价为每股经营现金流(TTM)的倍数。可回测的估值指标。总市值2/经营现金净流量TTM ',
			pcf_ncf_ttm double DEFAULT NULL COMMENT '分子=最近交易日收盘价*最新普通股总股数 ;分母=现金及现金等价物净增加额(TTM)*最近交易日转换汇率(记帐本位币转换为交易币种);返回=分子／分母',
			pcf_ocflyr double DEFAULT NULL COMMENT ' 每股股价为每股经营现金流(LYR)的倍数。可回测的估值指标。总市值2／市现率(经营现金流LYR) 注：	1、总市值2=指定日证券收盘价*指定日当日总股本	2、B股涉及汇率转换',
			pcf_ncflyr double DEFAULT NULL COMMENT '每股股价为每股净现金流(LYR)的倍数。可回测的估值指标。总市值2／现金净流量(LYR) 注： 1、总市值2=指定日证券收盘价*指定日当日总股本	2、B股涉及汇率转换',
			trade_status varchar(100) DEFAULT NULL COMMENT '指定日该证券的市场交易状态，如正常交易、停牌。注：但日期参数为最新时，指最新的已收盘交易日。',
			oi double DEFAULT NULL COMMENT '持仓量',
			oi_chg double DEFAULT NULL COMMENT ' 持仓量变化',
			pre_settle double DEFAULT NULL COMMENT ' 前结算价',
			settle double DEFAULT NULL COMMENT '结算价',
			chg_settlement double DEFAULT NULL COMMENT '涨跌（结算价）',
			pct_chg_settlement double DEFAULT NULL COMMENT '涨跌幅（结算价）',
			lastradeday_s date DEFAULT NULL COMMENT ' 表示某证券有交易的最新交易日期。',
			last_trade_day date DEFAULT NULL COMMENT '表示某证券所在市场的最新一个交易日期。',
			rel_ipo_chg double DEFAULT NULL COMMENT '相对发行价涨跌=指定交易日收盘价-首发价格',
			rel_ipo_pct_chg double DEFAULT NULL COMMENT ' 相对发行价涨跌幅=[(指定交易日收盘价-首发价格)／首发价格]*100% 注：复权计算方法参见“日行情／收盘价”',
			susp_reason varchar(200) DEFAULT NULL COMMENT '证券于某交易日停牌的原因。',
			close3 double DEFAULT NULL COMMENT '指定交易日的收盘价，若无成交则返回为空。',
			data_source varchar(100) NOT NULL DEFAULT 'Wind' COMMENT '数据来源',
			created_date datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建日期',
			updated_date datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '更新日期'
			) ENGINE=InnoDB DEFAULT CHARSET=utf8"""
		
		try:
			# print(sql)
			self.cursor.execute(sql)
			self.db.commit()
		except Exception as e:
			print("  ============>  " + e)
			return
		
		sql = "SELECT last_modified FROM updatelog WHERE stock_code='" + symbol +"' limit 1"
		self.cursor.execute(sql)
		result = self.cursor.fetchone()
		print(result)
		if result == None:
			self.updateLogTable(symbol)


	def createTables(self, symbols):
		for symbol in symbols:
			self.createSingleTable(symbol)
	

	def createUpdateLogTable(self):
		sql = "CREATE TABLE IF NOT EXISTS " + self.logTable + """ (
			stock_code varchar(10) NOT NULL UNIQUE KEY COMMENT '股票代码',
			last_modified datetime NOT NULL DEFAULT '1990-01-01' COMMENT '最后同步日期'
			) ENGINE=InnoDB DEFAULT CHARSET=utf8"""

		self.cursor.execute(sql)
		self.db.commit()


	def updateLogTable(self, symbol, lastModified = "1990-01-01"):
		print("=========> " + symbol)
		sql = "INSERT INTO " + self.logTable + " VALUES('" + symbol + "', '" + lastModified \
			+ "') ON DUPLICATE KEY UPDATE stock_code='" + symbol \
			+ "', last_modified='" + lastModified + "'"

		print(sql)
		self.cursor.execute(sql)
		self.db.commit()


	def destroy(self):
		self.cursor.close()
		self.db.close()


	def getStockUpdateDate(self, symbol):
		sql = "SELECT last_modified FROM updatelog WHERE stock_code='" + symbol +"' limit 1"
		# print(sql)
		try:
			self.cursor.execute(sql)
			result = self.cursor.fetchone()
			return result
		except Exception as e:
			print("XXXXXXXXXXXXX	getStockUpdateDate issue for stock: ", symbol)
			print(e)
			return None


	def insertData(self, symbol, data):
		data[21] = list(map(str, data[21]))
		data[20] = list(map(str, data[20]))
		data = list(tuple(i) for i in zip(*data))

		index = len(data)
		for i in range(len(data)):
			if data[i][0] != None:
				index = i
				break

		sli = slice(index, 99999999999)
		data = data[sli]
		
		if len(data) == 0:
			print("no valid data")
			return

		data = list(map(str, data))

		sql = ','.join(data)
		sql = "INSERT INTO `" + symbol + """` (open,high,low,close,pre_close,volume,amt,dealnum,
		chg,pct_chg,vwap,close2,turn,free_turn,oi,oi_chg,pre_settle,settle,chg_settlement,
		pct_chg_settlement, lastradeday_s,last_trade_day,rel_ipo_chg,rel_ipo_pct_chg,susp_reason,close3, 
		pe_ttm,val_pe_deducted_ttm,pe_lyr,pb_lf,ps_ttm,ps_lyr,dividendyield2,ev,mkt_cap_ard,pb_mrq,
		pcf_ocf_ttm,pcf_ncf_ttm,pcf_ocflyr,trade_status) VALUES""" + sql
		sql = sql.replace('None', 'null')

		try:
			self.cursor.execute(sql)
			self.db.commit()
			self.updateLogTable(symbol, datetime.datetime.now().strftime("%Y-%m-%d"))
		except Exception as e:
			print("XXXXXXXXXXXXX	insertData issue for stock: ", symbol)

		print(sql)