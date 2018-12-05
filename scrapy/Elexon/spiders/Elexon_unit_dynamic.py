# -*- coding: utf-8 -*-
import scrapy
from scrapy.spiders import CSVFeedSpider
from Elexon.items import unit_dynamic
import os
from datetime import date, timedelta
from dateutil.parser import parse


class unit_dynamicspider(CSVFeedSpider):

	name = "Elexon_unit_dynamic"
	allowed_domains = 'bmreports.com'

	delimiter = ','
	headers = [
		'Record Type', 'BMunitid', 'Date', 'value1', 'value2', 'value3', 'value4', 'value5'
	]

	custom_settings = {
		'FEED_FORMAT': 'csv',
		'FEED_URI': '../Main/csv/Elexon_unit_dynamic_%(STARTDATE)s.csv'
	}

	def start_requests(self):

		VERSION_NUMBER = 'v1'
		API_Key = '9urjhfmw814sqhn'
		TYPE = 'DYNBMDATA'
		SERVICETYPE = 'CSV'

		url = 'https://api.bmreports.com/BMRS/' + TYPE + '/' + VERSION_NUMBER + '?APIKey=' + API_Key
		url = url + '&ServiceType=' + SERVICETYPE + '&SettlementDate=' + self.STARTDATE

		yield scrapy.Request(url, self.parse)

	def __init__(self, *args, **kwargs):

		STARTD = kwargs.pop('STARTDATE', date.today().isoformat())
		self.STARTDATE = STARTD

		if os.path.exists('../Main/csv/Elexon_unit_dynamic_' + self.STARTDATE + '.csv'):
			os.remove('../Main/csv/Elexon_unit_dynamic_' + self.STARTDATE + '.csv')

		super(unit_dynamicspider, self).__init__(*args, **kwargs)

	def parse_row(self, response, row):
		item = unit_dynamic()
		item['Record'] = row['Record Type']
		item['Unit'] = row['BMunitid']
		item['Date'] = row['Date']
		item['Value1'] = row['value1']
		item['Value2'] = row['value2']
		item['Value3'] = row['value3']
		item['Value4'] = row['value4']
		item['Value5'] = row['value5']
		return item
