# -*- coding: utf-8 -*-
import scrapy
from scrapy.spiders import CSVFeedSpider
from Elexon.items import area_flows
import os
from datetime import date, timedelta
from dateutil.parser import parse


class actualGenperTypespider(CSVFeedSpider):

	name = "Flows"
	allowed_domains = 'bmreports.com'

	delimiter = ','
	headers = [
		'Record Tpe', 'Date', 'Period', 'FRANCE', 'NORTHERN_IRELAND', 'NETHERLANDS', 'IRELAND'
	]

	custom_settings = {
		'FEED_FORMAT': 'csv',
		'FEED_URI': '../Main/csv/Flows_%(STARTDATE)s.csv'
	}

	def start_requests(self):

		if os.path.exists('../Main/csv/Flows ' + self.STARTDATE + '.csv'):
			os.remove('../Main/csv/Flows ' + self.STARTDATE + '.csv')

		VERSION_NUMBER = 'v1'
		API_Key = '9urjhfmw814sqhn'
		TYPE = 'INTERFUELHH'
		SERVICETYPE = 'CSV'

		url = 'https://api.bmreports.com/BMRS/' + TYPE + '/' + VERSION_NUMBER + '?APIKey=' + API_Key
		url = url + '&ServiceType=' + SERVICETYPE + '&FromDate=' + self.STARTDATE
		url = url + '&ToDate=' + self.ENDDATE

		yield scrapy.Request(url, self.parse)

	def __init__(self, *args, **kwargs):

		STARTD = kwargs.pop('STARTDATE', (date.today() + timedelta(days=-3)).isoformat())
		self.STARTDATE = STARTD
		STARTD = parse(STARTD).date()
		self.ENDDATE = kwargs.pop('STARTDATE', (STARTD + timedelta(days=3)).isoformat())

		super(actualGenperTypespider, self).__init__(*args, **kwargs)

	def parse_row(self, response, row):
		item = area_flows()
		item['Date'] = row['Date']
		item['Period'] = row['Period']
		item['France'] = row['FRANCE']
		item['Northern_Ireland'] = row['NORTHERN_IRELAND']
		item['Netherlands'] = row['NETHERLANDS']
		item['Ireland'] = row['IRELAND']
		return item
