# -*- coding: utf-8 -*-
import scrapy
from scrapy.spiders import CSVFeedSpider
from Elexon.items import area_con_type
import os
from datetime import date, timedelta
from dateutil.parser import parse


class actualConspider(CSVFeedSpider):

	name = "AreaCon"
	allowed_domains = 'bmreports.com'

	delimiter = ','
	headers = [
		'Record Type', 'Date', 'Period', 'Zone', 'Published', 'Demand'
	]

	custom_settings = {
		'FEED_FORMAT': 'csv',
		'FEED_URI': '../Main/csv/AreaCon_%(STARTDATE)s.csv'
	}

	def start_requests(self):

		VERSION_NUMBER = 'v1'
		API_Key = '9urjhfmw814sqhn'
		TYPE = 'INDOITSDO'
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

		if os.path.exists('../Main/csv/AreaCon_' + self.STARTDATE + '.csv'):
			os.remove('../Main/csv/AreaCon_' + self.STARTDATE + '.csv')

		super(actualConspider, self).__init__(*args, **kwargs)

	def parse_row(self, response, row):
		item = area_con_type()
		item['Date'] = row['Date']
		item['Period'] = row['Period']
		item['consumption_type'] = row['Record Type']
		item['value'] = row['Demand']
		return item
