# -*- coding: utf-8 -*-
import scrapy
from scrapy.spiders import CSVFeedSpider
from Elexon.items import area_gen_fuel_type
import os
from datetime import date, timedelta
from dateutil.parser import parse


class actualGenperTypespider(CSVFeedSpider):

	name = "AreaGen"
	allowed_domains = 'bmreports.com'

	delimiter = ','
	headers = [
		'Record Type', 'Date', 'Period', 'CCGT', 'OIL', 'COAL', 'NUCLEAR', 'WIND', 'PS',
		'NPSHYD', 'OCGT', 'OTHER', 'INTFR', 'INTIRL', 'INTNED', 'INTEW', 'BIOMASS', 'INTNEM'
	]

	custom_settings = {
		'FEED_FORMAT': 'csv',
		'FEED_URI': '../Main/csv/AreaGen_%(STARTD)s.csv'
	}

	def start_requests(self):

		VERSION_NUMBER = 'v1'
		API_Key = '9urjhfmw814sqhn'
		TYPE = 'FUELHH'
		SERVICETYPE = 'CSV'

		url = 'https://api.bmreports.com/BMRS/' + TYPE + '/' + VERSION_NUMBER + '?APIKey=' + API_Key
		url = url + '&ServiceType=' + SERVICETYPE + '&FromDate=' + self.STARTDATE
		url = url + '&ToDate=' + self.ENDDATE

		yield scrapy.Request(url, self.parse)

	def __init__(self, *args, **kwargs):

		STARTD = kwargs.pop('STARTDATE', (date.today() + timedelta(days=-2)).isoformat())
		self.STARTD = STARTD
		STARTD = parse(STARTD).date()
		self.STARTDATE = (STARTD + timedelta(days=-1)).isoformat()
		self.ENDDATE = kwargs.pop('ENDDATE', (STARTD + timedelta(days=3)).isoformat())

		if os.path.exists('../Main/csv/AreaGen_' + self.STARTDATE + '.csv'):
			os.remove('../Main/csv/AreaGen_' + self.STARTDATE + '.csv')

		super(actualGenperTypespider, self).__init__(*args, **kwargs)

	def parse_row(self, response, row):
		item = area_gen_fuel_type()
		item['Date'] = row['Date']
		item['Period'] = row['Period']
		item['CCGT'] = row['CCGT']
		item['OIL'] = row['OIL']
		item['COAL'] = row['COAL']
		item['NUCLEAR'] = row['NUCLEAR']
		item['WIND'] = row['WIND']
		item['PS'] = row['PS']
		item['NPSHYD'] = row['NPSHYD']
		item['OCGT'] = row['OCGT']
		item['OTHER'] = row['OTHER']
		item['BIOMASS'] = row['BIOMASS']
		return item
