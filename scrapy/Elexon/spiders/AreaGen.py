# -*- coding: utf-8 -*-
import scrapy
from scrapy.spiders import CSVFeedSpider
from Elexon.items import area_gen_fuel_type
import os
from datetime import date, timedelta

class actualGenperTypespider(CSVFeedSpider):
	name = "AreaGen"

	

	allowed_domains = 'bmreports.com'
	custom_settings = {
		'FEED_FORMAT': 'csv',
		'FEED_URI': '../../../Main/csv/AreaGen '
    }
	
	delimiter = ','
	headers = ['Record Type','Date','Period','CCGT','OIL','COAL','NUCLEAR','WIND','PS','NPSHYD','OCGT','OTHER','INTFR','INTIRL','INTNED','INTEW','BIOMASS']

	def start_requests(self):

		STARTDATE = getattr(self, 'STARTDATE', (date.today() + timedelta(days=-3)).isoformat())
		ENDDATE = getattr(self, 'ENDDATE', (STARTDATE + timedelta(days=3).isoformat()))
		self.custom_settings['FEED_URI'] = self.custom_settings['FEED_URI']  + STARTDATE + '.csv'

		if os.path.exists('../../../Main/csv/AreaGen ' + STARTDATE + '.csv'):
			os.remove('../../../Main/csv/AreaGen ' + STARTDATE + '.csv')

		PORT = 443
		VERSION_NUMBER = 'v1'
		API_Key = '9urjhfmw814sqhn'
		TYPE = 'FUELHH'
		SERVICETYPE = 'CSV'

		url = 'https://api.bmreports.com/BMRS/' + TYPE + '/' + VERSION_NUMBER + '?APIKey=' + API_Key
		url = url  + '&ServiceType=' + SERVICETYPE + '&FromDate=' + STARTDATE + '&ToDate=' + ENDDATE

		yield scrapy.Request(url, self.parse)

	def parse_row(self, response,row):
		item = area_gen_fuel_type()
		item['Date'] = row['Date']
		item['Period'] = row ['Period']
		item['CCGT'] = row ['CCGT']
		item['OIL'] = row ['OIL']
		item['COAL'] = row ['COAL']
		item['NUCLEAR'] = row ['NUCLEAR']
		item['WIND'] = row ['WIND']
		item['PS'] = row ['PS']
		item['NPSHYD'] = row ['NPSHYD']
		item['OCGT'] = row ['OCGT']
		item['OTHER'] = row ['OTHER']
		item['BIOMASS'] = row ['BIOMASS']
		return item
