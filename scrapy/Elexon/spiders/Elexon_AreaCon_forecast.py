# -*- coding: utf-8 -*-
import scrapy
from scrapy.spiders import CSVFeedSpider
from Elexon.items import area_con_for_type
import os
from datetime import date, timedelta
from dateutil.parser import parse


class Elexon_Con_Forecast_spider(CSVFeedSpider):

	name = "Elexon_AreaCon_Forecast"
	allowed_domains = 'bmreports.com'

	delimiter = ','
	headers = [
		'Record Type', 'Date', 'Period', 'Zone', 'Published', 'Demand'
	]

	custom_settings = {
		'FEED_FORMAT': 'csv',
		'FEED_URI': '../Main/csv/Elexon_AreaCon_forecast_%(STARTDATE)s.csv'
	}

	def start_requests(self):

		VERSION_NUMBER = 'v1'
		API_Key = '9urjhfmw814sqhn'
		TYPE = 'FORDAYDEM'
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

		if os.path.exists('../Main/csv/Elexon_AreaCon_forecast_' + self.STARTDATE + '.csv'):
			os.remove('../Main/csv/Elexon_AreaCon_forecast_' + self.STARTDATE + '.csv')

		super(Elexon_Con_Forecast_spider, self).__init__(*args, **kwargs)

	def parse_row(self, response, row):
		item = area_con_for_type()
		item['dump_date'] = row['Published']
		item['Date'] = row['Date']
		item['Period'] = row['Period']
		item['runtype'] = row['Record Type']
		item['value'] = row['Demand']
		return item
