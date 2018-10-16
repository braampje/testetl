# -*- coding: utf-8 -*-
import scrapy
from scrapy.spiders import XMLFeedSpider
from Elexon.items import unit_dynamic
import os
from datetime import date, timedelta
from dateutil.parser import parse


class unit_dynamicspider(XMLFeedSpider):

	name = "Elexon_unit_dynamic"
	allowed_domains = 'bmreports.com'
	itertag = 'item'
	iterator = 'xml'

#	delimiter = ','

#	headers = [
#		'Record Type', 'BMunitid', 'Date', 'value1', 'value2', 'value3', 'value4', 'value5'
#	]

	custom_settings = {
		'FEED_FORMAT': 'csv',
		'FEED_URI': '../Main/csv/Elexon_unit_dynamic_%(STARTDATE)s.csv'
	}

	def start_requests(self):

		VERSION_NUMBER = 'v1'
		API_Key = '9urjhfmw814sqhn'
		TYPE = 'DYNBMDATA'
		SERVICETYPE = 'XML'

		url = 'https://api.bmreports.com/BMRS/' + TYPE + '/' + VERSION_NUMBER + '?APIKey=' + API_Key
		url = url + '&ServiceType=' + SERVICETYPE + '&SettlementDate=' + self.STARTDATE
		# + '&NGCBMUnitName=CRUA-1'

		yield scrapy.Request(url, self.parse)

	def __init__(self, *args, **kwargs):

		STARTD = kwargs.pop('STARTDATE', date.today().isoformat())
		self.STARTDATE = STARTD

		if os.path.exists('../Main/csv/Elexon_unit_dynamic_' + self.STARTDATE + '.csv'):
			os.remove('../Main/csv/Elexon_unit_dynamic_' + self.STARTDATE + '.csv')

		super(unit_dynamicspider, self).__init__(*args, **kwargs)

	def parse_node(self, response, node):
		item = unit_dynamic()
		item['unit_dynamic_type'] = node.xpath('recordType/text()').extract()
		item['unit'] = node.xpath('ngcBMUnitName/text()').extract()
		item['bmUnitID'] = node.xpath('bmUnitID/text()').extract()
		item['company'] = node.xpath('leadPartyName/text()').extract()
		item['unit_type'] = node.xpath('bMUnitType/text()').extract()
		item['dump_date'] = node.xpath('effectiveTime/text()').extract()
		item['RDR1'] = node.xpath('runDownRate1/text()').extract()
		item['RDelbow2'] = node.xpath('runDownElbow2/text()').extract()
		item['RDR2'] = node.xpath('runDownRate2/text()').extract()
		item['RDelbow3'] = node.xpath('runDownElbow3/text()').extract()
		item['RDR3'] = node.xpath('runDownRate3/text()').extract()
		item['RUR1'] = node.xpath('runUpRate1/text()').extract()
		item['RUelbow2'] = node.xpath('runUpElbow2/text()').extract()
		item['RUR2'] = node.xpath('runUpRate2/text()').extract()
		item['RUelbow3'] = node.xpath('runUpElbow3/text()').extract()
		item['RUR3'] = node.xpath('runUpRate3/text()').extract()
		item['NDZ'] = node.xpath('noticeToDeviateFromZero/text()').extract()
		item['NDB'] = node.xpath('noticeToDeliverBids/text()').extract()
		item['NDO'] = node.xpath('noticeToDeliverOffers/text()').extract()
		item['MZT'] = node.xpath('minimumZeroTime/text()').extract()
		item['MNZT'] = node.xpath('minimumNonZeroTime/text()').extract()
		item['SEL'] = node.xpath('stableExportLimit/text()').extract()
		item['SIL'] = node.xpath('stableImportLimit/text()').extract()
		item['MDV'] = node.xpath('maximumDeliveryVolume/text()').extract()
		item['MDP'] = node.xpath('maximumDeliveryPeriod/text()').extract()
		return item
