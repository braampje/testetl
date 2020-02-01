# -*- coding: utf-8 -*-
import scrapy
from scrapy.spiders import XMLFeedSpider
from Elexon.items import bsad
import os
from datetime import date, timedelta
from dateutil.parser import parse


class bsad_spider(XMLFeedSpider):

    name = "Elexon_bsad"
    allowed_domains = 'bmreports.com'
    itertag = 'item'
    iterator = 'xml'
    download_timeout = 600

#   delimiter = ','

#   headers = [
#       'Record Type', 'BMunitid', 'Date', 'value1', 'value2', 'value3', 'value4', 'value5'
#   ]

    custom_settings = {
        'FEED_FORMAT': 'csv',
        'FEED_URI': '../Main/csv/Elexon_bsad_%(STARTDATE)s.csv'
    }

    def start_requests(self):

        VERSION_NUMBER = 'v1'
        API_Key = '9urjhfmw814sqhn'
        TYPE = 'DISBSAD'
        SERVICETYPE = 'XML'

        url = 'https://api.bmreports.com/BMRS/' + TYPE + \
            '/' + VERSION_NUMBER + '?APIKey=' + API_Key
        url = url + '&ServiceType=' + SERVICETYPE + '&SettlementDate=' + \
            self.STARTDATE

        yield scrapy.Request(url, self.parse)

    def __init__(self, *args, **kwargs):

        STARTD = kwargs.pop('STARTDATE', date.today().isoformat())
        self.STARTDATE = STARTD

        if os.path.exists('../Main/csv/Elexon_bsad_' + self.STARTDATE + '.csv'):
            os.remove('../Main/csv/Elexon_bsad_' +
                      self.STARTDATE + '.csv')

        super(bsad_spider, self).__init__(*args, **kwargs)

    def parse_node(self, response, node):
        item = bsad()
        item['runtype'] = node.xpath('recordType/text()').extract()
        item['unit'] = node.xpath('ngcBMUnitName/text()').extract()
        item['bmunitid'] = node.xpath('bmUnitID/text()').extract()
        item['company'] = node.xpath('leadPartyName/text()').extract()
        item['unit_type'] = node.xpath('bMUnitType/text()').extract()
        item['start_time'] = node.xpath('timeFrom/text()').extract()
        item['end_time'] = node.xpath('timeTo/text()').extract()
        #print(True if not node.xpath('qpnLevelFrom/text()').extract() else False)
        return item
