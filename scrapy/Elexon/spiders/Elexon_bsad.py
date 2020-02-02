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
        item['Date'] = node.xpath('settDate/text()').extract()
        item['Period'] = node.xpath('settlementPeriod/text()').extract()
        item['cost'] = node.xpath('cost/text()').extract()
        item['volume'] = node.xpath('volume/text()').extract()
        item['action_id'] = node.xpath('id/text()').extract()
        item['active_flag'] = node.xpath('activeFlag/text()').extract()
        item['so_flag'] = node.xpath('soFlag/text()').extract()
        item['stor_flag'] = node.xpath('bsaaSTORProviderFlag/text()').extract()
        #print(True if not node.xpath('qpnLevelFrom/text()').extract() else False)
        return item
