# -*- coding: utf-8 -*-
import scrapy
from scrapy.spiders import XMLFeedSpider
from Elexon.items import bidoffer
import os
from datetime import date, timedelta
from dateutil.parser import parse


class bidoffer_spider(XMLFeedSpider):

    name = "Elexon_bidoffer"
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
        'FEED_URI': '../Main/csv/Elexon_bidoffer_%(STARTDATE)s.csv'
    }

    def start_requests(self):

        VERSION_NUMBER = 'v1'
        API_Key = '9urjhfmw814sqhn'
        TYPE = 'BOD'
        SERVICETYPE = 'XML'
        bMUnitType = ['*']

        url = 'https://api.bmreports.com/BMRS/' + TYPE + \
            '/' + VERSION_NUMBER + '?APIKey=' + API_Key
        url = url + '&ServiceType=' + SERVICETYPE + '&SettlementDate=' + \
            self.STARTDATE + '&NGCBMUnitName=*&BMUnitType='

        for ut in bMUnitType:
            for i in range(50):
                sp = i + 1
                yield scrapy.Request(url + ut + '&SettlementPeriod=' + str(sp), self.parse)

    def __init__(self, *args, **kwargs):

        STARTD = kwargs.pop('STARTDATE', date.today().isoformat())
        self.STARTDATE = STARTD

        if os.path.exists('../Main/csv/Elexon_bidoffer_' + self.STARTDATE + '.csv'):
            os.remove('../Main/csv/Elexon_bidoffer_' +
                      self.STARTDATE + '.csv')

        super(bidoffer_spider, self).__init__(*args, **kwargs)

    def parse_node(self, response, node):
        item = bidoffer()
        item['unit'] = node.xpath('ngcBMUnitName/text()').extract()
        item['bmunitid'] = node.xpath('bmUnitID/text()').extract()
        item['unit_type'] = node.xpath('bMUnitType/text()').extract()
        item['start_time'] = node.xpath('timeFrom/text()').extract()

        item['bidoffer_id'] = node.xpath('bidOfferPairNumber/text()').extract()
        item['volume_from'] = node.xpath('bidOfferLevelFrom/text()').extract()
        item['volume_to'] = node.xpath('bidOfferLevelTo/text()').extract()
        item['bid'] = node.xpath('bidPrice/text()').extract()
        item['offer'] = node.xpath('offerPrice/text()').extract()
        return item
