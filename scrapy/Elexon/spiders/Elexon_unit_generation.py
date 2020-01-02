# -*- coding: utf-8 -*-
import scrapy
from scrapy.spiders import XMLFeedSpider
from Elexon.items import unit_generation
import os
from datetime import date, timedelta
from dateutil.parser import parse


class unit_generationspider(XMLFeedSpider):

    name = "Elexon_unit_generation"
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
        'FEED_URI': '../Main/csv/Elexon_unit_generation_%(STARTDATE)s.csv'
    }

    def start_requests(self):

        VERSION_NUMBER = 'v1'
        API_Key = '9urjhfmw814sqhn'
        TYPE = 'PHYBMDATA'
        SERVICETYPE = 'XML'
        bMUnitType = ['E', 'S', 'T']

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

        if os.path.exists('../Main/csv/Elexon_unit_generation_' + self.STARTDATE + '.csv'):
            os.remove('../Main/csv/Elexon_unit_generation_' +
                      self.STARTDATE + '.csv')

        super(unit_generationspider, self).__init__(*args, **kwargs)

    def parse_node(self, response, node):
        item = unit_generation()
        item['runtype'] = node.xpath('recordType/text()').extract()
        item['unit'] = node.xpath('ngcBMUnitName/text()').extract()
        item['bmunitid'] = node.xpath('bmUnitID/text()').extract()
        item['company'] = node.xpath('leadPartyName/text()').extract()
        item['unit_type'] = node.xpath('bMUnitType/text()').extract()
        item['start_time'] = node.xpath('timeFrom/text()').extract()
        item['end_time'] = node.xpath('timeTo/text()').extract()
        #print(True if not node.xpath('qpnLevelFrom/text()').extract() else False)
        if node.xpath('pnLevelFrom/text()').extract():
            item['value_from'] = node.xpath('pnLevelFrom/text()').extract()
            item['value_to'] = node.xpath('pnLevelTo/text()').extract()
        elif node.xpath('qpnLevelFrom/text()').extract():
            item['value_from'] = node.xpath('qpnLevelFrom/text()').extract()
            item['value_to'] = node.xpath('qpnLevelTo/text()').extract()
        elif node.xpath('melLevelFrom/text()').extract():
            item['value_from'] = node.xpath('melLevelFrom/text()').extract()
            item['value_to'] = node.xpath('melLevelTo/text()').extract()
        elif node.xpath('milLevelFrom/text()').extract():
            item['value_from'] = node.xpath('milLevelFrom/text()').extract()
            item['value_to'] = node.xpath('milLevelTo/text()').extract()
        elif node.xpath('bidOfferLevelFrom/text()').extract():
            item['value_from'] = node.xpath(
                'bidOfferLevelFrom/text()').extract()
            item['value_to'] = node.xpath('bidOfferLevelTo/text()').extract()
            item['acceptance_id'] = node.xpath(
                'bidOfferAcceptanceNumber/text()').extract()
            item['acceptance_time'] = node.xpath(
                'acceptanceTime/text()').extract()
            item['bo_flag'] = node.xpath('deemedBidOfferFlag/text()').extract()
            item['so_flag'] = node.xpath('soFlag/text()').extract()
            item['stor_flag'] = node.xpath('storProviderFlag/text()').extract()
            item['rr_instruction_flag'] = node.xpath(
                'rrInstructionFlag/text()').extract()
            item['rr_schedule_flag'] = node.xpath(
                'rrScheduleFlag/text()').extract()
        return item
