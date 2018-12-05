# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class area_gen_fuel_type(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()

    Date = scrapy.Field()
    Period = scrapy.Field()
    CCGT = scrapy.Field()
    OIL = scrapy.Field()
    COAL = scrapy.Field()
    NUCLEAR = scrapy.Field()
    WIND = scrapy.Field()
    PS = scrapy.Field()
    NPSHYD = scrapy.Field()
    OCGT = scrapy.Field()
    OTHER = scrapy.Field()
    BIOMASS = scrapy.Field()
    pass


class area_flows(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()

    Date = scrapy.Field()
    Period = scrapy.Field()
    France = scrapy.Field()
    Northern_Ireland = scrapy.Field()
    Netherlands = scrapy.Field()
    Ireland = scrapy.Field()
    Belgium = scrapy.Field()
    pass


class area_con_type(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()

    Date = scrapy.Field()
    Period = scrapy.Field()
    consumption_type = scrapy.Field()
    value = scrapy.Field()
    pass


class area_con_for_type(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()

    dump_date = scrapy.Field()
    Date = scrapy.Field()
    Period = scrapy.Field()
    runtype = scrapy.Field()
    value = scrapy.Field()
    pass


class unit_dynamic(scrapy.Item):
    Record = scrapy.Field()
    Unit = scrapy.Field()
    Date = scrapy.Field()
    Value1 = scrapy.Field()
    Value2 = scrapy.Field()
    Value3 = scrapy.Field()
    Value4 = scrapy.Field()
    Value5 = scrapy.Field()
    pass
