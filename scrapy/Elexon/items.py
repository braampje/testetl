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
    unit_dynamic_type = scrapy.Field()
    unit = scrapy.Field()
    bmunitid = scrapy.Field()
    dump_date = scrapy.Field()
    company = scrapy.Field()
    unit_type = scrapy.Field()
    RDR1 = scrapy.Field()
    RDelbow2 = scrapy.Field()
    RDR2 = scrapy.Field()
    RDelbow3 = scrapy.Field()
    RDR3 = scrapy.Field()
    RUR1 = scrapy.Field()
    RUelbow2 = scrapy.Field()
    RUR2 = scrapy.Field()
    RUelbow3 = scrapy.Field()
    RUR3 = scrapy.Field()
    NDZ = scrapy.Field()
    NDB = scrapy.Field()
    NDO = scrapy.Field()
    MZT = scrapy.Field()
    MNZT = scrapy.Field()
    SEL = scrapy.Field()
    SIL = scrapy.Field()
    MDV = scrapy.Field()
    MDP = scrapy.Field()
    pass
