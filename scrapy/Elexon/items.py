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


class unit_generation(scrapy.Item):
    runtype = scrapy.Field()
    unit = scrapy.Field()
    bmunitid = scrapy.Field()
    company = scrapy.Field()
    unit_type = scrapy.Field()
    start_time = scrapy.Field()
    end_time = scrapy.Field()
    value_from = scrapy.Field()
    value_to = scrapy.Field()
    acceptance_id = scrapy.Field()
    acceptance_time = scrapy.Field()
    bo_flag = scrapy.Field()
    so_flag = scrapy.Field()
    stor_flag = scrapy.Field()
    rr_instruction_flag = scrapy.Field()
    rr_schedule_flag = scrapy.Field()
    pass


class bsad(scrapy.Item):
    Date = scrapy.Field()
    Period = scrapy.Field()
    cost = scrapy.Field()
    volume = scrapy.Field()
    action_id = scrapy.Field()
    active_flag = scrapy.Field()
    so_flag = scrapy.Field()
    stor_flag = scrapy.Field()
    pass


class derived(scrapy.Item):
    Date = scrapy.Field()
    Period = scrapy.Field()
    cost = scrapy.Field()
    volume = scrapy.Field()
    action_id = scrapy.Field()
    active_flag = scrapy.Field()
    so_flag = scrapy.Field()
    stor_flag = scrapy.Field()
    pass


class bidoffer(scrapy.Item):
    unit = scrapy.Field()
    bmunitid = scrapy.Field()
    unit_type = scrapy.Field()
    start_time = scrapy.Field()
    bidoffer_id = scrapy.Field()
    volume_from = scrapy.Field()
    volume_to = scrapy.Field()
    bid = scrapy.Field()
    offer = scrapy.Field()
    pass
