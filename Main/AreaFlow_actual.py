# compare for all common tables if static data is complete vs scraped data
# and dump new timeseries data

import pandas as pd
import processors.SQL as SQL
import time
from pytz import timezone
import sys
import os

# scraped data

data = pd.read_csv('csv/Flows_%s.csv' %
                   sys.argv[1]).fillna(value=0, downcast='infer')
# print(data.head())
# print(data.dtypes)
# create dataset and clean
dumper = pd.melt(data, id_vars=['Date', 'Period'], var_name='area_from')
if dumper.Period.dtype == object:
    dumper = dumper[dumper['Period'] != 'Period']
    dumper['Period'] = pd.to_numeric(dumper['Period'])
    dumper['value'] = pd.to_numeric(dumper['value'])

# add static data columns
dumper['area_to'] = 'Great Britain'
dumper['source'] = 'ELEXON'

# create/open database connection
conn, cur = SQL.connect()

# start = time.time()
# format/convert columns to database ids etc and check if static data is complete
# fill columns  source_id, dump_date,start_time,period,border_id
dumper = SQL.common(conn, cur, dumper, 'source')

tze = timezone('Europe/Amsterdam')
dumper['dump_date'] = pd.to_datetime('now')
dumper['dump_date'] = dumper.dump_date.dt.tz_localize(tze)

# calculate  start_time
dumper = SQL.Elexontime(dumper)

dumper['period'] = pd.Timedelta('30 minutes')

dumper = SQL.common_border(conn, cur, dumper, 'border_area')
# print(dumper.head())
SQL.dumpseries(conn, cur, dumper, 'Elexon_border', 'border.flow_physical')

os.remove('csv/Flows_%s.csv' % sys.argv[1])
# end = time.time()

# print(dumper.head(5))

# print(end - start)

# print(dumper)

# take data period requirements
# scrape data with scrapy
# melt scraper to format of table with pandas in conversions
# check for static data in db and dump if not complete
# reformat data by replacing static data
