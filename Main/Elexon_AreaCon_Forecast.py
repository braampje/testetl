# compare for all common tables if static data is complete vs scraped data
# and dump new timeseries data

import pandas as pd
import processors.SQL as SQL
import time
from pytz import timezone
import sys
import os
# testing
# scraped data

dumper = pd.read_csv('csv/Elexon_AreaCon_forecast_%s.csv' % sys.argv[1])

# create dataset and clean
if dumper.Period.dtype == object:
    dumper = dumper[dumper['Period'] != 'Period']
    dumper['Period'] = pd.to_numeric(dumper['Period'])
    dumper['value'] = pd.to_numeric(dumper['value'])

# add static data columns
dumper['area'] = 'Great Britain'
dumper['source'] = 'ELEXON'

# replace consumption ID names
run_types = {'DANF': 'NDF',
             'DATF': 'TSDF',
             'DAID': 'INDDEM',
             'DAIG': 'INDGEN'}

dumper['runtype'].replace(run_types, inplace=True)

con_types = {'NDF': 'INDO',
             'TSDF': 'ITSDO',
             'INDDEM': 'DEMschedule',
             'INDGEN': 'GENschedule'}

dumper['consumption_type'] = dumper['runtype']
dumper['consumption_type'].replace(con_types, inplace=True)
# create/open database connection
conn, cur = SQL.connect()

# start = time.time()
# format/convert columns to database ids etc and check if static data is complete
dumper = SQL.common(conn, cur, dumper, 'runtype')
dumper = SQL.common(conn, cur, dumper, 'area')
dumper = SQL.common(conn, cur, dumper, 'source')
dumper = SQL.common(conn, cur, dumper, 'consumption_type')

dumper = SQL.Elexontime(dumper)

dumper['dump_date'] = pd.to_datetime(
    dumper['dump_date'].astype(str), format='%Y%m%d%H%M%S')
dumper['dump_date'] = dumper.dump_date.dt.tz_localize(timezone('UTC'))
dumper['period'] = pd.Timedelta('30 minutes')

# print(dumper.head())

SQL.dumpseries(conn, cur, dumper, 'Elexon_AreaCon_forecast',
               'area.forecast_consumption')

os.remove('csv/Elexon_AreaCon_forecast_%s.csv' % sys.argv[1])
# end = time.time()

# print(dumper.head(5))

# print(end - start)

# print(dumper)

# take data period requirements
# scrape data with scrapy
# melt scraper to format of table with pandas in conversions
# check for static data in db and dump if not complete
# reformat data by replacing static data with IDs
# dump data
