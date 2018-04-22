# compare for all common tables if static data is complete vs scraped data 
# and dump new timeseries data

import pandas as pd
import processors.SQL as SQL
import time
from pytz import timezone

#scraped data
areagen = pd.read_csv('/home/vagrant/dev/powerdb/Scrapers/Main/csv/AreaGen.csv')

#print(areagen.dtypes)
#print(areagen.dtypes)
dumper = pd.melt(areagen, id_vars=['Date', 'Period'],var_name='fuel')
if dumper.Period.dtype == object:
	dumper = dumper[dumper['Period'] != 'Period']
	dumper['Period'] = pd.to_numeric(dumper['Period'])
	dumper['value'] = pd.to_numeric(dumper['value'])

print(dumper.dtypes)
#print(dumper)
conn, cur = SQL.connect()

start = time.time()
#format/convert columns to database ids etc and check if static data is complete
dumper = SQL.common(conn, cur, dumper,'fuel')

dumper['area'] = 'Great Britain'
dumper['source'] = 'ELEXON'

dumper = SQL.common(conn,cur,dumper,'area')
dumper = SQL.common(conn,cur,dumper,'source')
dumper = SQL.Elexontime(dumper)

tze = timezone('Europe/Amsterdam')
dumper['dump_date'] = pd.to_datetime('now')
dumper['dump_date'] = dumper.dump_date.dt.tz_localize(tze)
dumper['period'] = pd.Timedelta('30 minutes')

SQL.dumpareaseries(conn,cur,dumper,'actual_production')

end = time.time()

#print(dumper.head(5))

print(end-start)

#print(dumper)

# take data period requirements
# scrape data with scrapy
# melt scraper to format of table with pandas in conversions
# check for static data in db and dump if not complete
# reformat data by replacing static data with IDs
# dump data

#