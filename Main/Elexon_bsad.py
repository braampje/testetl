# compare for all common tables if static data is complete vs scraped data
# and dump new timeseries data
import numpy as np
import pandas as pd
import processors.SQL as SQL
import processors.Elexon as Elexon
from pytz import timezone
import sys
import os
import requests
from tempfile import NamedTemporaryFile

# scraped data


def main():
    dumper = pd.read_csv('csv/bsad_%s.csv' % sys.argv[1])
    conn, cur = SQL.connect()
    # transform all mutual columns
    dumper['source'] = 'ELEXON'
    dumper = SQL.common(conn, cur, dumper, 'source')
    tze = timezone('UTC')
    dumper['dump_date'] = pd.to_datetime('now')
    dumper['dump_date'] = dumper.dump_date.dt.tz_localize(tze)
    dumper['period'] = pd.Timedelta('30 minutes')
    dumper = SQL.Elexontime(dumper)

    dumper.loc[:, ['active_flag',
                  'so_flag', 'stor_flag']] = dumper.loc[:, ['active_flag', 'so_flag', 'stor_flag']].replace(to_replace=['T', 'F'], value=[True, False])
#    print(BOALF.dtypes)
    dumper = dumper.astype({'cost': 'float64', 'volume': 'float64', 'action_id': int,
                          'active_flag': bool, 'so_flag': bool, 'stor_flag': bool})


    SQL.dumpseries(conn, cur, dumper,
                   'Elexon_bsad', 'unit.bsad')

    os.remove('csv/Elexon_bsad_%s.csv' % sys.argv[1])
    # end = time.time()


main()

"""
resp = get_fuel('1')
test = resp.content

tmp_f = NamedTemporaryFile(delete=False)
with open(tmp_f.name, 'wb') as fhh:
    fhh.write(resp.content)

fueler = pd.read_excel(tmp_f.name)

print(fueler.head())
"""
# print(dumper.head(5))

# print(end - start)

# print(dumper)

# take data period requirements
# scrape data with scrapy
# melt scraper to format of table with pandas in conversions
# check for static data in db and dump if not complete
# reformat data by replacing static data with IDs
# dump data
