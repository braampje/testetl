# compare for all common tables if static data is complete vs scraped data
# and dump new timeseries data

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
    dumper = pd.read_csv('csv/Elexon_unit_dynamic_%s.csv' % sys.argv[1])

    # print(dumper.head())

    dumper = dumper.melt(
        id_vars=['company', 'dump_date', 'unit_dynamic_type',
                 'unit', 'bmunitid', 'unit_type'],
        var_name='unit_dynamic_subtype')

    dumper = dumper[pd.notnull(dumper['value'])]
    dumper = dumper.astype({'value': int})
    # print(dumper)

    # add static data columns
    dumper['source'] = 'ELEXON'

    # create/open database connection
    conn, cur = SQL.connect()

    # start = time.time()
    # format/convert columns to database ids etc and check if static data is complete
    dumper = SQL.common(conn, cur, dumper, 'source')
    dumper = SQL.common(conn, cur, dumper, 'unit_dynamic_type')
    dumper = SQL.common(conn, cur, dumper,
                        'unit_dynamic_subtype', common_table='unit_dynamic_type')

    dumper['dump_date'] = pd.to_datetime(dumper['dump_date'])
    dumper['dump_date'] = dumper.dump_date.dt.tz_localize(timezone('UTC'))

    # check if new units are available, if yes, gather necessary data
    dumper = Elexon.unit(conn, cur, dumper)

    # print(dumper)
    SQL.dumpseries(conn, cur, dumper, 'Elexon_unit_dynamic', 'unit.dynamic')

    os.remove('csv/Elexon_unit_dynamic_%s.csv' % sys.argv[1])
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
