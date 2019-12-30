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
    dumper = pd.read_csv('csv/Elexon_unit_generation_%s.csv' % sys.argv[1])
    BOALF = dumper.loc[dumper.runtype == 'BOALF', :]
    print(BOALF.head())
    # Dump all normal generation data
    dumper = dumper.loc[dumper.runtype != 'BOALF', :]
    # print(dumper.head())
    dumper['value'] = pd.mean(dumper.value_from, dumper.value_to)
    dumper = dumper.astype({'value': int})
    dumper = dumper[dumper.value != 0]
    # print(dumper)

    # add static data columns
    dumper['source'] = 'ELEXON'
    tze = timezone('Europe/Amsterdam')
    dumper['dump_date'] = pd.to_datetime('now')
    dumper['dump_date'] = dumper.dump_date.dt.tz_localize(tze)
    dumper['period'] = (dumper.end_time - dumper.start_time) / \
        np.timedelta64(1, 'm')
    print(dumper.head())
    return
    # create/open database connection
    conn, cur = SQL.connect()
    exit
    # start = time.time()
    # format/convert columns to database ids etc and check if static data is complete
    dumper = SQL.common(conn, cur, dumper, 'source')
    dumper = SQL.common(conn, cur, dumper, 'runtype')

    dumper = Elexon.unit(conn, cur, dumper)

    # check if new units are available, if yes, gather necessary data

    # print(dumper)
    #SQL.dumpseries(conn, cur, dumper, 'Elexon_unit_generation','unit.generation')

    #dump bid offer acceptances in database

    os.remove('csv/Elexon_unit_generation_%s.csv' % sys.argv[1])
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
