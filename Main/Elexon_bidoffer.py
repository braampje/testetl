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
    dumper = pd.read_csv('csv/Elexon_bidoffer_%s.csv' % sys.argv[1])
    conn, cur = SQL.connect()
    # transform all mutual columns
    dumper['source'] = 'ELEXON'
    dumper = SQL.common(conn, cur, dumper, 'source')
    tze = timezone('UTC')
    dumper['dump_date'] = pd.to_datetime('now')
    dumper['dump_date'] = dumper.dump_date.dt.tz_localize(tze)
    dumper.loc[:, 'start_time'] = pd.to_datetime(dumper.start_time)
    dumper['period'] = pd.Timedelta('30 minutes')

    dumper = Elexon.unit(conn, cur, dumper)

    # print(dumper.head())

    # get all generation data to database except BOALF
    dumper = dumper.astype({'volume_from': int, 'volume_to': int,
                            'bid': 'float64', 'offer': 'float64', 'bidoffer_id': int})
    dumper = dumper[(dumper.volume_from != 0) & (((dumper.bidoffer_id > 0) & (
        dumper.offer < 3000)) | ((dumper.bidoffer_id < 0) & (dumper.bid > -3000)))]
    # print(dumper)

    # add static data columns

    # create/open database connection

    # exit
    # start = time.time()
    # format/convert columns to database ids etc and check if static data is complete

    # check if new units are available, if yes, gather necessary data

    # print(dumper)
    SQL.dumpseries(conn, cur, dumper,
                   'Elexon_bidoffer', 'unit.bidoffer')

    # dump bid offer acceptances in database

    os.remove('csv/Elexon_bidoffer_%s.csv' % sys.argv[1])
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
