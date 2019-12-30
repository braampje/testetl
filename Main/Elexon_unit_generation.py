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
    dumper = pd.read_csv('csv/Elexon_unit_generation_%s.csv' % sys.argv[1])
    conn, cur = SQL.connect()
    # transform all mutual columns
    dumper['source'] = 'ELEXON'
    dumper = SQL.common(conn, cur, dumper, 'source')
    dumper = SQL.common(conn, cur, dumper, 'runtype')
    tze = timezone('Europe/Amsterdam')
    dumper['dump_date'] = pd.to_datetime('now')
    dumper['dump_date'] = dumper.dump_date.dt.tz_localize(tze)
    dumper.loc[:,'end_time'] = pd.to_datetime(dumper.end_time)
    dumper.loc[:, 'start_time'] = pd.to_datetime(dumper.start_time)
    dumper['period'] = pd.to_timedelta((dumper.end_time - dumper.start_time) / \
        np.timedelta64(1, 'm'), unit='m')


    dumper = Elexon.unit(conn, cur, dumper)

    # split to two table insert dataframes
    BOALF = dumper.loc[dumper.runtype == 'BOALF', :]
    print(BOALF.head(),BOALF.dtypes)
    # Dump all normal generation data
    dumper = dumper.loc[dumper.runtype != 'BOALF', :]
    # print(dumper.head())

    # get all data to database except BOALF
    dumper['value'] = dumper[['value_from','value_to']].mean(axis=1)
    dumper = dumper.astype({'value': int})
    dumper = dumper[dumper.value != 0]
    # print(dumper)

    # add static data columns


    # create/open database connection

    exit
    # start = time.time()
    # format/convert columns to database ids etc and check if static data is complete



    # check if new units are available, if yes, gather necessary data

    # print(dumper)
    SQL.dumpseries(conn, cur, dumper, 'Elexon_unit_generation','unit.generation')

    #dump bid offer acceptances in database
    BOALF = BOALF.astype({'value_from': int, 'value_to': int,'acceptance_id': int,'bo_flag': bool,'rr_instruction_flag': bool,'rr_schedule_flag':bool,'so_flag':bool,'stor_flag':bool})
    dumper.loc[:,'acceptance_time'] = pd.to_datetime(dumper.acceptance_time)
    BOALF.rename(columns={'acceptance_time': 'trade_date','acceptance_id':'trade_id','bo_flag':'bo','rr_instruction_flag':'rr_instruction','rr_schedule_flag':'rr_schedule'}, inplace=True)

    print(BOALF.dtypes)
    SQL.dumpseries(conn, cur, dumper, 'Elexon_BOALF','unit.BOALF')

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
