# create SQL connection and save main query functions

import psycopg2 as pg
import pandas.io.sql as psql
import pandas as pd
import io
from pytz import timezone
import sys


def connect():
    conn = pg.connect(
        "dbname=power user=bram password=scraper1 host=localhost port=5432")
    conn.autocommit = True
    cur = conn.cursor()
    return conn, cur


def readcommon(conn, cur, table):
    common = psql.read_sql("select * from common.%s" % table, conn)
    return common


def dumpcommon(conn, cur, cnew, columns, table):
    # need to retrieve common type specific extra columns.
    temp = io.StringIO()
    cnew.to_csv(temp, index=False, header=False)
    temp.seek(0)
    cur.copy_from(file=temp, columns=columns,
                  sep=',', table='common.%s' % table)
    conn.commit()
    print('new common %s added' % table)
    print(cnew)
    return None


def dumpseries(conn, cur, data, temptable, table):
    temp = io.StringIO()
    cur.execute("Select * from %s limit 0" % table)
    colnames = [desc[0] for desc in cur.description]

    data.to_csv(temp, columns=colnames, index=False, header=False)
    temp.seek(0)
    # print(temp.read_csv())
    cur.execute("""CREATE TEMP TABLE %s
				AS
				SELECT *
				FROM %s
				WITH NO DATA;""" % (temptable, table))

    cur.copy_from(file=temp, columns=colnames, sep=',', table=temptable)

    cur.execute(""" INSERT INTO %s
				select *
				from %s
				ON CONFLICT DO NOTHING;""" % (table, temptable))
    cur.execute(""" DROP TABLE %s;""" % temptable)
    # except pg.IntegrityError:
    # print('already in database')

    # conn.commit()
    print('series dumped')
    return None


def common(conn, cur, data, cname, ctable=None):
    # add new common types to common data scheme
    if ctable is None:
        ctable = cname

    ctype = readcommon(conn, cur, ctable)

    newc = pd.DataFrame(getattr(data, cname).unique(), columns=[ctable])
    newc = pd.merge(newc, ctype, on=ctable, how='left')
    newc = newc[pd.isnull(newc.id)][[ctable]]

    if not newc.empty:
        # if new fuel types found add to database and reread common data and merge
        dumpcommon(conn, cur, newc, list(ctable), ctable)
        ctype = readcommon(conn, cur, ctable)
    else:
        print('no new %s' % cname)

    ctype.rename(columns={ctable: cname, 'id': '%s_id' % cname}, inplace=True)
    data = pd.merge(data, ctype[[cname, '%s_id' %
                                 cname]], on=cname, how='left')

    return data


def common2(conn, cur, data, cname, ctable=None):
    # add new common types to common data scheme
    if ctable is None:
        ctable = cname

    ctype = readcommon(conn, cur, ctable)

    newc = pd.DataFrame(getattr(data, cname).unique(), columns=[ctable])
    newc = pd.merge(newc, ctype, on=ctable, how='left')
    newc = newc[pd.isnull(newc.id)][[ctable]]

    if not newc.empty:
        # if new fuel types found add to database and reread common data and merge
        colnames = [col for col in ctype.columns]
        #newc = newc.reindex(columns=[*newc.columns.tolist(), *colnames])
        print(colnames)
        newc = pd.merge(newc, data[*colnames], on ctable, how='left')
        dumpcommon(conn, cur, newc, list(ctable), ctable)
        ctype = readcommon(conn, cur, ctable)
    else:
        print('no new %s' % cname)

    ctype.rename(columns={ctable: cname, 'id': '%s_id' % cname}, inplace=True)
    data = pd.merge(data, ctype[[cname, '%s_id' %
                                 cname]], on=cname, how='left')

    return data


def common_border(conn, cur, data, table_name):
    # add new border ids to common_border and check if area exist in areas

    # check for new areas and dump
    newareas = pd.DataFrame(pd.unique(
        data[['area_from', 'area_to']].values.ravel('K')),
        columns=['area'])
    common(conn, cur, newareas, 'area')

# check for new borders and dump
    borders = readcommon(conn, cur, 'v_borders')

    newborders = data[['area_from', 'area_to']].drop_duplicates()
    newborders = pd.merge(
        newborders, borders, how='left',
        left_on=['area_from', 'area_to'],
        right_on=['border_from', 'border_to'])
    newborders = newborders[pd.isnull(newborders.border_from)][[
        'area_from', 'area_to']]

    if not newborders.empty:
        # dump new borders
        areas = readcommon(conn, cur, 'area')
        newborders = pd.merge(newborders, areas[['id', 'area']], how='left',
                              left_on=['area_from'],
                              right_on=['area'])
        newborders.rename(columns={'id': 'border_source_id'}, inplace=True)
        newborders = pd.merge(newborders, areas[['id', 'area']], how='left',
                              left_on=['area_to'],
                              right_on=['area'])
# 		print(newborders.head())
        newborders.rename(columns={'id': 'border_target_id'}, inplace=True)
        newborders['area_function_id'] = 22
        print(newborders)
        newborders = newborders[['area_function_id',
                                 'border_source_id', 'border_target_id']]
        print(newborders.columns)
        dumpcommon(conn, cur, newborders, newborders.columns, table_name)
        borders = readcommon(conn, cur, 'v_borders')
    else:
        print('no new borders')

    data = pd.merge(data, borders[['id', 'border_from', 'border_to']], how='left',
                    left_on=['area_from', 'area_to'],
                    right_on=['border_from', 'border_to'])
    data.rename(columns={'id': 'border_id'}, inplace=True)

    return data


def Elexontime(data):
    # transform Elexon period to start_time(UKtimezone)
    tz = timezone('Europe/London')

    data['Date'] = pd.to_datetime(data['Date'].astype(str), format='%Y%m%d')

    data['start_time'] = data.Date.dt.tz_localize(tz)
    data['start_time'] = data.start_time.dt.tz_convert(timezone('UTC'))
    data['start_time'] = data.start_time + \
        pd.to_timedelta((data['Period'] - 1) * 30, 'm')
    data['start_time'] = data.start_time.dt.tz_convert(tz)

    return data
