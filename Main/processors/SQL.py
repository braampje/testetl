# create SQL connection and save main query functions

import psycopg2 as pg
import pandas.io.sql as psql
import pandas as pd
import io
from pytz import timezone
import sys

def connect():
	conn = pg.connect("dbname=power user=bram password=scraper1 host=localhost port=5432")
	conn.autocommit = True
	cur = conn.cursor()
	return conn, cur


def readcommon(conn, cur, table):
	common = psql.read_sql("select * from common.%s" % table, conn)
	return common


def dumpcommon(conn, cur, cnew, table):
	# need to retrieve common type specific extra columns.
	temp = io.StringIO()
	cnew.to_csv(temp, index=False, header=False)
	temp.seek(0)
	cur.copy_from(file=temp, columns=[table], table='common.%s' % table)
	conn.commit()
	print('new common %s added' % table)
	print(cnew)
	return None


def dumpareaseries(conn, cur, data, table):
	temp = io.StringIO()
	areacol = ['dump_date', 'start_time', 'period', 'area_id', 'fuel_id', 'source_id', 'value']
	data.to_csv(temp, columns=areacol, index=False, header=False)
	temp.seek(0)
	# print(temp.read_csv())
	# test2 = data[areacol]
	# print(test2.dtypes)
	cur.execute("""CREATE TEMP TABLE dumper
				AS
				SELECT *
				FROM area.actual_production
				WITH NO DATA;""")

	cur.copy_from(file=temp, columns=areacol, sep=',', table='dumper')

	cur.execute(""" INSERT INTO area.actual_production
				select *
				from dumper
				ON CONFLICT DO NOTHING""")
	cur.execute(""" DROP TABLE dumper;""")
	# except pg.IntegrityError:
	# print('already in database')

	# conn.commit()
	print('series dumped')
	return None


def common(conn, cur, data, cname):
	# add new common types to common data scheme
	ctype = readcommon(conn, cur, cname)

	newc = pd.DataFrame(getattr(data, cname).unique(), columns=[cname])
	newc = pd.merge(newc, ctype, on=cname, how='left')
	newc = newc[pd.isnull(newc.id)][[cname]]

	if not newc.empty:
		# if new fuel types found add to database and reread common data and merge
		dumpcommon(conn, cur, newc, cname)
		ctype = readcommon(conn, cur, cname)
	else:
		print('no new %s' % cname)

	data = pd.merge(data, ctype[[cname, 'id']], on=cname, how='left')
	data.rename(columns={'id': '%s_id' % cname}, inplace=True)

	return data


def Elexontime(data):
	# transform Elexon period to start_time(UKtimezone)
	tz = timezone('Europe/London')

	data['Date'] = pd.to_datetime(data['Date'].astype(str), format='%Y%m%d')

	data['start_time'] = data.Date.dt.tz_localize(tz)
	data['start_time'] = data.start_time.dt.tz_convert(timezone('UTC'))
	data['start_time'] = data.start_time + pd.to_timedelta((data['Period'] - 1) * 30, 'm')
	data['start_time'] = data.start_time.dt.tz_convert(tz)

	return data
