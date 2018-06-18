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


def dumpcommon(conn, cur, cnew, columns, table):
	# need to retrieve common type specific extra columns.
	temp = io.StringIO()
	cnew.to_csv(temp, index=False, header=False)
	temp.seek(0)
	cur.copy_from(file=temp, columns=columns, sep=',',  table='common.%s' % table)
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
				FROM area.%s
				WITH NO DATA;""" % table)

	cur.copy_from(file=temp, columns=areacol, sep=',', table='dumper')

	cur.execute(""" INSERT INTO area.%s
				select *
				from dumper
				ON CONFLICT DO NOTHING;""" % table)
	cur.execute(""" DROP TABLE dumper;""")
	# except pg.IntegrityError:
	# print('already in database')

	# conn.commit()
	print('series dumped')
	return None


def dumpborderseries(conn, cur, data, table):
	temp = io.StringIO()
	bordercol = ['source_id', 'dump_date', 'start_time', 'period', 'border_id', 'value']
	data.to_csv(temp, columns=bordercol, index=False, header=False)
	temp.seek(0)
	cur.execute("""CREATE TEMP TABLE dumpborder
				AS
				SELECT *
				FROM border.%s
				WITH NO DATA;""" % table)

	cur.copy_from(file=temp, columns=bordercol, sep=',', table='dumpborder')

	cur.execute("""INSERT INTO border.%s
				select *
				from dumpborder
				on conflict do nothing;""" % table)
	cur.execute("DROP TABLE dumpborder;")
	print('series dumped')
	pass


def common(conn, cur, data, cname):
	# add new common types to common data scheme
	ctype = readcommon(conn, cur, cname)

	newc = pd.DataFrame(getattr(data, cname).unique(), columns=[cname])
	newc = pd.merge(newc, ctype, on=cname, how='left')
	newc = newc[pd.isnull(newc.id)][[cname]]

	if not newc.empty:
		# if new fuel types found add to database and reread common data and merge
		dumpcommon(conn, cur, newc, cname, cname)
		ctype = readcommon(conn, cur, cname)
	else:
		print('no new %s' % cname)

	data = pd.merge(data, ctype[[cname, 'id']], on=cname, how='left')
	data.rename(columns={'id': '%s_id' % cname}, inplace=True)

	return data


def common_border(conn, cur, data, table_name):
	# add new border ids to common_border and check if area exist in areas

	# check for new areas and dump
	newareas = pd.DataFrame(pd.unique(data[['area_from', 'area_to']].values.ravel('K')),
							columns=['area'])
	common(conn, cur, newareas, 'area')

# check for new borders and dump
	borders = readcommon(conn, cur, 'v_borders')

	newborders = data[['area_from', 'area_to']].drop_duplicates()
	newborders = pd.merge(newborders, borders, how='left',
							left_on=['area_from', 'area_to'],
							right_on=['border_from', 'border_to'])
	newborders = newborders[pd.isnull(newborders.border_from)][['area_from', 'area_to']]

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
#		print(newborders.head())
		newborders.rename(columns={'id': 'border_target_id'}, inplace=True)
		newborders['area_function_id'] = 22
#		print(newborders.head())
		newborders = newborders[['area_function_id', 'border_source_id', 'border_target_id']]
		dumpcommon(conn, cur, newborders, list(newborders), table_name)
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
	data['start_time'] = data.start_time + pd.to_timedelta((data['Period'] - 1) * 30, 'm')
	data['start_time'] = data.start_time.dt.tz_convert(tz)

	return data
