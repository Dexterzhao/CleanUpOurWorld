#!/usr/bin/python
from configparser import ConfigParser
import linecache
import numpy as np
from osgeo import ogr
import psycopg2
import psycopg2.extensions
import pandas as pd
import pandas.io.sql as pdsql
import re
import sys


def drawPolygon(pointlist):
    geomtxt = []
    polyList = []
    if pointlist[3]>0 and pointlist[2]<0:
        polyList.append([pointlist[0],pointlist[1],180.0,pointlist[3]])
        polyList.append([pointlist[0],pointlist[1],pointlist[2],-180.0])
    else:
        polyList.append(pointlist)
    for poly in polyList:
        ring = ogr.Geometry(ogr.wkbLinearRing)
        ring.AddPoint(poly[3], poly[1])
        ring.AddPoint(poly[2], poly[1])
        ring.AddPoint(poly[2], poly[0])
        ring.AddPoint(poly[3], poly[0])
        ring.AddPoint(poly[3], poly[1])

        # Create polygon
        polygon = ogr.Geometry(ogr.wkbPolygon)
        polygon.AddGeometry(ring)
        polygon.FlattenTo2D()
        txt = polygon.ExportToWkt()
        geomtxt.append(txt)
    return geomtxt


def level6Divide():

    conn = None

    try:
        # For level 6 aggregate table division
        df = pd.read_csv('level6.csv')
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        conn.set_session(autocommit=True)

        # create a cursor
        cur = conn.cursor()

        df['north'] = np.nan
        df['south'] = np.nan
        df['east'] = np.nan
        df['west'] = np.nan

        df['trashamount'] = 0
        df['level'] = 6
        df['tablenames'] = pd.np.empty((len(df), 0)).tolist()

        for i, row in df.iterrows():
            trashamount = 0
            # delete the space
            df.at[i,'bounds'] = row['bounds'].replace('\n','').replace(' ','')
            # point list, order N:y2, S:y1, E:x2, W:x1
            point_list = row['bounds'].split(',')
            for x in range(0, len(point_list), 1):
                # print(re.findall(r"[-+]?\d*\.?\d+", point_list[x])[0])
                point_list[x] = float(re.findall(r"[-+]?\d*\.?\d+|\d+", point_list[x])[0])
            geomtxt = drawPolygon(point_list)

            for txt in geomtxt:
                # Count the rows for trash amount
                cur.execute('SELECT date FROM public.maintable WHERE ST_Contains(ST_GEOMFROMTEXT(\'SRID=4326;'+txt+'\'),maintable.loc);')
                # for record in cur:
                #     df.at[i,'date'].append(record['tablename'])
                # print(geomtxt)
                df.at[i,'north'] = point_list[0]
                df.at[i,'south'] = point_list[1]
                df.at[i,'east'] = point_list[2]
                df.at[i,'west'] = point_list[3]
                print(point_list)
                trashamount += cur.rowcount
                # print(trashamount,geomtxt)
                # Count the table names
                cur.execute('SELECT DISTINCT tablename FROM public.maintable WHERE ST_Contains(ST_GEOMFROMTEXT(\'SRID=4326;'+txt+'\'),maintable.loc);')
                # print(cur.mogrify('SELECT DISTINCT tablename FROM public.maintable WHERE ST_Contains(ST_GEOMFROMTEXT(\'SRID=4326;'+geomtxt+'\'),maintable.loc);'))

                for record in cur:
                    df.at[i,'tablenames'].append(record[0])
                df.at[i,'tablenames'] = list(set(df.at[i,'tablenames']))
                # print(record)
            # print('INSERT INTO public.aggregatetable (trashamount, loclevel, tablenames, north, south, east, west) VALUES('+str(trashamount)+','+str(row['level'])+',\'{'+','.join(row['tablenames'])+'}\','+str(point_list[0])+','+str(point_list[1])+','+str(point_list[2])+','+str(point_list[3]) +')')
            cur.execute('INSERT INTO public.aggregatetable (trashamount, loclevel, tablenames, north, south, east, west, nextlevel) VALUES('+str(trashamount)+','+str(row['level'])+',\'{'+','.join(row['tablenames'])+'}\','+str(point_list[0])+','+str(point_list[1])+','+str(point_list[2])+','+str(point_list[3])+',\''+row['nextlevel']+'\')')
            # cur.execute('INSERT INTO public.aggregatetable (trashamount, loclevel, tablenames, north, south, east, west) VALUES(0,6,\'{1,2,3}\',-53.88,90.0,-169.09,-169.08)')
        # cur.execute('SELECT * FROM aggregatetable')
        # print(cur.statusmessage)
        # for record in cur:
        #     print(record)
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        exc_type, exc_obj, tb = sys.exc_info()
        f = tb.tb_frame
        lineno = tb.tb_lineno
        filename = f.f_code.co_filename
        linecache.checkcache(filename)
        line = linecache.getline(filename, lineno, f.f_globals)
        print ('EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj))
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

def otherLevelDivide(level, nextlevel):
    conn = None

    try:

        params = config()


        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        conn.set_session(autocommit=True)

        # create a cursor
        cur = conn.cursor()

        df = pd.DataFrame(columns = ['trashamount', 'level', 'tablenames', 'north', 'south', 'east', 'west'])
        df['north'] = np.nan
        df['south'] = np.nan
        df['east'] = np.nan
        df['west'] = np.nan

        df['trashamount'] = 0
        df['level'] = level
        df['tablenames'] = pd.np.empty((len(df), 0)).tolist()


        # fetching higher level regions
        # cur.execute('SELECT * FROM public.aggregate WHERE loclevel = '+str(level+1)+';')
        df1 = pdsql.read_sql_query('SELECT * FROM public.aggregatetable WHERE loclevel = '+str(level+1)+';', conn)
        # print(df1)
        for i, row in df1.iterrows():

            lastlevelid = row['inc_id']
            level_division = row['nextlevel'].split('x')
            # point list, order N:y2, S:y1, E:x2, W:x1
            # point_list = row['nextlevel'].split(',')
            lat_interval = (row['south'] - row['north'])/float(level_division[1])
            if(row['west']>0 and row['east']<0):
                lon_interval = (row['east'] - row['west']+ 360.0)/float(level_division[0])
            else:
                lon_interval = (row['east'] - row['west'])/float(level_division[0])
            # print(row['east'],row['west'],'\n\n\n')
            for x in range(0, int(level_division[1]), 1):
                for y in range(0, int(level_division[0]),1):

                    tablenames = []
                    trashamount = 0


                    point_list = [row['north']+lat_interval*x,row['north']+lat_interval*(x+1.0),row['west']+lon_interval*(y+1.0),row['west']+lon_interval*y]
                    if point_list[2] > 180: point_list[2] -= 360.0
                    if point_list[3] > 180: point_list[3] -= 360.0
                    geomtxt = drawPolygon(point_list)

                    for txt in geomtxt:
                        # Count the rows for trash amount
                        cur.execute('SELECT date FROM public.maintable WHERE ST_Contains(ST_GEOMFROMTEXT(\'SRID=4326;'+txt+'\'),maintable.loc);')
                        # for record in cur:
                        #     df.at[i,'date'].append(record['tablename'])
                        # print(geomtxt)
                        # df.at[i,'north'] = point_list[0]
                        # df.at[i,'south'] = point_list[1]
                        # df.at[i,'east'] = point_list[2]
                        # df.at[i,'west'] = point_list[3]

                        # print(point_list)
                        trashamount += cur.rowcount
                        # print(trashamount,geomtxt)
                        # Count the table names
                        if(cur.rowcount > 0):
                            cur.execute('SELECT DISTINCT tablename FROM public.maintable WHERE ST_Contains(ST_GEOMFROMTEXT(\'SRID=4326;'+txt+'\'),maintable.loc);')
                            # print(cur.mogrify('SELECT DISTINCT tablename FROM public.maintable WHERE ST_Contains(ST_GEOMFROMTEXT(\'SRID=4326;'+geomtxt+'\'),maintable.loc);'))
                            for record in cur:
                                tablenames.append(record[0])
                            tablenames = list(set(tablenames))
                            # print(record)
                    # print('INSERT INTO public.aggregatetable (trashamount, loclevel, tablenames, north, south, east, west) VALUES('+str(trashamount)+','+str(row['level'])+',\'{'+','.join(row['tablenames'])+'}\','+str(point_list[0])+','+str(point_list[1])+','+str(point_list[2])+','+str(point_list[3]) +')')
                    if(trashamount > 0):
                        cur.execute('INSERT INTO public.aggregatetable (trashamount, loclevel, tablenames, north, south, east, west, nextlevel, lastlevelid) VALUES('+str(trashamount)+','+str(level)+',\'{'+','.join(tablenames)+'}\','+str(point_list[0])+','+str(point_list[1])+','+str(point_list[2])+','+str(point_list[3])+',\''+nextlevel+'\','+str(lastlevelid)+')')
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        exc_type, exc_obj, tb = sys.exc_info()
        f = tb.tb_frame
        lineno = tb.tb_lineno
        filename = f.f_code.co_filename
        linecache.checkcache(filename)
        line = linecache.getline(filename, lineno, f.f_globals)
        print ('EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj))
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

def clearLevel(level):

    conn = None

    try:
        params = config()
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        conn.set_session(autocommit=True)
        # create a cursor
        cur = conn.cursor()
        cur.execute('DELETE FROM public.aggregatetable WHERE loclevel='+str(level)+';')
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        exc_type, exc_obj, tb = sys.exc_info()
        f = tb.tb_frame
        lineno = tb.tb_lineno
        filename = f.f_code.co_filename
        linecache.checkcache(filename)
        line = linecache.getline(filename, lineno, f.f_globals)
        print ('EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj))
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

def setArea(level):

    conn = None

    try:
        params = config()
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        conn.set_session(autocommit=True)
        # create a cursor
        cur = conn.cursor()

        df1 = pdsql.read_sql_query('SELECT * FROM public.aggregatetable WHERE loclevel = '+str(level)+';', conn)
        # print(df1)
        for i, row in df1.iterrows():


            # point list, order N:y2, S:y1, E:x2, W:x1
            # point_list = row['nextlevel'].split(',')

            incid = row['inc_id']
            if(row['west']>0 and row['east']<0):
                mod_east = row['east'] + 360.0
                print(mod_east)
                cur.execute('UPDATE aggregatetable SET area= ST_SetSRID(ST_MakeEnvelope(west,south,'+str(mod_east)+',north), 4326) where inc_id = '+str(incid)+';')
                # print(cur.mogrify('UPDATE aggregatetable SET area= ST_SetSRID(ST_MakeEnvelope(west,south,'+str(mod_east)+',north), 4326) where loclevel = '+str(level)+';'))
            else:
                cur.execute('UPDATE aggregatetable SET area= ST_SetSRID(ST_MakeEnvelope(west,south,east,north), 4326) where inc_id = '+str(incid)+';')
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        exc_type, exc_obj, tb = sys.exc_info()
        f = tb.tb_frame
        lineno = tb.tb_lineno
        filename = f.f_code.co_filename
        linecache.checkcache(filename)
        line = linecache.getline(filename, lineno, f.f_globals)
        print ('EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj))
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

def config(filename='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db

if __name__ == "__main__":
    # level6Divide()
    # otherLevelDivide(5,'2x2')
    # otherLevelDivide(4,'8x8')
    # otherLevelDivide(3,'8x8')
    # otherLevelDivide(2,'1x1')
    # setArea(6)
    setArea(5)
    setArea(4)
    setArea(3)
    setArea(2)
    # clearLevel(5)
    # clearLevel(4)
    # clearLevel(3)
    # clearLevel(2)
    # clearLevel(1)
