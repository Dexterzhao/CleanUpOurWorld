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
    if pointlist[3] > pointlist[2]:
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


def level2count():


    # wood-1, glass-2, fabric-3, metal-4, food-5, plastic-6, paper-7, other-8

    conn = None
    typecount = {1:0, 2:0, 3:0, 4:0, 5:0, 6:0, 7:0, 8:0}

    # read connection parameters
    params = config()

    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        conn.set_session(autocommit=True)
        # create a cursor
        cur = conn.cursor()

        df = pdsql.read_sql_query('SELECT * FROM public.aggregatetable WHERE loclevel = 2;', conn)
        # print(df1)
        trashNameList =['wood', 'glass', 'fabric', 'metal', 'food', 'plastic', 'paper', 'other']
        insertcount = 0
        for i, row in df.iterrows():
            insertcount += 1
            print(insertcount)
            east = row['east']
            south = row['south']
            west = row['west']
            north =row['north']

            # point list, order N:y2, S:y1, E:x2, W:x1
            geomtxt = drawPolygon([north,south,east,west])
            typecount = dict.fromkeys(typecount, 0)

            for txt in geomtxt:
                df1 = pdsql.read_sql_query('SELECT wood, glass, fabric, metal, food, plastic, paper, other FROM public.maintable WHERE ST_Contains(ST_GEOMFROMTEXT(\'SRID=4326;'+txt+'\'),maintable.loc);', conn)
                for j, pointrow in df1.iterrows():
                    for i in range(1,9):
                        typecount[i] += pointrow[trashNameList[i-1]]
            countstr = ''
            for k in range(1,8):
                countstr = countstr + str(typecount[k])+','
            countstr += str(typecount[8])
            cur.execute('UPDATE public.aggregatetable SET (wood, glass, fabric, metal, food, plastic, paper, other) = ('+countstr+') WHERE inc_id ='+str(row['inc_id'])+';')
            # print(cur.mogrify('UPDATE public.aggregatetable SET (wood, glass, fabric, metal, food, plastic, paper, other) = ('+countstr+') WHERE inc_id ='+str(row['inc_id'])+';'))
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

def otherLevelCount(level):


    # wood-1, glass-2, fabric-3, metal-4, food-5, plastic-6, paper-7, other-8

    conn = None
    typecount = {1:0, 2:0, 3:0, 4:0, 5:0, 6:0, 7:0, 8:0}

    # read connection parameters
    params = config()

    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        conn.set_session(autocommit=True)
        # create a cursor
        cur = conn.cursor()

        df = pdsql.read_sql_query('SELECT * FROM public.aggregatetable WHERE loclevel = '+str(level)+';', conn)
        # print(df1)
        trashNameList =['wood', 'glass', 'fabric', 'metal', 'food', 'plastic', 'paper', 'other']
        insertcount = 0
        for i, row in df.iterrows():
            insertcount += 1
            print(insertcount)
            typecount = dict.fromkeys(typecount, 0)
            df1 = pdsql.read_sql_query('SELECT wood, glass, fabric, metal, food, plastic, paper, other FROM public.aggregatetable WHERE lastlevelid ='+ str(row['inc_id'])+';', conn)
            for j, pointrow in df1.iterrows():
                for i in range(1,9):
                    typecount[i] += pointrow[trashNameList[i-1]]
            countstr = ''
            for k in range(1,8):
                countstr = countstr + str(typecount[k])+','
            countstr += str(typecount[8])
            cur.execute('UPDATE public.aggregatetable SET (wood, glass, fabric, metal, food, plastic, paper, other) = ('+countstr+') WHERE inc_id ='+str(row['inc_id'])+';')
            # print(cur.mogrify('UPDATE public.aggregatetable SET (wood, glass, fabric, metal, food, plastic, paper, other) = ('+countstr+') WHERE inc_id ='+str(row['inc_id'])+';'))
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
    # level2count()
    otherLevelCount(6)
    # printColumnNames()
