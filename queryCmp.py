#!/usr/bin/python
from configparser import ConfigParser
from osgeo import ogr
import psycopg2
import psycopg2.extensions
from psycopg2.extras import LoggingConnection, LoggingCursor
import logging
import time
import pandas as pd
import os
import linecache
import sys
import csv
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import re


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
time_total = 0
cmpStats = {}

# MyLoggingCursor simply sets self.timestamp at start of each query
class MyLoggingCursor(LoggingCursor):
    def execute(self, query, vars=None):
        self.timestamp = time.time()
        return super(MyLoggingCursor, self).execute(query, vars)

    def callproc(self, procname, vars=None):
        self.timestamp = time.time()
        return super(MyLoggingCursor, self).callproc(procname, vars)

# MyLogging Connection:
#   a) calls MyLoggingCursor rather than the default
#   b) adds resulting execution (+ transport) time via filter()
class MyLoggingConnection(LoggingConnection):
    def filter(self, msg, curs):
        # print(msg)
        global time_total
        exectime = time.time() - curs.timestamp
        time_total = time_total + exectime
        # return msg.decode("utf-8") + "   %d ms" % int(exectime * 1000)
        # return msg.decode("utf-8") + ' time:' +str(exectime)
        return

    def cursor(self, *args, **kwargs):
        kwargs.setdefault('cursor_factory', MyLoggingCursor)
        return LoggingConnection.cursor(self, *args, **kwargs)

def queryCompare():

    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(connection_factory=MyLoggingConnection,**params)
        conn.initialize(logger)
        # create a cursor
        cur = conn.cursor()

        # execute a statement
        # print('PostgreSQL database version:')
        # cur.execute('SELECT *, ST_AsText(loc) FROM maintable WHERE ST_Contains(ST_GEOMFROMTEXT(\'SRID=4326;POLYGON((30 45,45 45,45 50,30 50,30 45))\'),maintable.loc)')
        # query1(cur)
        # query2(cur)
        # query3(cur)
        # with open('stats.csv', 'w') as f:  # Just use 'w' mode in 3.x
        #     w = csv.DictWriter(f, cmpStats.keys())
        #     w.writeheader()
        #     w.writerow(cmpStats)
        # f.close()

        csv_file = csv.DictReader(open("stats.csv"))

        # csv_file.close()
        for x in csv_file:
            csvDict = x
            break
        for query in ['query1', 'query2', 'query3']:
            x = [200,1000,5000,25000,125000]
            # axes = plt.subplot()
            # axes.set_xticks(x)
            plt.xscale('log')
            plt.yscale('log')
            plt.xticks(x,  x)

            # ax.xaxis.set_major_locator(ticker.MultipleLocator(5))
            # plt.xticks(np.arange(5), x)
            y = re.findall(r"[-+]?\d*\.\d+|\d+", csvDict[query])
            y.reverse()
            fy= [float(i) for i in y]
            # plt.yticks(y,  y)
            plt.xlabel(x)
            plt.plot(x,fy, label = query)

            # fig = plt.figure()
            # ax = fig.add_subplot()
            for i,j in zip(x, fy):                                       # <--
                plt.annotate('%.2f' % j, xy=(i,j), textcoords='data')
        plt.xlabel('Query Size')
        plt.ylabel('Execution Time(s)')

        plt.title("Tentative result")

        plt.legend()
        plt.show()
        # print(conn.filter())
        # conn.commit()
        # polygon_select(cur)
     # close the communication with the PostgreSQL
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

def polygon_select(cur):
    x1_zero= 40
    x2_zero= 40
    y1_zero= 20
    y2_zero= 20
    ac = 0.01
    max = int(90/ac)
    for i in range(0, max, 1):
        y1 = y1_zero - i * ac
        y2 = y2_zero + i * ac
        x1 = x1_zero - i * ac * 2
        x2 = x2_zero + i * ac * 2
        # Create ring
        ring = ogr.Geometry(ogr.wkbLinearRing)
        ring.AddPoint(x1, y1)
        ring.AddPoint(x2, y1)
        ring.AddPoint(x2, y2)
        ring.AddPoint(x1, y2)
        ring.AddPoint(x1, y1)


        # Create polygon
        poly = ogr.Geometry(ogr.wkbPolygon)
        poly.AddGeometry(ring)
        poly.FlattenTo2D()
        geomtxt = poly.ExportToWkt()

        cur.execute('SELECT postgis.ST_AsText(loc) FROM public.maintable WHERE ST_Contains(ST_GEOMFROMTEXT(\'SRID=4326;'+geomtxt+'\'),maintable.loc);')
        # if cur.rowcount in (range(100,300) or range(900,1100) or range(4500,5500) or range(22500,27500) or range(112500,137500)):
        print(geomtxt)
        print(cur.rowcount)

def query1(cur):
    stats ={}

    geomtext =['POLYGON ((-82.6 -41.3,82.6 -41.3,82.6 41.3,-82.6 41.3,-82.6 -41.3))',\
    'POLYGON ((-75.6 -37.8,75.6 -37.8,75.6 37.8,-75.6 37.8,-75.6 -37.8))',\
    'POLYGON ((-40 -20,40 -20,40 20,-40 20,-40 -20))',\
    'POLYGON ((32.4 16.2,47.6 16.2,47.6 23.8,32.4 23.8,32.4 16.2))',\
    'POLYGON ((36.62 18.31,43.38 18.31,43.38 21.69,36.62 21.69,36.62 18.31))']
    for x in geomtext:
        global time_total
        time_total = 0
        cur.execute('SELECT * FROM public.maintable WHERE ST_Contains(ST_GEOMFROMTEXT(\'SRID=4326;'+x+'\'),maintable.loc);')
        rows = cur.fetchall()
        for record in rows:
            # print(record)
            cur.execute('SELECT * FROM public."'+ record[2]+'" WHERE inc_id =' + str(record[3]))
        print(time_total)
        if 'query1' in cmpStats.keys():
            cmpStats['query1'].append(time_total)
        else:
            cmpStats['query1']=[]
            cmpStats['query1'].append(time_total)

def query2(cur):
    geomtext =['POLYGON ((-82.6 -41.3,82.6 -41.3,82.6 41.3,-82.6 41.3,-82.6 -41.3))',\
    'POLYGON ((-75.6 -37.8,75.6 -37.8,75.6 37.8,-75.6 37.8,-75.6 -37.8))',\
    'POLYGON ((-40 -20,40 -20,40 20,-40 20,-40 -20))',\
    'POLYGON ((32.4 16.2,47.6 16.2,47.6 23.8,32.4 23.8,32.4 16.2))',\
    'POLYGON ((36.62 18.31,43.38 18.31,43.38 21.69,36.62 21.69,36.62 18.31))']
    for x in geomtext:
        global time_total
        time_total = 0
        cur.execute('SELECT DISTINCT tablename FROM public.maintable WHERE ST_Contains(ST_GEOMFROMTEXT(\'SRID=4326;'+x+'\'),maintable.loc);')
        rows = cur.fetchall()
        # print(x+":"+str(time_total))
        for record in rows:
            # print(record)
            cur.execute('SELECT * FROM public."'+ record[0]+'" WHERE ST_Contains(ST_GEOMFROMTEXT(\'SRID=4326;'+x+'\'),"'+record[0]+'".loc);' )
            # print(record[0]+":"+str(time_total))
        print(x+":"+str(time_total)+'\n')
        if 'query2' in cmpStats.keys():
            cmpStats['query2'].append(time_total)
        else:
            cmpStats['query2']=[]
            cmpStats['query2'].append(time_total)
    print(cmpStats['query2'])


def query3(cur):
    geomtext =['POLYGON ((-82.6 -41.3,82.6 -41.3,82.6 41.3,-82.6 41.3,-82.6 -41.3))',\
    'POLYGON ((-75.6 -37.8,75.6 -37.8,75.6 37.8,-75.6 37.8,-75.6 -37.8))',\
    'POLYGON ((-40 -20,40 -20,40 20,-40 20,-40 -20))',\
    'POLYGON ((32.4 16.2,47.6 16.2,47.6 23.8,32.4 23.8,32.4 16.2))',\
    'POLYGON ((36.62 18.31,43.38 18.31,43.38 21.69,36.62 21.69,36.62 18.31))']
    filetext =['AFF', 'BASMAA', 'Bight 2013', 'CleanCoastIndex', 'CleanCoastIndex2', 'ConneticutRiverConservancy', 'Eriksen', 'GPS-PPM-Item-Earth', 'Lebreton', 'MDTData', 'NOAA', 'ShorelineCleanup2014', 'ShorelineCleanup2015', 'ShorelineCleanup2016', 'TotalOldCCdata']

    for x in geomtext:
        global time_total
        time_total = 0
        for record in filetext:

            cur.execute('SELECT * FROM public."'+ record+'" WHERE ST_Contains(ST_GEOMFROMTEXT(\'SRID=4326;'+x+'\'),"'+record+'".loc);' )
            # print(record+":"+str(time_total))
        print(x+":"+str(time_total)+'\n')
        if 'query3' in cmpStats.keys():
            cmpStats['query3'].append(time_total)
        else:
            cmpStats['query3']=[]
            cmpStats['query3'].append(time_total)


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
    queryCompare()
