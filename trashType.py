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


def level1count():
    # wood-1, glass-2, fabric-3, metal-4, food-5, plastic-6, paper-7, other-8 AFF.csv 'Bight 2013.csv','CleanCoastIndex2.csv','CleanCoastIndex.csv',done for test.
    sortedfiles =['GPS-PPM-Item-Earth.csv','NOAA.csv','ShorelineCleanup2014.csv','ShorelineCleanup2015.csv','ShorelineCleanup2016.csv']
    sortedfiles_number = {'AFF.csv' :{'est_volunt':8, 'num_bagsof':6, 'num_bags_1':6, 'num_realre':8, 'num_tires':7, 'est_looset':8, 'numplastic':6},\
    'Bight 2013.csv':{'totalbiode':8, 'totalbioha':8, 'totalconst':8, 'totalfabri':3, 'totalglass':2, 'totallarge':8, 'totalmetal':4, 'totalmisc':8,\
        'totalplast':6, 'totaltoxic':8, 'aluminumfo':4, 'aluminumor':4, 'autoparts':4, 'balloons':6, 'biodegrage':8, 'biohazardo':8, 'bricks':8, 'cds_dvds':6,\
        'cellphone':6,'ceramicpot':2, 'chemicalco':8, 'cigaretteb':8, 'concrete_a':8, 'constructi':8, 'deadanimal':8, 'ewaste':6, 'expandedfo':8, 'fabricothe':3,\
        'foamballs':6, 'foamrubber':6, 'foodwaste':5, 'furniture':1, 'garbagebag':8, 'glassbottl':2, 'glassbottl':2, 'hosepieces':3, 'humanwaste':8, 'largeother':8,\
        'lighters':6, 'metalbottl':4, 'metalobjec':4, 'metalpipe_':4, 'miscother':8, 'miscpieces':8, 'nailsscrew':4, 'naturalcot':3, 'nodebrispr':8, 'papercardb':7,\
        'pensormark':8, 'petwaste':8, 'plastic6pa':6, 'plasticbag':6, 'plasticbal':6, 'plasticbot':6, 'plasticcon':6, 'plasticcup':6, 'plastichar':6, 'plasticlid':6,\
        'plasticmis':6, 'plasticoth':6, 'plasticpip':6, 'plasticpip':6, 'plasticwra':6, 'rebar':4, 'ribbon':3, 'rubberpiec':6, 'shoppingca':4, 'smallbatte':4,\
        'softplasti':6, 'softplasti':6, 'spraypaint':8, 'styrofoamp':6, 'styrofoa_1':6, 'styrofoamc':6, 'syntheticf':8, 'syringesor':6, 'tarp':3, 'tires':6,\
        'toxicother':8, 'vinylrecor':6, 'waxpaper':7, 'waxedpaper':7, 'wire_barb_':8, 'wooddebris':1, 'yardwaste_':8},\
    'CleanCoastIndex2.csv':{'PET(1)':8,'HDPE (2) shopping':6,'HDPE(2) Cans/Caps':4,'PVC(3)':6,'LDPE(4)':6,'PP(5)':6,'Polystyrene(6) Regi + Sponge':3,\
        'Polystyrene(6) Cups/Spoon':6,'Other(7)':8,'paper':7,'cardboard':7,'Glass White':2,'Glass Colour':2,'Scrap Metal':4,'Metal Tin & Cans':4,\
        'rubber':6,'Cloth/tex ':3,'wood':1,'construction':8,'No. of plastics':6},\
    'CleanCoastIndex.csv':{'PET(1)':8,'HDPE (2) shopping':6,'HDPE(2) Cans/Caps':4,'PVC(3)':6,'LDPE(4)':6,'PP(5)':6,'Polystyrene(6)':6,'Other(7)':8,\
        'paper':7,'cardboard':7,'Glass White':2,'Glass Colour':2,'Scrap Metal':4,'Metal Tin & Cans':4,'rubber':6,'Cloth tex':3,'wood':1,'construction':8,\
        'No. of plastics':6},\
    'GPS-PPM-Item-Earth.csv':{'bags':6,'cigarettebutts':7,'foodwrapperscandychipsetc':7,'takeoutawaycontainersplastic':6,'takeoutawaycontainersfoam':6,\
        'bottlecapsplastic':6,'bottlecapsmetal':4,'lidsplastic':6,'strawsstirrers':6,'forksknivesspoons':6,'beveragebottlesplastic':6,'beveragebottlesglass':2,\
        'beveragecans':4,'grocerybagsplastic':6,'otherplasticbags':6,'paperbags':7,'cupsplatespaper':7,'cupsplatesplastic':6,'cupsplatesfoam':6,\
        'fishingbuoyspotstraps':3,'fishingnetandpieces':3,'fishingline1yardmeter1piece':6,'rope1yardmeter1piece':6,'FishingGearClean Swell':6,\
        'sixpackholders':6,'otherplasticfoampackaging':6,'otherplasticbottlesoilbleachetc':6,'strappingbands':6,'tobaccopackagingwrap':7,'otherpackagingcleanswell':8,\
        'appliancesrefrigeratorswashersetc':8,'balloons':6,'cigartips':7,'cigarettelighters':7,'constructionmaterials':8,'fireworks':8,'tires':6,'toys':8,\
        'othertrashcleanswell':8,'condoms':6,'diapers':7,'syringes':6,'tamponstamponapplicators':3,'PersonalHygieneClean Swell':8,'foampieces':6,'glasspieces':2,\
        'plasticpieces':6},\
    'NOAA.csv':{'plastic':6,'hardplasti':6,'foamedplas':6,'filmedplas':6,'foodwrappe':6,'plasticbev':6,'otherjugsc':8,'bottlecont':2,'cigartips':7,'cigarettes':7,\
        'disposable':8,'sixpackrings':6,'bags':6,'plasticrop':6,'buoysfloat':6,'fishinglur':6,'cups':6,'plasticute':6,'straws':6,'balloonsmy':6,'personalca':8,\
        'plasticoth':6,'metal':4,'aluminumti':4,'aerosolcan':4,'metalfragm':4,'metalother':4,'glass':2,'glassbever':2,'jars':2,'glassfragm':2,'glassother':2,\
        'rubber':6,'flipflops':4,'rubberglov':6,'tires':6,'balloonsla':6,'rubberfrag':6,'rubberothe':6,'processedl':8,'cardboardc':7,'paperandca':7,'paperbags':7,\
        'lumberbuil':8,'processe_1':8,'clothfabri':3,'clothingsh':3,'glovesnonr':3,'towelsrags':3,'ropenetpie':3,'fabricpiec':3,'clothfab_1':3,'unclassifi':8},\
    'ShorelineCleanup2014.csv':{'Cigarettes/Cigarette Filters':7,'Food Wrappers/Containers':7,'Takeout Containers (Plastic)':6,'Takeout Containers (Foam)':6,\
        'Bottle Caps (Plastic)':6,'Bottle Caps (Metal)':4,'Lids (Plastic)':6,'Straws Stirrers':6,'Forks Knives Spoons':6,'Beverage Bottles (Plastic) 2 liters or less':6,\
        'Glass Beverage Bottles':2,'Beverage Cans':4,'Bags (Plastic)':6,'Other plastic bags':6,'Bags (Paper)':7,'Cups and Plates (Paper)':7,'Cups and Plates (Plastic)':6,\
        'Cups and Plates (Foam)':6,'Buoys Floats':6,'Fishing Line':6,'Fishing Nets':6,'rope':3,'Fishing Lures/Light Sticks':6,'6-Pack Holders':6,\
        'Other Plastic/Foam Packaging':6,'Other Plastic Bottles (oil bleach etc)':6,'Strapping Bands':6,'Tobacco Packaging/Wrappers':7,\
        'Appliances (refrigerators washers etc.)':8,'balloons':6,'Cigar Tips':7,'Cigarette Lighters':4,'Building Materials':8,'fireworks':8,'tires':6,'batteries':4,\
        'Clothing Shoes':3,'toys':3,'condoms':6,'diapers':7,'syringes':6,'Tampons/Tampon Applicators':7,'Foam Pieces':6,'Glass Pieces':2,'Plastic Pieces':6},\
    'ShorelineCleanup2015.csv':{'Cigarettes/Cigarette Filters':7,'Food Wrappers/Containers':7,'Takeout Containers (Plastic)':6,'Takeout Containers (Foam)':6,\
        'Bottle Caps (Plastic)':6,'Bottle Caps (Metal)':4,'Lids (Plastic)':6,'Straws Stirrers':6,'Forks Knives Spoons':6,'Beverage Bottles (Plastic) 2 liters or less':6,\
        'Glass Beverage Bottles':2,'Beverage Cans':4,'Bags (Plastic)':6,'Other plastic bags':6,'Bags (Paper)':7,'Cups and Plates (Paper)':7,'Cups and Plates (Plastic)':6,\
        'Cups and Plates (Foam)':6,'Buoys Floats':6,'Fishing Line':6,'Fishing Nets':6,'rope':3,'Fishing Lures/Light Sticks':6,'6-Pack Holders':6,\
        'Other Plastic/Foam Packaging':6,'Other Plastic Bottles (oil bleach etc)':6,'Strapping Bands':6,'Tobacco Packaging/Wrappers':7,\
        'Appliances (refrigerators washers etc.)':8,'balloons':6,'Cigar Tips':7,'Cigarette Lighters':4,'Building Materials':8,'fireworks':8,'tires':6,\
        'batteries':4,'Clothing Shoes':3,'toys':3,'condoms':6,'diapers':7,'syringes':6,'Tampons/Tampon Applicators':7,'Foam Pieces':6,\
        'Glass Pieces':2,'Plastic Pieces':6},\
    'ShorelineCleanup2016.csv':{'Food Wrappers':7,'Take Out/Away Containers (Plastic)':6,'Take Out/Away Containers (Foam)':6,'Bottle Caps (Plastic)':6,\
        'Bottle Caps (Metal)':4,'Lids (Plastic)':6,'Straws Stirrers':6,'Forks Knives Spoons':6,'Beverage Bottles (Plastic)':6,'Beverage Bottles (Glass)':2,\
        'Beverage Cans':4,'Grocery Bags (Plastic)':6,'Other Plastic Bags':6,'Paper Bags':7,'Cups & Plates (Paper)':7,'Cups & Plates (Plastic)':6,\
        'Cups & Plates (Foam)':6,'6-Pack Holders':6,'Cigarette Butts':7,'Cigar Tips':7,'Cigarette Lighters':4,'Tobacco Packaging/Wrap':7,'Fishing Buoys Pots & Traps':6,\
        'Fishing Line (1 yard/meter = 1 piece)':6,'Fishing Net & Pieces':6,'Rope (1 yard/meter = 1 piece)':3,'Fishing Lures/Light Sticks':6,'condoms':6,'diapers':7,\
        'syringes':6,'Tampons/Tampon Applicators':7,'appliances':8,'balloons':6,'Construction Materials':8,'fireworks':8,'tires':6,'Other Plastic/Foam Packaging':6,\
        'Other Plastic Bottles':6,'Strapping bands':6,'batteries':4,'Clothing Shoes':3,'toys':3,'Foam Pieces':6,'Glass Pieces':2,'Plastic Pieces':6}
    }
    # wood-1, glass-2, fabric-3, metal-4, food-5, plastic-6, paper-7, other-8
    sortedfiles_weight = {'BASMAA.csv' :{'Recyclable Beverage Containers (CRV Labeled) (lbs)':6, 'Plastic Grocery Bags (lbs)':8, \
        'Styrofoam Food and Beverage Ware (lbs)':5, 'Other Plastic (lbs)':8, 'Paper (lbs)':7, 'Metal (lbs)':4, 'Misc. (lbs)':8 }
    }
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

        for filename in sortedfiles:
            # cur.execute('SELECT * FROM public.'+filename+' where;')
            df = pdsql.read_sql_query('SELECT * FROM public.\"'+filename+'\";', conn)
            print('\n'+filename)
            for i, row in df.iterrows():
                typecount = dict.fromkeys(typecount, 0)
                for trashname,count in sortedfiles_number[filename].items():
                    if row[trashname] != None and row[trashname].isdigit():
                        typecount[count] += int(row[trashname])
                countstr = ''
                for i in range(1,8):
                    countstr = countstr + str(typecount[i])+','
                countstr += str(typecount[8])
                cur.execute('UPDATE public.maintable SET (wood, glass, fabric, metal, food, plastic, paper, other) = ('+countstr+') WHERE tablename = \''+filename+'\' and tableid ='+str(row['inc_id'])+';')
                # print(cur.mogrify('UPDATE public.maintable SET (wood, glass, fabric, metal, food, plastic, paper, other) = ('+countstr+') WHERE tablename = \''+filename+'\' and tableid ='+str(row['inc_id'])+';'))
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

def printColumnNames():
    conn = None

    try:

        params = config()

        filenames =['CleanCoastIndex2.csv','CleanCoastIndex.csv','GPS-PPM-Item-Earth.csv','NOAA.csv','ShorelineCleanup2014.csv','ShorelineCleanup2015.csv','ShorelineCleanup2016.csv']
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        conn.set_session(autocommit=True)

        # create a cursor
        cur = conn.cursor()
        for files in filenames:
            cur.execute('Select * FROM public.\"'+files+'\" LIMIT 0;' )
            colnames = [desc[0] for desc in cur.description]
            nameToType = ''
            print('\n'+files)
            for name in colnames:
                nameToType += '\''+name + '\':0,'
            print(nameToType)
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
    level1count()
    # printColumnNames()
