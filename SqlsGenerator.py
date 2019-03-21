from os import listdir
from os.path import isfile, join
import pandas as pd



mypath='/home/dexter/Documents/PostGisData'
onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
fredict = {}
frecnt = {}

onlyfiles.sort()

for filename in onlyfiles:
    df = pd.read_csv(mypath+'/'+filename, error_bad_lines=False)
    cn = df.columns
    n = cn.tolist()

    #importing csv
    print('select load_csv_file(\''+filename+'\',\'/home/dexter/Documents/PostGisData/'+filename+'\''+','+str(len(n))+');'+'\n')

    # update public."TotalOldCCdata" set longitude = substring (longitude FROM '\-?\d+\.?\d*');
    # print('update public."'+filename+'" set longitude = substring (longitude FROM \'\\-?\d+\.?\d*\');')

    # ALTER TABLE public."TotalOldCCdata" ALTER COLUMN latitude TYPE DOUBLE PRECISION USING latitude::double precision;
    # print('ALTER TABLE public."'+filename+'" ALTER COLUMN latitude TYPE DOUBLE PRECISION USING latitude::double precision;')

    #ALTER TABLE table_name ADD COLUMN new_column_name data_type;
    # print('ALTER TABLE public."'+filename+'" ADD COLUMN tname text;')

    #UPDATE mytable SET table_column = 'test';
    # print('UPDATE public."'+filename+'" SET tname = \''+filename+'\';')

    # SELECT AddGeometryColumn ('t1', 'loc', 4326, 'POINT', 2);
    # print('SELECT AddGeometryColumn(\''+filename+ '\',\'loc\', 4326, \'POINT\', 2);')

    # UPDATE maintable SET loc= ST_SetSRID(ST_MakePoint(longitude, latitude), 4326);
    # print('UPDATE "'+filename+ '" SET loc= ST_SetSRID(ST_MakePoint(longitude, latitude), 4326);')

    # INSERT INTO maintable(date,tablename,tableid,latitude,longitude)
    # SELECT date,tname,inc_id,latitude,longitude
    # from public."AFF"
    # print('INSERT INTO maintable(date,tablename,tableid,latitude,longitude) SELECT date,tname,inc_id,latitude,longitude from public."'+filename+'";')
