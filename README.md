Environmental Data Importing Management using POSTGIS based on postgreSQL, and visualization based on GoogleMap and Flask
==============================================================


Install postgre and postgis before you start.

The following link gives specifc instructions.
<https://trac.osgeo.org/postgis/wiki/UsersWikiPostGIS24UbuntuPGSQL10Apt>

Steps for adding new files
--------------------------
1. Import csv file, using *SqlsGenerator.py*, as well as adding some columns(date,tname,inc_id,latitude,longitude), among them date might be in the original file, but in a form of different names.

2. Insert the data into maintable, with
```sql
INSERT INTO maintable(date,tablename,tableid,latitude,longitude) SELECT date,tname,inc_id,latitude,longitude from public.YOURFILE
```


Scripts
-------
There are some scripts used to help importing these files, make sure you have installed python and dependent modules to run them.

1. Suppose you have a bunch of *.csv* files

The *FrequencySummary.py* helps you to count the colomn names. Remember to change the **mypath** variable in the code to your own directory to find these *.csv* files. Then
```
$ python FrequencySummary.py
```
Data Format, when the following code won't work, use text editor to load the file and save it, this can be scripted as shell command in Linux like system.
```bash
#output encoding of your file
$ file -i yourfile
#convert encoding
$ iconv options -f from-encoding -t to-encoding input -o output
```
Delete empty columns
<https://www.extendoffice.com/documents/excel/823-excel-delete-multiple-empty-columns.html>
Or we can do it in sql:
```sql
SELECT * FROM table_name WHERE column_name IS NULL
```
If the last statement returns anything, then
```sql
ALTER TABLE table_name DROP COLUMN column_name;
```
2. To reduce manual works that are simply some sqls with different table names, *SqlGenerator.py* is used to generate sqls for all your *.csv* files to create maintable. The script generates sqls after all the *.csv* files imported by load_csv_file functions, these sqls can be executed using **cur.execute()** if further automation is needed, though the script only prints these sqls.

Remember to change the **mypath** variable in the code to your own directory to find these *.csv* files.
```
$ python SqlsGenerator.py
```

3. When you have some *.csv* files that only have location names but not the specific coordinates. Use *LocationToCooridnates.py* to extract the geographical locations from location names. In the case of this script, there are two columns that have related information to get coordinates, which are **water body** and **city**. This requires some basic understandings of python programming.  The location description is limited to 10 words in order to avoid errors, this has not been tested for possible extension. This will generate another *.csv* file.

4. *queryCmp.py* is used to compare customized queries at different scale, **polygon_select()** is used to select a polygon based on the same center but different areas in ascending order. Then the polygon can be used to measure the query performance using built-in logging mechanism from *psycopg2*, **queryCompare()** also records the statistics and plots the data.

5. *levelDivide.py* is employed to divide the whole earth to 5 levels, from a user-defined whole earth division(level6.csv, including 10 districts), **level6Divide()** generates records for level 6, while **otherLevelDivide()** deal with levels from 5 to 2. This craetes points for aggregatetable.

6. *trashType.py* is used to generate trash type summary in maintable, for every specific location, after this, we can collect trash types based on a bottom-up approach.

7. *aggreType.py* is used to collect trash type information to inserted to aggregatetable based on maintable, **level2count()** summarizes trash types from maintable, while **otherLevelCount()** is responsible for level 3 to 6 based on itself(aggregatetable)

Sqls
----
To load the csv into the database you might need to create a built-in function using sqls in *loadcsv.sql*.

There is one line commented to avoid errors, which is used to delete the first column of the table. this can be done manually or you can uncomment if there is no error in your postgre database.

To use this, change the **schema_name** in line *set schema* copy the file into any query interface or shell that has been connected to your database and execute it.

Some important sqls:
1. Importing data from another table
```sql
INSERT INTO target_table_name(date,tablename,tableid,latitude,longitude)
SELECT date,tname,inc_id,latitude,longitude
from schema_name.source_table_name
```

2. Add a geometry column(point of two coordinates)  to a table
```sql
SELECT AddGeometryColumn ('table_name', 'column_name', 4326, 'POINT', 2)
```

3. Coordinate extraction from column using *REGEX*(Necessary before data type change sql)
```sql
update schema_name.table_name set longitude = substring (longitude FROM '\-?\d+\.?\d*');
```

4. Change the data type
```sql
ALTER TABLE schema_name.table_name ALTER COLUMN latitude TYPE DOUBLE PRECISION USING latitude::double precision;
```

5. Get geometry coordinates from other two colums(longitude,latitude), this should be done after data type change.
```sql
UPDATE maintable SET loc= ST_SetSRID(ST_MakePoint(longitude, latitude), 4326);
```

6. Add auto incremental primary key column
```sql
ALTER TABLE your_table ADD COLUMN key_column BIGSERIAL PRIMARY KEY;
```
7. Sample query of points in a polygon area, make sure the start point is the same withe the end one of that polygon, here ST_Contains can also be ST_Intersect for solving intersection, this is used in the flask backend query.
```sql
SELECT *, ST_AsText(loc) FROM maintable WHERE ST_Contains(ST_GEOMFROMTEXT('SRID=4326;POLYGON((30 45,45 45,45 50,30 50,30 45))'),maintable.loc);
```

8. Dump schema and restore, need to grant access(both postgis and public, you need access to postgis and your own schema) to username.
Make sure you are connected to the database through *psql*
```sql
GRANT CONNECT ON DATABASE gisdb TO username;
GRANT USAGE ON SCHEMA public TO ;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO username;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO username;
```
Then:
```bash
$ pg_dump -U username -h host -p port -d database_name  -Fc -n schema > dumpfile_name.dump
$ pg_restore -U username -h host -p port -d database_name dumpfile_name.dump
```
9. Get a rectangle(envelop) from 4 coordinates
```sql
UPDATE maintable SET loc= ST_SetSRID(ST_MakeEnvelope(x1,y1,x2,y2), 4326);
```
Web Application
---------------
1. The web visualization is based on Flask and GoogleApi. Install flask at <http://flask.pocoo.org/>, get google api key at <https://developers.google.com/maps/documentation/javascript/tutorial>

2. To run the application, using your username and password for *database.ini*
```bash
export FLASK_APP=map.py
flask run
```
Some tips when importing
------------------------
1. There might be some column without names or with duplicated names, delete them if no data is in this column, or simply give it another name.

2. It is better not to have "," or "/" in column name when using *load_csv_file* function

3. Some data in the column might not be the data type that is convertible(e.g. from string to double), use Regex sql mentioned above to fix this

4. When there is a format problem, use some text editor to save file in UTF-8 format.

5. Do not extend the columns in some *csv* viewers incautiously, this will give you empty columns with empty names.

6. It is better to use PgAdmin as a graphical user interface. Link: <https://www.pgadmin.org/download/>
