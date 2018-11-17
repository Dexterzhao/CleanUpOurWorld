Environmental Data Importing using POSTGIS based on postgreSQL
==============================================================
Install postgre and postgis before you start.

The following link gives specifc instructions.
<https://trac.osgeo.org/postgis/wiki/UsersWikiPostGIS24UbuntuPGSQL10Apt>

Scripts
-------
There are some scripts used to help importing these files, make sure you have installed python and dependent modules to run them.

1. Suppose you have a bunch of *.csv* files

The *FrequencySummary.py* helps you to count the colomn names. Remember to change the **mypath** variable in the code to your own directory to find these *.csv* files. Then
```
$ python FrequencySummary.py
```
Data Format
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
2. To reduce manual works that are simply some sqls with different table names, *SqlGenerator.py* is used to generate sqls for all your *.csv* files. The file generate sqls for using load_csv_file functions to import all the *.csv* files.

Remember to change the **mypath** variable in the code to your own directory to find these *.csv* files.
```
$ python SqlsGenerator.py
```

3. When you have some *.csv* files that only have location names but not the specific coordinates. Use *LocationToCooridnates.py* to extract the geographical locations from location names. In the case of this script, there are two columns that have related information to get coordinates, which are **water body** and **city**. This requires some basic understandings of python programming.

The location description is limited to 10 words in order to avoid errors, this has not been tested for possible extension. This will generate another *.csv* file.

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
7. Sample query of points in a polygon area, make sure the start point is the same withe the end one of that polygon
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

Some tips when importing
------------------------
1. There might be some column without names or with duplicated names, delete them if no data is in this column, or simply give it another name.

2. It is better not to have "," or "/" in column name when using *load_csv_file* function

3. Some data in the column might not be the data type that is convertible(e.g. from string to double), use Regex sql mentioned above to fix this

4. When there is a format problem, use some text editor to save file in UTF-8 format.

5. Do not extend the columns in some *csv* viewers incautiously, this will give you empty columns with empty names.

6. It is better to use PgAdmin as a graphical user interface. Link: <https://www.pgadmin.org/download/>
