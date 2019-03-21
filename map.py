from flask import Flask, render_template, request, jsonify

from configparser import ConfigParser
import psycopg2
import psycopg2.extensions

from osgeo import ogr

app = Flask(__name__, template_folder=".", static_folder = '', static_url_path = '')


@app.route("/")
def home():


    # read connection parameters
    params = config()

    # connect to the PostgreSQL server
    print('Connecting to the PostgreSQL database...')
    conn = psycopg2.connect(**params)
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    # Get top level division
    cur.execute('Select north,south,east,west FROM public.aggregatetable WHERE loclevel = 6')

    lat_list = []
    lon_list = []
    for record in cur:
        lat_list.append((float(record[0]) + float(record[1]))/2)
        if(float(record[2]) > float(record[3])):
            lon_list.append((float(record[2]) + float(record[3]))/2)
        else:
            lon_list.append((float(record[2]) + 360.0 + float(record[3]))/2)
    print(lat_list,lon_list)

    #cursor close
    cur.close()

    return render_template('index.html')
    # return render_template('index.html', lat_list = lat_list, lon_list = lon_list)

@app.route("/zoomchange")
def zoom_change():

    level = request.args.get('zoom',0,type = int)
    east = float(request.args.get('east'))
    south = float(request.args.get('south'))
    west = float(request.args.get('west'))
    north = float(request.args.get('north'))
    rslist = []
    print(type(east))
    print(level)
    print(east,south,west,north)

    geomtxt = drawPolygon([north,south,east,west])

    # Create polygon


    # read connection parameters
    params = config()

    # connect to the PostgreSQL server
    print('Connecting to the PostgreSQL database...')
    conn = psycopg2.connect(**params)
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    # Get top level division
    for txt in geomtxt:
        cur.execute('SELECT * FROM public.maintable WHERE ST_Contains(ST_GEOMFROMTEXT(\'SRID=4326;'+txt+'\'),maintable.loc);')
        rslist = rslist + cur.fetchall()

    for rs in rslist:
        print(rs)
    print(jsonify(rslist))
    #cursor close
    cur.close()

    # str = 'Response'
    return jsonify(rslist)

@app.route("/zoomchangechart")
def zoom_changechart():

    level = request.args.get('zoom',0,type = int)
    east = float(request.args.get('east'))
    south = float(request.args.get('south'))
    west = float(request.args.get('west'))
    north = float(request.args.get('north'))
    rslist = []
    print(level)
    print(east,south,west,north)
    # point list, order N:y2, S:y1, E:x2, W:x1
    geomtxt = drawPolygon([north,south,east,west])

    # read connection parameters
    params = config()

    # connect to the PostgreSQL server
    print('Connecting to the PostgreSQL database...')
    conn = psycopg2.connect(**params)
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    # Get top level division
    for txt in geomtxt:
        cur.execute('SELECT * FROM public.aggregatetable WHERE loclevel ='+str(level)+' and ST_Intersects(ST_GEOMFROMTEXT(\'SRID=4326;'+txt+'\'),aggregatetable.area);')
        rslist = rslist + cur.fetchall()
        # print(cur.mogrify('SELECT * FROM public.aggregatetable WHERE loclevel ='+str(level)+' and ST_Intersects(ST_GEOMFROMTEXT(\'SRID=4326;'+geomtxt+'\'),aggregatetable.area);'))

    # for record in cur:
    #     print(record)
    for rs in rslist:
        print(rs)
    # print(jsonify(rslist))
    #cursor close
    cur.close()

    return jsonify(rslist)


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

def drawPolygon(pointlist):
    # point list, order N:y2, S:y1, E:x2, W:x1
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

if __name__ == "__main__":
    app.run(debug=True)
