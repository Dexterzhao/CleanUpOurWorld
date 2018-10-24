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

    #importing sql
    print('select load_csv_file(\''+filename+'\',\'/home/dexter/Documents/PostGisData/'+filename+'\''+','+str(len(n))+');'+'\n')

    
