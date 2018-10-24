from os import listdir
from os.path import isfile, join
import pandas as pd



mypath='/home/dexter/Documents/PostGisData'
onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
fredict = {}
frecnt = {}

onlyfiles.sort()

for filename in onlyfiles:
    df = pd.read_csv('/home/dexter/Documents/PostGisData/'+filename, error_bad_lines=False)
    cn = df.columns
    n = cn.tolist()
    for x in n:
        y=x.lower()
        if y in fredict.keys():
            fredict[y].append(filename)
            frecnt[y] += 1
        else:
            fredict[y] = []
            fredict[y].append(filename)
            frecnt[y] = 1
fw = open("fre.txt",'w')
for x,y in sorted(frecnt.items(),key = lambda item:item[1],reverse = True):
    print(x,y)
    fw.writelines(x+':'+str(y)+"\nfilenames:\t"+"\t".join(fredict[x]))
    fw.writelines('\n\n')
fw.close()
