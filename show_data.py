# -*- coddata = f.read()ing: utf-8 -*- 

import os
import gzip
import json
import time

count = 0
for filename in os.listdir("./bike-04-data/2017-04-09/") :
    count += 1
    f = gzip.open(os.path.join("bike-04-data/2017-04-09/",filename), 'r')
    jdata = f.read()
    f.close()
    data = json.loads(jdata)
    
    print str(count) + filename

    number = 0
    for key,value in data["retVal"].iteritems() :
        number += 1
        sno = value["sno"]
        sna = value["sna"]
        tot = value["tot"]
        sbi = value["sbi"]
        sarea = value["sarea"]
        mday = value["mday"]
        lat = value["lat"]
        lng = value["lng"]
        ar = value["ar"]
        bemp = value["bemp"]
        act = value["act"]
        sareaen = value["sareaen"]
        snaen = value["snaen"]
        aren = value["aren"]
        now = time.time()

        print str(number) + " " + sno + " " + sna


