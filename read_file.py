# -*- coding: UTF-8 -*-
import os
import urllib
import gzip
import json
import MySQLdb
import time

DATA_DIR = "./Taipei/0509/"

db = MySQLdb.connect(host="10.60.0.13", user="aibike", db="aibike", passwd="aibike-ekibia")
for filename in os.listdir(DATA_DIR):
    loadFile = open(os.path.join(DATA_DIR, filename), 'rb')
    jdata = loadFile.read()
    loadFile.close()
    data = json.loads(jdata)
    
    cursor = db.cursor()
    print filename
	for key,value in data["retVal"].iteritems():
		sno = value["sno"]
        mday = value["mday"]
		tot = value["tot"]
		sbi = value["sbi"]
		bemp = value["bemp"]
		act = value["act"]
		now = time.time()

		sql = "INSERT INTO bike_history(station_id,info_update_time,empty_amount,activate_status,\
                                                available_vehicles,total_parking_space,hist_insert_time) VALUES(%s,%s,%s,%s,%s,%s,%s)"
		try:
			cursor.execute(sql,(sno,mday,bemp,act,sbi,tot,now))
			db.commit()
		except MySQLdb.Error,e:
			print e

                #print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
db.close()
