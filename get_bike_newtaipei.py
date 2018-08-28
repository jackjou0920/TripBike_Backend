# -*- coddata = f.read()ing: utf-8 -*- 

import urllib
import json
import requests
import MySQLdb
import time
import hashlib

import math
url = "http://data.ntpc.gov.tw/od/data/api/54DDDC93-589C-4858-9C95-18B2046CC1FC?$format=json"
bike_newtaipei = requests.get(url)

with open('bike_newtaipei.json', "wb") as data:
        data.write(bike_newtaipei.content)
        data.close()

print("url complete")
f = open('bike_newtaipei.json', 'r')
jdata = f.read()
f.close()

data = json.loads(jdata)
print("jason complete")

db = MySQLdb.connect(host="10.60.0.13", user="aibike", passwd="aibike-ekibia", db="aibike")
print("db complete")
cursor = db.cursor()
db.set_character_set('utf8')

isEmpty = cursor.execute("SELECT * FROM bike_info_newtaipei")
db.commit()
print("select complete")

md5_old = "NULL"
if isEmpty != 0 :
    cursor.execute("SELECT stations_update_md5 FROM bike_info_newtaipei ORDER BY id DESC LIMIT 1")
    info = cursor.fetchone()
    db.commit()
    md5_old = info[0]
print md5_old

src = ""
md5_new = ""

for arr in data:
	mday = arr["mday"]
    	sno = arr["sno"]
    	sna = arr["sna"]
    	tot = arr["tot"]
    	sbi = arr["sbi"]
    	sarea = arr["sarea"]
    	lat = arr["lat"]
    	lng = arr["lng"]
    	ar = arr["ar"]
    	bemp = arr["bemp"]
    	act = arr["act"]
   	sareaen = arr["sareaen"]
    	snaen = arr["snaen"] 
    	aren = arr["aren"]
    	now = time.time()   
   	src += (sno + sna + snaen + sarea + sareaen + ar + aren + tot + str(lng) + str(lat)).encode("utf-8")
    	md5 = hashlib.md5()
    	md5.update(src)   
    	md5_new = md5.hexdigest()

    	sql_info = "INSERT INTO bike_info_newtaipei(station_id,insert_time,update_time,available_vehicles,empty_amount,\
                                    activate_status,stations_update_md5) VALUES(%s,%s,%s,%s,%s,%s,%s)"
    	try:
        	cursor.execute(sql_info,(sno,now,mday,sbi,bemp,act,md5_new))
        	db.commit()
    	except MySQLdb.Error as e:
        	print e
    

print md5_new
if md5_new != md5_old :
    cursor.execute("TRUNCATE TABLE bike_stations_newtaipei")
    db.commit()
    
    #latitude1, longitude1, latitude2, longitude2
    def countDistance(x1,y1,x2,y2):
        f = 1/298.25722
        a = 6378137

        fi_1 = math.atan((1 - f)*math.tan(x1))
        fi_2 = math.atan((1 - f)*math.tan(x2))
        p = (math.sin(fi_1) + math.sin(fi_2))**2
        q = (math.sin(fi_1) - math.sin(fi_2))**2
        sigma = math.acos(math.sin(fi_1)*math.sin(fi_2) + math.cos(fi_1)*math.cos(fi_2)*math.cos(y1 - y2))

        bigx = (sigma - math.sin(sigma))/(4*(1 + math.cos(sigma)))
        bigy = (sigma + math.sin(sigma))/(4*(1 - math.cos(sigma)))
        d = a*(sigma - f*(p*bigx + q*bigy))
        return d

    for arr in data:
        sno = arr["sno"]
        sna = arr["sna"]
        tot = arr["tot"]
        sarea = arr["sarea"]
        lat = arr["lat"]
        lng = arr["lng"]
        ar = arr["ar"]
        sareaen = arr["sareaen"]
        snaen = arr["snaen"]
        aren = arr["aren"]
        now = time.time()

        sql_weather = "SELECT * from  weather_station"
        try:
            cursor.execute(sql_weather)
            db.commit()
            result_w = cursor.fetchall()
        except MySQLdb.Error as e:
            print e

        min = 100000
        t_min = 10000000
        sta = ""
        r_sta = ""
        temperatureStation = [(466930,25.1639,121.536), (466910,25.1844,121.521),(466920,25.0394,121.507)]
        
        for row in result_w :
            distance =  countDistance(math.radians(float(lat)),math.radians(float(lng)), math.radians(row[3]), math.radians(row[4]))
            if distance/1000 < min :
                sta = str(row[1])
                min = distance/1000
        
        for t_row in temperatureStation :
            t_dis = countDistance(math.radians(float(lat)), math.radians(float(lng)), math.radians(t_row[1]), math.radians(t_row[2]))
            if t_dis/1000 < t_min :
                t_sta = str(t_row[0])
                t_min = t_dis/1000

        sql_stations = "INSERT INTO bike_stations_newtaipei(id,station_id,weather_station_id,temperature_station_id,insert_time,station_name_chinese,\
                        station_name_english,station_area_chinese,station_area_english,address_chinese,\
                        address_english,total_parking_space,longitude,latitude) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        try:
            cursor.execute(sql_stations,(sno,sno,sta,t_sta,now,sna,snaen,sarea,sareaen,ar,aren,tot,lng,lat))
            db.commit()
        except MySQLdb.Error as e:
            print e
	
    print "bike_stations_newtaipei update"

    last_id = cursor.lastrowid
    
    for x in range(1001,last_id):
	sql_del = "DELETE bike_stations WHERE id=" + repr(x)
	x = x+1

    cursor.execute("INSERT INTO bike_stations SELECT * FROM bike_stations_newtaipei")
    db.commit()

    print "Copy to info OK!"
  
cursor.close()
db.close()
print(time.time())
