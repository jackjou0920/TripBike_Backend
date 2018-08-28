# -*- coddata = f.read()ing: utf-8 -*- 

import urllib
import gzip
import json
import MySQLdb
import time
import hashlib

import math
url = "http://data.taipei/youbike"

urllib.urlretrieve(url, "data.gz")

print("url complete")
f = gzip.open('data.gz', 'r')
jdata = f.read()
f.close()
data = json.loads(jdata)

print("jason complete")
db = MySQLdb.connect(host="10.60.0.13", user="aibike", passwd="aibike-ekibia", db="aibike")

print("db complete")
cursor = db.cursor()
db.set_character_set('utf8')


isEmpty = cursor.execute("SELECT * FROM bike_info")
db.commit()
print("select complete")
md5_old = "NULL"
if isEmpty != 0 :
    cursor.execute("SELECT stations_update_md5 FROM bike_info ORDER BY id DESC LIMIT 1")
    info = cursor.fetchone()
    db.commit()
    md5_old = info[0]
print md5_old

src = ""
md5_new = ""
for key,value in data["retVal"].iteritems():
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

    src += (sno + sna + snaen + sarea + sareaen + ar + aren + tot + str(lng) + str(lat)).encode("utf-8")
    md5 = hashlib.md5()
    md5.update(src)   
    md5_new = md5.hexdigest()

    sql_info = "INSERT INTO bike_info(station_id,insert_time,update_time,available_vehicles,empty_amount,\
                                    activate_status,stations_update_md5) VALUES(%s,%s,%s,%s,%s,%s,%s)"
    try:
        cursor.execute(sql_info,(sno,now,mday,sbi,bemp,act,md5_new))
        db.commit()
    except MySQLdb.Error as e:
        print e
    

print md5_new
if md5_new != md5_old :
    cursor.execute("TRUNCATE TABLE bike_stations")
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

    for key,value in data["retVal"].iteritems():
        sno = value["sno"]
        sna = value["sna"]
        tot = value["tot"]
        sarea = value["sarea"]
        lat = value["lat"]
        lng = value["lng"]
        ar = value["ar"]
        sareaen = value["sareaen"]
        snaen = value["snaen"]
        aren = value["aren"]
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

        sql_stations = "INSERT INTO bike_stations(station_id,weather_station_id,temperature_station_id,insert_time,station_name_chinese,\
                        station_name_english,station_area_chinese,station_area_english,address_chinese,\
                        address_english,total_parking_space,longitude,latitude) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        try:
            cursor.execute(sql_stations,(sno,sta,t_sta,now,sna,snaen,sarea,sareaen,ar,aren,tot,lng,lat))
            db.commit()
        except MySQLdb.Error as e:
            print e
	
    print "bike_stations update"
	
    cursor.execute("INSERT INTO bike_stations SELECT * FROM bike_stations_newtaipei")
    db.commit()

    print "New Taipei copy OK!"

cursor.close()
db.close()
print(time.time())
