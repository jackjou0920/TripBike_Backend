# -*- coddata = f.read()ing: utf-8 -*-

import MySQLdb

db = MySQLdb.connect(host="10.60.0.13", user="aibike", passwd="aibike-ekibia", db="aibike")
cursor = db.cursor()
db.set_character_set('utf8')

with open("weatherForUbike.txt","r") as f:
    for line in f:
        station_id = line.rstrip('\n').split()[1]
        data = line.rstrip('\n').split()[2]  

        try:
            cursor.execute("UPDATE bike_stations SET weather_station_id=%s WHERE station_id=%s",([data],station_id))
            db.commit()
        except MySQLdb.Error as e:
            print e

cursor.close()
db.close()
