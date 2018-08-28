
# -*- coddata = f.read()ing: utf-8 -*-

import MySQLdb
import time

db = MySQLdb.connect(host="10.60.0.13", user="aibike", passwd="aibike-ekibia", db="aibike")
cursor = db.cursor()
db.set_character_set('utf8')

now = time.time()
print now

sql = "INSERT INTO weather_history(rain_id,station_id,location_name,latitude,longitude,mins10_rain,town,town_id,\
		rain_update_time,rain_insert_time,temperature,relative_humidity,mins10_max_average_wind_speed,hist_insert_time) \
        SELECT id,station_id,location_name,latitude,longitude,mins10_rain,town,town_id,\
		update_time,insert_time,temperature,relative_humidity,mins10_max_average_wind_speed," + str(now) + "\
        FROM weather\
		WHERE weather.insert_time <" + str(now)

try:
    cursor.execute(sql)
    db.commit()
except MySQLdb.Error as e:
    print e



dataDelete = "DELETE FROM weather WHERE insert_time <" + str(now)

try:
    cursor.execute(dataDelete)
    db.commit()
except MySQLdb.Error as e:
    print e







cursor.close()
db.close()
