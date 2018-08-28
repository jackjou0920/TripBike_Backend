# -*- coddata = f.read()ing: utf-8 -*-

import MySQLdb
import time

db = MySQLdb.connect(host="10.60.0.13", user="aibike", passwd="aibike-ekibia", db="aibike")
db_hist = MySQLdb.connect(host="163.21.245.131", user="aibike-hist", passwd="aibike-ekibia", db="aibike-hist")
cursor = db.cursor()
cursor_hist = db_hist.cursor()

db.set_character_set('utf8')
db_hist.set_character_set('utf8')

now = time.time()
print now


sql_ins = "INSERT INTO bike_history(info_id,station_id,info_insert_time,info_update_time,available_vehicles,empty_amount,\
            activate_status,total_parking_space,hist_insert_time) VALUE(%s,%s,%s,%s,%s,%s,%s,%s,%s)" 

sql_sel = "SELECT bike_info.id,bike_info.station_id,bike_info.insert_time,update_time,\
        available_vehicles,empty_amount,activate_status,total_parking_space FROM bike_info,bike_stations \
        WHERE bike_info.station_id = bike_stations.station_id AND bike_info.insert_time <" + str(now)

try:
    cursor.execute(sql_sel)
    db.commit()
except MySQLdb.Error as e:
    print e

tem = cursor.fetchall()

for i in tem:
    try:
        cursor_hist.execute(sql_ins,(str(i[0]),str(i[1]),str(i[2]),str(i[3]),str(i[4]),str(i[5]),str(i[6]),str(i[7]),str(now)))
        db_hist.commit()
    except MySQLdb.Error as e:
        print e

dataDelete = "DELETE FROM bike_info WHERE insert_time <" + str(now)
try:
    cursor.execute(dataDelete)
    db.commit()
except MySQLdb.Error as e:
    print e


cursor.close()
cursor_hist.close()
db.close()
db_hist.close()
