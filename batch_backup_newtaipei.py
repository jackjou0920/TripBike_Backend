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


sql_ins = "INSERT INTO bike_history_newtaipei(info_id,station_id,info_insert_time,info_update_time,available_vehicles,empty_amount,\
            activate_status,total_parking_space,hist_insert_time) VALUE(%s,%s,%s,%s,%s,%s,%s,%s,%s)" 

sql_sel = "SELECT bike_info_newtaipei.id,bike_info_newtaipei.station_id,bike_info_newtaipei.insert_time,update_time,\
        available_vehicles,empty_amount,activate_status,total_parking_space FROM bike_info_newtaipei,bike_stations_newtaipei \
        WHERE bike_info_newtaipei.station_id = bike_stations_newtaipei.station_id AND bike_info_newtaipei.insert_time <" + str(now)

try:
    cursor.execute(sql_sel)
    db.commit()
except MySQLdb.Error as e:
    print e

print("select complete")

tem = cursor.fetchall()

count = 0
rows = []
for i in tem:
    count += 1
    
    data = (str(i[0]),str(i[1]),str(i[2]),str(i[3]),str(i[4]),str(i[5]),str(i[6]),str(i[7]),str(now))
    rows.append(data)
    
    print str(count) + " " + str(i[1])
    
    if count == 5000 :
        try:
            cursor_hist.executemany(sql_ins,rows)
            db_hist.commit()
        except MySQLdb.Error as e:
            print e

	print("insert complete")
        count = 0
        rows = []

try:
    cursor_hist.executemany(sql_ins,rows)
    db_hist.commit()
except MySQLdb.Error as e:
    print e

dataDelete = "DELETE FROM bike_info_newtaipei WHERE insert_time <" + str(now)
try:
    cursor.execute(dataDelete)
    db.commit()
    print("delete complete")
except MySQLdb.Error as e:
    print e

cursor.close()
cursor_hist.close()
db.close()
db_hist.close()
print time.time()
