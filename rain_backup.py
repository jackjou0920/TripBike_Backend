# -*- coddata = f.read()ing: utf-8 -*-

import MySQLdb
import time

db = MySQLdb.connect(host="10.60.0.13", user="aibike", passwd="aibike-ekibia", db="aibike")
cursor = db.cursor()
db.set_character_set('utf8')

sql = "SELECT * FROM weather_temperary"
cursor.execute(sql)
rows = cursor.fetchall()
db.commit()
for row in rows:
	print "row[1]: " + row[1] 
	print "row[8]: " + row[8] 
	print "row[9]: " + str(row[9]) 
	print "row[10]: " + str(row[10]) 
	print "row[11]: " + str(row[11]) 
	print "--end--"
	cursor.execute("UPDATE weather_test\
					SET temperature = " + str(row[9]) +\
					"WHERE weather_test.station_id = " + row[1] +\
					"AND weather_test.rain_update_time = " + row[8]) 
cursor.close()
db.close()
