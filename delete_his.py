# -*- coddata = f.read()ing: utf-8 -*-
import MySQLdb
db = MySQLdb.connect(host="163.21.245.131", user="aibike-hist", passwd="aibike-ekibia", db="aibike-hist")
cursor = db.cursor()

db.set_character_set('utf8')

dataDelete = "DELETE FROM bike_history WHERE hist_insert_time = 1499759478.03"
try:
    cursor.execute(dataDelete)
    db.commit()
except MySQLdb.Error as e:
    print e
