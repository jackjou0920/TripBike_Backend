# -*- coding: utf-8 -*-
import urllib
import json
import MySQLdb
import time
import requests
from datetime import datetime

url = "http://data.ntpc.gov.tw/api/v1/rest/datastore/382000000A-000077-002"
r = requests.get(url)
data = json.loads(r.text)
# print data

db = MySQLdb.connect(host="10.60.0.13", user="aibike", passwd="aibike-ekibia", db="aibike")

cursor = db.cursor()
db.set_character_set('utf8')
sql = "INSERT INTO holiday(date, holiday_name, isholiday, why_holiday, description) VALUES (%s,%s,%s,%s,%s)"

records = data["result"]["records"]
for i in range(len(records)):
    t = records[i]["date"]
    int_time = int(datetime.strptime(t, '%Y/%m/%d').strftime('%Y%m%d'))

    if (int_time > 20170408) and (records[i]["isHoliday"] != u"否"):
        print records[i]["date"]
        date = records[i]["date"]
        print records[i]["name"]
        holiday_name = records[i]["name"]
        print records[i]["isHoliday"]
        isholiday = records[i]["isHoliday"]
        print records[i]["holidayCategory"]
        why_holiday = records[i]["holidayCategory"]
        print records[i]["description"]
        description = records[i]["description"]

        try:
            cursor.execute(sql, (date, holiday_name, isholiday, why_holiday, description))
            db.commit()
        except MySQLdb.Error as e:
            print e
