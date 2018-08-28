#!/user/bin/env python
# -*- coding: utf-8 -*-
import xml.etree.ElementTree as ET
import requests
import json
import urllib
import urllib2
import time
import MySQLdb
url_rain = 'http://opendata.cwb.gov.tw/opendataapi?dataid=O-A0002-001&authorizationkey=CWB-B71E8AAB-588C-455F-BFBB-5A1356FC3004'
url_weather = 'http://opendata.cwb.gov.tw/opendataapi?dataid=O-A0003-001&authorizationkey=CWB-B71E8AAB-588C-455F-BFBB-5A1356FC3004'
r_rain = requests.get(url_rain)
r_weather = requests.get(url_weather)
rain = 'rain.xml'
weather = 'weather.xml'
with open(weather, "wb") as data:
	data.write(r_weather.content)
	data.close()
with open(rain, "wb") as data:
	data.write(r_rain.content)
	data.close()

ns = {'cwb': 'urn:cwb:gov:tw:cwbcommon:0.1'}

tree_rain = ET.parse("rain.xml")
root_rain = tree_rain.getroot()
tree_weather = ET.parse("weather.xml")
root_weather = tree_weather.getroot()


conn = MySQLdb.connect(host="10.60.0.13", user="aibike", db="aibike", passwd="aibike-ekibia")
c = conn.cursor()
conn.set_character_set('utf8')

sql = "INSERT INTO weather(station_id,location_name,update_time,insert_time,latitude,longitude,town,town_id,temperature,relative_humidity,mins10_max_average_wind_speed,mins10_rain) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

sql2 = "INSERT INTO weather(station_id,location_name,update_time,insert_time,latitude,longitude,town,town_id,mins10_rain) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
for locations_rain in root_rain.findall('cwb:location', ns):
	flag = False
	for data in locations_rain.findall('cwb:parameter', ns):
		# print data.tag, data.attrib, data.text
		if data.find('cwb:parameterName', ns).text == 'CITY_SN':
			if data.find('cwb:parameterValue', ns).text == '01':
				#print locations_rain.find('cwb:stationId', ns).text
				station_id = locations_rain.find('cwb:stationId', ns).text
				#print locations_rain.find('cwb:locationName', ns).text
				location_name = locations_rain.find('cwb:locationName', ns).text
				#print locations_rain.find('cwb:time/cwb:obsTime', ns).text
				update_time = locations_rain.find('cwb:time/cwb:obsTime', ns).text
				#print locations_rain.find('cwb:lat', ns).text, locations_rain.find('cwb:lon', ns).text
				latitude = locations_rain.find('cwb:lat', ns).text
				longitude = locations_rain.find('cwb:lon', ns).text
                
				for rains in root_rain.findall('cwb:location', ns):
					if station_id == rains.find('cwb:stationId', ns).text:
 						for rain in rains.findall('cwb:weatherElement', ns):
							if rain.find('cwb:elementName', ns).text == 'MIN_10':
								#print rain.find('cwb:elementName', ns).text
								#print rain.find('cwb:elementValue/cwb:value', ns).text
								mins10_rain = rain.find('cwb:elementValue/cwb:value', ns).text
					for parameter in rains.findall('cwb:parameter', ns):
						if station_id == rains.find('cwb:stationId', ns).text:
							if parameter.find('cwb:parameterName', ns).text == 'TOWN':
								#print parameter.find('cwb:parameterName', ns).text
								#print parameter.find('cwb:parameterValue', ns).text
								town = parameter.find('cwb:parameterValue', ns).text
							if parameter.find('cwb:parameterName', ns).text == 'TOWN_SN':
								#print parameter.find('cwb:parameterName', ns).text
								#print parameter.find('cwb:parameterValue', ns).text
								town_id = parameter.find('cwb:parameterValue', ns).text
				for weathers in root_weather.findall('cwb:location', ns):
					if station_id == weathers.find('cwb:stationId', ns).text:
						for weather in weathers.findall('cwb:weatherElement', ns):
							if weather.find('cwb:elementName', ns).text == 'TEMP':
								#print weather.find('cwb:elementValue/cwb:value', ns).text
								temperature = weather.find('cwb:elementValue/cwb:value', ns).text
							if weather.find('cwb:elementName', ns).text == 'HUMD':
								#print weather.find('cwb:elementValue/cwb:value', ns).text
								relative_humidity = weather.find('cwb:elementValue/cwb:value', ns).text
							if weather.find('cwb:elementName', ns).text == 'H_F10':
								#print weather.find('cwb:elementValue/cwb:value', ns).text
								mins10_max_average_wind_speed = weather.find('cwb:elementValue/cwb:value', ns).text
							insert_time = time.time()

						try:
        					# 執行sql語句
							c.execute(sql,(station_id,location_name,update_time,insert_time,latitude,longitude,town,town_id,temperature,relative_humidity,mins10_max_average_wind_speed,mins10_rain))
        					# save the changes
							flag = True
						except MySQLdb.Error,e:	
							print "Mysql Error %d: %s" % (e.args[0], e.args[1])				
				insert_time = time.time()
				if flag == False:
					try:
        				# 執行sql語句
						c.execute(sql2,(station_id,location_name,update_time,insert_time,latitude,longitude,town,town_id,mins10_rain))
# save the changes
						conn.commit()
					except MySQLdb.Error,e:	
						print "Mysql Error %d: %s" % (e.args[0], e.args[1])				
conn.close()
