# -*- coddata = f.read()ing: utf-8 -*-


import MySQLdb
import math



db = MySQLdb.connect(host="10.60.0.13", user="aibike", passwd="aibike-ekibia", db="aibike")

cursor = db.cursor()
db.set_character_set('utf8')

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

try :
    f = open('weatherForUbike.txt','w')
except IOError as e:
    print e

sql_info = "SELECT * from  bike_stations"
try:
    cursor.execute(sql_info)
    db.commit()
    result = cursor.fetchall()
    #print(result)

    i = 1
    for r in result :
        #print str(i) + " : " + "station : "+ str(r[1]) + " longitude = " + str(r[10]) + ", latitude = " + str(r[11])
        print str(i) + " " + str(r[1]) + " ",
        f.write(str(i) + " " + str(r[1]) + " ")
        sql_weather = "SELECT * from  weather_station"
        try:
            cursor.execute(sql_weather)
            db.commit()
            result_w = cursor.fetchall()

            min = 100000
            t_min = 10000000
            sta = ""
            r_sta = ""
            temperatureStation = [(466930,25.1639,121.536), (466910,25.1844,121.521),(466920,25.0394,121.507)]
            for row in result_w :
                #print "weather station" + row[1] +  " : longitude = " + str(row[4]) + ", latitude = " + str(row[3])

                distance =  countDistance(math.radians(r[12]),math.radians(r[11]), math.radians(row[3]), math.radians(row[4]))
                #print str(distance) + "m, " + str(distance/1000) + "Km."
                if distance/1000 < min :
                    sta = str(row[1])
                    min = distance/1000
            for t_row in temperatureStation :
                t_dis = countDistance(math.radians(r[12]), math.radians(r[11]),math.radians(t_row[1]), math.radians(t_row[2]))

                if t_dis/1000 < t_min :
                    t_sta = str(t_row[0])
                    t_min = t_dis/1000
            #print "The closest station : "+sta+", Distance : "+str(min) + "KM"
            print sta + " " + t_sta
            f.write(sta + " " + t_sta + "\n")
        except MySQLdb.Error as e:
            print e
        i += 1

except MySQLdb.Error as e:
    print e


cursor.close()
db.close()
