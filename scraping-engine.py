from datetime import datetime
import schedule
import time
import requests
import utm
import csv
from bs4 import BeautifulSoup
import json


a_file = open("scrape-data.json", "r")
json_object = json.load(a_file)
a_file.close()
dataNum = json_object["DATA_NUM"]
lastTimestamp = json_object["LAST_TIMESTAMP"]
print("dataNum: %d \t\tlastTimestamp: %s "%(dataNum,lastTimestamp))


def updateScrapeData(newNum):
  json_object["DATA_NUM"] = newNum
  a_file = open("scrape-data.json", "w")
  json.dump(json_object, a_file)
  a_file.close()


def updateScrapeTime(newTime):
  json_object["LAST_TIMESTAMP"] = newTime
  a_file = open("scrape-data.json", "w")
  json.dump(json_object, a_file)
  a_file.close()
  print("Update time")



def compare_dates(date1, date2):
 
    dt_obj1 = date1
    dt_obj2 = date2
    
    if dt_obj1 == dt_obj2:
        return dt_obj1
    elif dt_obj1 > dt_obj2:
        return dt_obj1
    else:
        return dt_obj2

def scrapeData():
  global dataNum ,lastTimestamp

  url = 'https://earthquake.tmd.go.th/inside.html?ps=100000'
  res = requests.get(url)
  res.encoding = "utf-8"
  # print(res)
  if res.status_code == 200:
      print("Successful")
  elif res.status_code == 404:
      print("Error 404 page not found")
  else:
      print("Not both 200 and 404")
  soup = BeautifulSoup(res.text, 'html.parser')
  
  table = soup.find_all('table')
  dataTable = table[1]
  dataTable = dataTable.find_all('tr')
  if(dataNum<len(dataTable)):
     print("Last Timestamp:",lastTimestamp)
     extractData(dataTable)


def extractData(dataTable):
  global dataNum ,lastTimestamp
  tempArr =[None]*10
  header = ['Date','Magnitute','Latitute','Longitute','UTM-x-coordinate',
            'UTM-y-coordinate','Depth','Phase','Center-Th','Center-En','Severity-level']
  with open('earth-quake-dataset.csv', 'w',newline='', encoding='utf8') as f:
      write = csv.writer(f)
      write.writerow(header)
      for i in range(len(dataTable)-1):
          tempArr.clear()
          dataTd = dataTable[i+1].find_all('td')

          dataDateTime = datetime.strptime((str(dataTd[0].p.string)[:-4]), "%Y-%m-%d %H:%M:%S")
          if dataNum != 0:
            lastTimestampDateTime = datetime.strptime(lastTimestamp, "%Y-%m-%d %H:%M:%S")    #Edit
            if dataDateTime <= lastTimestampDateTime:
              break
          else:
            # lastTimestamp = compare_dates(lastTimestamp,(str(dataTd[0].p.string)[:-4]))
            if lastTimestamp == "":
              lastTimestamp=dataDateTime
            else:
              lastTimestamp = compare_dates(lastTimestamp,dataDateTime)
            updateScrapeTime(lastTimestamp.strftime("%Y-%m-%d %H:%M:%S"))

            tempArr.append((str(dataTd[0].p.string)[:-4]))                                #date UTC
            tempArr.append(float(dataTd[1].string))                                       #Magnitute
            tempArr.append(float((dataTd[2].string)[0:-2]))                               #Lat
            tempArr.append(float((dataTd[3].string)[0:-2]))                               #Long
            #convert lat long to UTM format
            coordinate=utm.from_latlon(float((str(dataTd[2].string))[0:-2]),
                                      float((str(dataTd[3].string))[0:-2]))
            tempArr.append(int(coordinate[0]))                                            #UTM-X-coordinate
            tempArr.append(int(coordinate[1]))                                            #UTM-Y-coordinate
            tempArr.append(str(dataTd[4].string))                                         #Depth
            tempArr.append(str(dataTd[5].string))                                         #Phase
            pos=((str(dataTd[6].select('span')).replace('[<span class="style10">','')).
                replace('</span>]','')).split('<br/>')
            tempArr.append(str(pos[0]))                                                   #Center-th
            tempArr.append(str(pos[1]))                                                   #Center-en
            # print(float(dataTd[1].string))
            tempMag = float(dataTd[1].string)
            if(tempMag>=7):
              tempArr.append(str('6'))
            elif(tempMag<=6.9 and tempMag >= 6):
              tempArr.append(str('5'))
            elif(tempMag<=5.9 and tempMag >= 5):
              tempArr.append(str('4'))
            elif(tempMag<=4.9 and tempMag >= 4):
              tempArr.append(str('3'))
            elif(tempMag<=3.9 and tempMag >= 3):
              tempArr.append(str('2'))
            elif(tempMag<=2.9):
              tempArr.append(str('1'))
            write.writerow(tempArr)
  dataNum = len(dataTable)
  updateScrapeData(len(dataTable))
  print("dataNum: %d \t\tlastTimestamp: %s "%(dataNum,lastTimestamp))

  
scrapeData()
schedule.every(10).minutes.do(scrapeData)
while True:
   schedule.run_pending()
   time.sleep(1)
   