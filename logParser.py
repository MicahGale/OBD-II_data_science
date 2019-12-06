#!/usr/bin/env python3

import csv
import pandas as pd
import psycopg2 as psql
import struct


class trip:
    pass

class tripLog:
    pass

class dataLog:
    def __init__(self):
        pass

    def parseCSV(self,fileName):
        int_to_four_bytes = struct.Struct('<I').pack
        lastTime = -1
        dataFrames = []
        with open(fileName, 'r') as fh:
            spamreader = csv.reader(fh)
            for row in spamreader:
                timeOffset = float(row[0])/1000.0
                if abs(timeOffset-lastTime) > 1e-7:
                    #print(abs(timeOffset-lastTime))
                    try:
                        dataFrames.append(currentFrame)
                        lastTime = timeOffset
                    except NameError:
                        pass
                    currentFrame = dataFrame(timeOffset)
                PIDtemp = int(row[1],16)
                y1,y2,y3,y4 = int_to_four_bytes(PIDtemp & 0xFFFF)
                PID = y1 
                service = y2
                data = []
                for cell in row[2:]:
                    if cell != "":
                        data.append(float(cell))
                currentFrame.addDataPoint(dataPoint(service,PID,data))
        print(dataFrames)
class dataFrame:
    def __init__(self, timeOffset):
        self.time = timeOffset
        self.data = {}

    def addDataPoint(self,dataPoint):
        self.data[dataPoint.getID()]=dataPoint

    def __str__(self):
        return "{}:\n{}\n".format(self.time, self.data)

    def __repr__(self):
        return self.__str__()

class dataPoint: 
    def __init__(self,service, PID, data):
        self.service = service
        self.PID = PID
        self.dataList = data

    def getID(self):
        return (self.service, self.PID)
    def __str__(self):
        return "({},{}): {}".format(self.service, self.PID, self.dataList)

    def __repr__(self):
        return self.__str__()

    def cleanUpData(self):
        if self.service == 0: #Freematics breaks everything
            if self.PID == 0x20:
                pass


test = dataLog()
test.parseCSV("05121855.CSV")
