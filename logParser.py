#!/usr/bin/env python3

import csv
import datetime
import glob
import gSheets
import os
import pandas as pd
import psycopg2 as psql
import struct

LAT_LONG_CONVERT_FACTOR = 1e6


class car:
    def __init__(self, VIN):
        self.VIN = VIN
        self.trips = []

    def getLastOdometerFromDB(self, dbConn):
        curr = dbConn.cursor()
        curr.execute(
            """
                    SELECT EndOdometer, OdometerUnitID 
                    from Trip where VIN=%s  
                    order by EndOdometer desc
                    """,
            (self.VIN,),
        )
        if curr.rowcount > 0:
            results = curr.fetchone()
            self.lastOdometer = results[0]
            self.OdometerUnit = results[1]
        else:
            self.lastOdometer = 0
            self.OdometerUnit = "mi"
        curr.close()

    def getTripsFromGoogleDrive(self, dbConn):
        self.getLastOdometerFromDB(dbConn)
        creds = gSheets.getAuthCreds()
        tripsFrame = gSheets.getTrips(creds, self.lastOdometer)
        if tripsFrame:
            for index, row in tripsFrame.iterrows():
                self.trips.append(trip.makeTripFromSheets(row, dbConn))

            for trip1 in self.trips:
                if trip1:
                    trip1.writeToDB(dbConn, self.OdometerUnit, self.VIN)
            print("finished adding all trips")
        else:
            print("No new trips")


class trip:
    def __init__(self):
        self.tripLegs = []


class tripLog:
    def __init__(self, dataFrames):
        self.frames = dataFrames


class dataLog:
    def __init__(self, fileName):
        self.frames = []
        self.fileName = fileName

    def findDates(self):
        """
        These times seem to be around +/- 3 seconds off
        """
        startTime = None
        startDate = None
        offSet = None
        count = 0
        for frame in self.frames:
            if frame.getGPS_Time() is not None and startTime is None:
                startTime = frame.getGPS_Time()
                offSet = frame.time
            if frame.getGPS_Date() is not None and startDate is None:
                startDate = frame.getGPS_Date()
        # get end time
        if startTime and startDate:
            self.start = datetime.datetime.combine(startDate, startTime) - offSet
        else:
            name = os.path.basename(os.path.splitext(self.fileName)[0])
            self.start = datetime.datetime.strptime(name,"%m%d%H%M")
        lastTime = self.frames[-1].time
        self.last = self.start + lastTime


    def testClockDrift(self):
        self.findDates()
        thresholdUpper = datetime.timedelta(seconds=0.5)
        thresholdLower = datetime.timedelta(seconds=-0.5)
        offset = datetime.timedelta(minutes=1)
        for frame in self.frames:
            delta = frame.getTimeDrift(self.start)
            if delta and (delta > thresholdUpper or delta < thresholdLower):
                print("bad: {}".format(delta + offset))

    def parseCSV(self):
        int_to_four_bytes = struct.Struct("<I").pack
        lastTime = -1
        dataFrames = []
        with open(self.fileName, "r") as fh:
            spamreader = csv.reader(fh)
            for row in spamreader:
                timeOffset = float(row[0]) / 1000.0
                if abs(timeOffset - lastTime) > 1e-7:
                    # print(abs(timeOffset-lastTime))
                    try:
                        dataFrames.append(currentFrame)
                        lastTime = timeOffset
                    except NameError:
                        pass
                    currentFrame = dataFrame(timeOffset)
                PIDtemp = int(row[1], 16)
                y1, y2, y3, y4 = int_to_four_bytes(PIDtemp & 0xFFFF)
                PID = y1
                service = y2
                data = []
                if ";" in row[2]:
                    cells = row[2].split(";")
                else:
                    cells = row[2:]
                for cell in cells:
                    if cell != "":
                        data.append(float(cell))
                # print(row)
                currentFrame.addDataPoint(dataPoint(service, PID, data))
        self.frames = dataFrames


class tripLeg:
    def __init__(self):
        pass


class dataFrame:
    def __init__(self, timeOffset):
        self.time = datetime.timedelta(seconds=timeOffset)
        self.data = {}

    def addDataPoint(self, dataPoint):
        self.data[dataPoint.getID()] = dataPoint

    def __str__(self):
        return "{}:\n{}\n".format(self.time, self.data)

    def __repr__(self):
        return self.__str__()

    def getGPS_Date(self):
        try:
            return self.data[(0, 0x11)].dataList[0]
        except KeyError:
            return None

    def getGPS_Time(self):
        try:
            return self.data[(0, 0x10)].dataList[0]
        except KeyError:
            return None

    def getTimeDrift(self, start):
        gpsTime = self.getGPS_Time()
        if gpsTime:
            gpsTime = datetime.datetime.combine(start.date(), gpsTime)
            clockTime = start + self.time
            return gpsTime - clockTime


class dataPoint:
    def __init__(self, service, PID, data):
        self.service = service
        self.PID = PID
        self.dataList = data
        self.cleanUpData()

    def getID(self):
        return (self.service, self.PID)

    def __str__(self):
        return "({},{}): {}".format(self.service, self.PID, self.dataList)

    def __repr__(self):
        return self.__str__()

    def cleanUpData(self):
        if self.service == 0:  # Freematics breaks everything
            if self.PID == 0x10:  # convert to a time object
                buffStr = str(int(self.dataList[0]))
                if len(buffStr) < 8:  # if leading 0s are stripped pad them back
                    # because you know no-one apparently is competent enough to log things
                    padding = 8 - len(buffStr)
                    buffStr = "0" * padding + buffStr
                hours = int(buffStr[0:2])
                minutes = int(buffStr[2:4])
                seconds = int(buffStr[4:6])
                microsecs = int(buffStr[6:])/100
                time = datetime.datetime.strptime(buffStr[0:6],"%H%M%S")
                time = (time + datetime.timedelta(seconds=microsecs)).time()
                self.dataList[0] = time
            elif self.PID == 0x11:  # convert to date object
                buffStr = str(int(self.dataList[0]))
                if len(buffStr) < 6:  # if leading 0s are stripped pad them back
                    # because you know no-one apparently is competent enough to log things
                    padding = 6 - len(buffStr)
                    buffStr = "0" * padding + buffStr
                date = datetime.datetime.strptime(buffStr,"%d%m%y").date()
                self.dataList[0] = date
            elif self.PID in [
                0xA,
                0xB,
            ]:  # convert the lat and long to decimals of degrees
                self.dataList[0] = self.dataList[0] / LAT_LONG_CONVERT_FACTOR


def testClockDrift():
    for file in glob.glob("log_data/*.CSV"):
        print(file)
        test = dataLog(file)
        test.parseCSV()
        test.testClockDrift()


testClockDrift()
test.parseCSV("05121855.CSV")
