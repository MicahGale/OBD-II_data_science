#!/usr/bin/env python3

# TODO handle configurations, and repairs (tire change, crashes)
import csv
import datetime
import glob
import gSheets
import os
import pandas as pd
import psycopg2 as psql
import re
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
                    SELECT "EndOdometer", "OdometerUnitID" 
                    from "Trip" where "VIN"=%s  
                    order by "EndOdometer" desc
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
    def __init__(
        self,
        startOdometer,
        endOdemeter,
        date,
        start,
        end,
        drivers,
        description,
        categories=None,
        conditions=None,
        tripLegs=None,
    ):
        self.startOdometer = startOdometer
        self.endOdometer = endOdemeter
        self.date = date.date()
        if start:
            self.startTime = start.time()
        else:
            self.startTime = None
        if end:
            self.endTime = end.time()
        else:
            self.endTime = None
        self.description = description
        self.drivers = drivers
        if len(drivers) == 0:
            raise DriverUndefined(None)
        for driver in self.drivers:
            if driver not in self.driversList:
                raise DriverUndefined(driver)

        if categories and categories[0] != "":
            for category in categories:
                if category not in self.categoryList:
                    raise CategoryUndefined(category)
            self.categories = categories
        if conditions and conditions[0] != "":
            for condition in conditions:
                if condition not in self.conditionList:
                    raise ConditionUndefined(condition)
            self.conditions = conditions
        if tripLegs:
            self.tripLegs = tripLegs
        else:
            self.tripLegs = []

    def writeToDB(self, dbConn, odometerUnit, VIN):
        try:
            curr = dbConn.cursor()
            curr.execute(
                """
                INSERT INTO "Trip"(
                    "StartOdometer",
                    "EndOdometer",
                    "OdometerUnitID",
                    "TripDate",
                    "TripStart",
                    "TripEnd",
                    "VIN",
                    "Description")
                values (%s, %s, %s, %s, %s, %s, %s, %s) 
                """,
                (
                    self.startOdometer,
                    self.endOdometer,
                    odometerUnit,
                    self.date,
                    self.startTime,
                    self.endTime,
                    VIN,
                    self.description,
                ),
            )
            curr.execute(
                """
                SELECT "TripID" FROM "Trip" 
                    where "StartOdometer"=%s
                         and "VIN"=%s
                """,
                (self.startOdometer, VIN),
            )
            self.tripId = curr.fetchone()[0]
            for driver in self.drivers:
                curr.execute(
                    """
                    Insert into "DriverTrip"("TripID", "DriverID")
                    values(%s, %s)
                    """,
                    (self.tripId, driver),
                )
            try:
                for category in self.categories:
                    curr.execute(
                        """
                        Insert into "TripCategoryLink"("TripID", "TripCategory")
                        values(%s, %s)
                        """,
                        (self.tripId, category),
                    )
            except AttributeError:
                pass

            try:
                for condition in self.conditions:
                    curr.execute(
                        """
                        Insert into "TripRoadCondition"("TripID", "RoadCondition")
                        values(%s, %s)
                        """,
                        (self.tripId, condition),
                    )
            except AttributeError:
                pass
            if self.tripLegs:
                for tripLeg in self.tripLegs:
                    tripLeg.writeToDB()
            dbConn.commit()
        except Exception as e:
            dbConn.rollback()
            raise e
        finally:
            curr.close()

    @classmethod
    def makeTripFromSheets(cls, dataFrame, dbConn):
        try:
            driversList = cls.driversList
        except AttributeError:
            cls.getDrivers(dbConn)
        try:
            cls.categoryList
        except AttributeError:
            cls.getCategoryList(dbConn)
        try:
            cls.conditionList
        except AttributeError:
            cls.getConditionList(dbConn)
        if dataFrame["Date"] != "":
            date = datetime.datetime.strptime(dataFrame["Date"], "%d. %b. %Y")
            if dataFrame["Start time"] != "":
                startTime = datetime.datetime.strptime(dataFrame["Start time"], "%H:%M")
            else:
                startTime = ""
            if dataFrame["End Time"] != "":
                endTime = datetime.datetime.strptime(dataFrame["End Time"], "%H:%M")
            else:
                endTime = ""
            startOdom = int(dataFrame["Start Odometer"].replace(",", ""))
            endOdom = int(dataFrame["End Odometer"].replace(",", ""))
            drivers = re.split("[+/]", dataFrame["Driver"])
            categories = re.split("[+]", dataFrame["Category"])
            conditions = re.split("[+/]", dataFrame["Road Condition"])
            for i in range(0, len(drivers)):
                drivers[i] = drivers[i].strip().upper()
            for i in range(0, len(categories)):
                categories[i] = categories[i].strip().lower()
            for i in range(0, len(conditions)):
                conditions[i] = conditions[i].strip().lower()
            otherArgs = {}
            if categories:
                otherArgs["categories"] = categories
            if conditions:
                otherArgs["conditions"] = conditions
            return trip(
                startOdom,
                endOdom,
                date,
                startTime,
                endTime,
                drivers,
                dataFrame["Destination/ purpose"],
                **otherArgs
            )

    @classmethod
    def getDrivers(cls, dbConn):
        curr = dbConn.cursor()
        curr.execute('Select "DriverID" from "Driver"')
        cls.driversList = []
        for row in curr:
            cls.driversList.append(row[0])
        curr.close()

    @classmethod
    def getCategoryList(cls, dbConn):
        with dbConn.cursor() as curr:
            curr.execute('Select "TripCategory" from "TripCategory"')
            cls.categoryList = []
            for row in curr:
                cls.categoryList.append(row[0])

    @classmethod
    def getConditionList(cls, dbConn):
        with dbConn.cursor() as curr:
            curr.execute('Select "RoadCondition" from "RoadCondition"')
            cls.conditionList = []
            for row in curr:
                cls.conditionList.append(row[0])


class dataLog:
    def __init__(self, fileName):
        self.frames = []
        self.fileName = fileName
        self.parseCSV()
        self.findDates()

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
            self.start = datetime.datetime.strptime(name, "%m%d%H%M")
        lastTime = self.frames[-1].time
        self.end = self.start + lastTime

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
                currentFrame.addDataPoint(dataPoint(service, PID, data))
        self.frames = dataFrames

    def checkEngineRunningAtEnd(self):
        for frame in reversed(self.frames):
            running = frame.checkEngineRunning()
            if running is not None:
                return running
        return False


class tripLeg:
    def __init__(self):
        self.frames = []

    def addLogFile(self, dataLog):
        self.frames = self.frames + dataLog.frames

    def setStart(self, start):
        self.start = start

    def setEnd(self, end):
        self.end = end


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

    def checkEngineRunning(self):
        try:
            return self.data[(1, 0xC)].dataList[0] > 0
        except KeyError:
            pass


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
                microsecs = int(buffStr[6:]) / 100
                time = datetime.datetime.strptime(buffStr[0:6], "%H%M%S")
                time = (time + datetime.timedelta(seconds=microsecs)).time()
                self.dataList[0] = time
            elif self.PID == 0x11:  # convert to date object
                buffStr = str(int(self.dataList[0]))
                if len(buffStr) < 6:  # if leading 0s are stripped pad them back
                    # because you know no-one apparently is competent enough to log things
                    padding = 6 - len(buffStr)
                    buffStr = "0" * padding + buffStr
                date = datetime.datetime.strptime(buffStr, "%d%m%y").date()
                self.dataList[0] = date
            elif self.PID in [
                0xA,
                0xB,
            ]:  # convert the lat and long to decimals of degrees
                self.dataList[0] = self.dataList[0] / LAT_LONG_CONVERT_FACTOR


def testClockDrift():
    for file in glob.glob("log_data/*.CSV"):
        test = dataLog(file)
        test.parseCSV()
        test.testClockDrift()


def parseFilesBatch(blobEx):
    logs = []
    for fh in glob.glob(blobEx):
        logs.append(dataLog(fh))
    byDate = sorted(logs, key=lambda log: log.start)
    tripLegs = []
    startLog = False
    trip = tripLeg()
    for log in byDate:
        if startLog:
            tripLegs.append(tripLeg)
            trip = tripLeg()
            trip.setStart(log.start)
            startLog = False
        if not log.checkEngineRunningAtEnd():
            startLog = True
            trip.setEnd(log.end)
        trip.addLogFile(log)


class DriverUndefined(Exception):
    def __init__(self, driver):
        super().__init__()
        self.driver = driver

    def __str__(self):
        return "Driver {} not in database".format(self.driver)


class CategoryUndefined(Exception):
    def __init__(self, category):
        super().__init__()
        self.category = category

    def __str__(self):
        return "Category {} not in database".format(self.category)


class ConditionUndefined(Exception):
    def __init__(self, condition):
        super().__init__()
        self.condition = condition

    def __str__(self):
        return "Condition {} not in database".format(self.condition)


# parseFilesBatch("log_data/*.CSV")
saboobaru = car("JF2GPAVC2E8306226")
try:
    dbConn = psql.connect("dbname=carData user=mgale")
    saboobaru.getTripsFromGoogleDrive(dbConn)
finally:
    dbConn.close()
