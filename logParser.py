#!/usr/bin/env python3

# TODO handle configurations, and repairs (tire change, crashes)
import csv
import datetime
import glob
import gSheets
import math
import os
import pandas as pd
import pytz
import psycopg2 as psql
import re
import struct
import time

LAT_LONG_CONVERT_FACTOR = 1e6
SECONDS_TO_DAYS = 86400  # 60*60*24
DEFAULT_ODOMETER_UNIT = "mi"
FORBIDDEN_DATA = [(0, 0x24)]  # misbehaving data collection


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
        tripsFrame = gSheets.getTrips(creds, self, self.lastOdometer)
        if tripsFrame and not tripsFrame.empty:
            for index, row in tripsFrame.iterrows():
                newTrip = trip.makeTripFromSheets(row, dbConn)
                if newTrip:
                    self.trips.append(newTrip)
            self.cleanUpLogs()
            print("finished adding all trips from G-Drive")
        else:
            print("No new trips")

    def addTripLegs(self, legs):
        self.tripLegs = legs
        self.tripLegs.sort()

    def cleanUpLogs(self):
        """
        Cleans up start and end times
        """
        # ensure they are in order
        self.trips.sort()

        back = datetime.timedelta(minutes=-1)
        forward = datetime.timedelta(minutes=1)
        timeZone = gSheets.sheetsPointer.DEFAULT_TZ
        for i, trip in enumerate(self.trips):
            if not trip.startTime:
                try:
                    if trip.date == self.trips[i - 1].date:
                        lastEnd = self.trips[i - 1].endTime
                        if lastEnd:
                            bufferTime = (
                                datetime.datetime.combine(
                                    datetime.date.today(), lastEnd
                                )
                                + forward
                            )
                        else:
                            raise AmbiguousTripLog(
                                trip.date.strftime("%d %b %y"),
                                trip.startOdometer,
                                trip.endOdometer,
                            )
                    else:
                        bufferTime = datetime.datetime.combine(
                            trip.date, datetime.time(0, 1)
                        )
                    if not bufferTime.tzinfo:
                        bufferTime = timeZone.localize(bufferTime)
                    trip.startTime = bufferTime.timetz()
                except IndexError:
                    pass
            if not trip.endTime:
                try:
                    if trip.date == self.trips[i + 1].date:
                        nextStart = self.trips[i + 1].startTime
                        if nextStart:
                            bufferTime = (
                                datetime.datetime.combine(
                                    datetime.date.today(), nextStart
                                )
                                + back
                            )
                        else:
                            raise AmbiguousTripLog(
                                trip.date.strftime("%d %b %y"),
                                trip.startOdometer,
                                trip.endOdometer,
                            )
                    else:
                        bufferTime = datetime.datetime.combine(
                            trip.date, datetime.time(23, 59)
                        )
                    if not bufferTime.tzinfo:
                        bufferTime = timeZone.localize(bufferTime)
                    self.endTime = bufferTime.timetz()

                except IndexError:
                    pass

    def matchUpLogsAndData(self):
        self.cleanUpLogs()
        badTrips = []
        legsToDelete = []
        for trip in self.trips:
            for i, leg in enumerate(self.tripLegs):
                if leg:
                    try:
                        if (
                            leg.getStartTime() >= trip.getStartTime()
                            and leg.getEndTime() <= trip.getEndTime()
                        ):
                            trip.addTripLeg(leg)
                            legsToDelete.append(i)
                    except TypeError as e:
                        badTrips.append(trip)
                        break
            if self.tripLegs:
                for i in sorted(legsToDelete, reverse=True):
                    del self.tripLegs[i]
                legsToDelete = []
        print("Finished matching logs and trips")
        # boil trip leg times up to trip
        # TODO better split up trip logs
        # parse year from the file name and folders

    def writeToDB(self, dbConn):
        files = []
        try:
            for trip in self.trips:
                if trip.writeToDB(dbConn, DEFAULT_ODOMETER_UNIT, self.VIN):
                    files = files + trip.getFiles()
            print("Wrote car: {} to DB".format(self.VIN))
            return files
        except Exception as e:
            print("Error: {}".format(e))
            return files

    def clean(self):
        del self.trips
        del self.tripLegs


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
            self.startTime = start.timetz()
        else:
            self.startTime = None
        if end:
            self.endTime = end.timetz()
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

    def addTripLeg(self, leg):
        self.tripLegs.append(leg)

    def writeToDB(self, dbConn, odometerUnit, VIN):
        print("Started writing trip to DB")
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
            self.combineDrivers()
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
                    tripLeg.writeToDB(curr, self.tripId)
            print(
                "Wrote trip: {} starting: {} mi".format(self.date, self.startOdometer)
            )
            dbConn.commit()
            curr.close()
            return True
        except Exception as e:
            dbConn.rollback()
            raise e
        finally:
            if curr:
                curr.close()

    def combineDrivers(self):
        """
        Makes MG + MJ into MM
        """
        if len(self.drivers) == 2 and "MG" in self.drivers and "MJ" in self.drivers:
            self.drivers = ["MM"]

    def __lt__(self, other):
        return self.startOdometer < other.startOdometer

    def __eq__(self, other):
        return self.startOdometer == other.startOdometer

    def getEndTime(self):
        return datetime.datetime.combine(self.date, self.endTime)

    def getStartTime(self):
        return datetime.datetime.combine(self.date, self.startTime)

    def getFiles(self):
        files = []
        for leg in self.tripLegs:
            files = files + leg.files

        return files

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
            timeZone = gSheets.sheetsPointer.DEFAULT_TZ
            date = datetime.datetime.strptime(dataFrame["Date"], "%d. %b. %Y")
            if dataFrame["Start time"] != "":
                startTime = timeZone.localize(
                    datetime.datetime.strptime(dataFrame["Start time"], "%H:%M")
                )
            else:
                startTime = ""
            if dataFrame["End Time"] != "":
                endTime = timeZone.localize(
                    datetime.datetime.strptime(dataFrame["End Time"], "%H:%M")
                )
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
            self.start = pytz.utc.localize(datetime.datetime.strptime(name, "%m%d%H%M"))
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
        self.files = []

    def addLogFile(self, dataLog):
        self.frames = self.frames + dataLog.frames
        self.files.append(dataLog.fileName)

    def setStart(self, start):
        self.startDateTime = start

    def setEnd(self, end):
        self.endDateTime = end

    def getStartTime(self):
        return self.startDateTime

    def getEndTime(self):
        return self.endDateTime

    def writeToDB(self, curr, ParentTripId):
        curr.execute(
            """
            Insert into "TripLeg" ("TripID")
            values(%s)
            """,
            (ParentTripId,),
        )
        curr.execute(
            """
            Select "TripLegID" from "TripLeg" where "TripID"=%s
            Order by "TripLegID" desc
            """,
            (ParentTripId,),
        )
        self.TripLegId = curr.fetchone()[0]

        for frame in self.frames:
            frame.writeToDB(curr, self.TripLegId)
        print("Wrote trip-Leg to database")

    def __lt__(self, other):
        return self.startDateTime < other.startDateTime


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
            return self.data[(0, 0x11)].specialData
        except KeyError:
            return None

    def getGPS_Time(self):
        try:
            return self.data[(0, 0x10)].specialData
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

    def writeToDB(self, curr, parentTripLegId):
        curr.execute(
            """
            Insert into "DataFrame" ("TimeOffset", "TripLegID")
            values (%s, %s)
            """,
            (self.time.total_seconds(), parentTripLegId),
        )
        curr.execute(
            """
            Select "DataFrameID" from "DataFrame"
            where "TripLegID" = %s
            Order by "DataFrameID" DESC
            """,
            (parentTripLegId,),
        )
        self.DataFrameId = curr.fetchone()[0]
        for point in self.data:
            self.data[point].writeToDB(curr, self.DataFrameId)


class dataPoint:
    def __init__(self, service, PID, data):
        self.service = service
        self.PID = PID
        self.dataList = data
        # if len(data) == 0:
        self.rawData = int(data[0])
        self.cleanUpData()

    def getID(self):
        return (self.service, self.PID)

    def __str__(self):
        return "({},{}): {}".format(self.service, self.PID, self.dataList)

    def __repr__(self):
        return self.__str__()

    def writeToDB(self, cur, dataFrameId):

        if self.getID() not in FORBIDDEN_DATA:
            self.convert(cur)

            if self.rawData:
                # write in the raw data
                cur.execute(
                    """
                    Insert into "RawData" ("DataFrameID", "OBD_Service", "PID", "Value")
                    values(%s, %s, %s, %s)
                    """,
                    (dataFrameId, self.service, self.PID, self.rawData),
                )

            for i, convert in enumerate(self.conversions[(self.service, self.PID)]):
                byteStart = convert["start"]
                cur.execute(
                    """
                    Insert into "ParsedData" ("DataFrameID", "OBD_Service", "PID", "byteStart", "Value")
                    values(%s, %s, %s, %s, %s)
                    """,
                    (dataFrameId, self.service, self.PID, byteStart, self.dataList[i]),
                )

    def convert(self, cur):
        self.getRawConversion(cur)
        if self.service == 0:
            if self.PID == 0x10:  # convert to time
                hour = int(self.specialData.strftime("%H"))
                minute = int(self.specialData.strftime("%M"))
                sec = int(self.specialData.strftime("%S"))
                micro = int(self.specialData.strftime("%f"))
                self.dataList = [(hour * 60 + minute) * 60 + sec + micro / 1e6]
                return
            elif self.PID == 0x11:  # back convert date object
                epochSec = time.mktime(self.specialData.timetuple())
                epochDays = math.floor(epochSec / SECONDS_TO_DAYS)
                self.dataList = [epochDays]
                return
            elif self.PID in [0x20, 0x21, 0x22]:
                self.rawData = None
                return
            elif self.PID in [0xA, 0xB]:
                return  # all data processing already handled in the clean-up

        # handles all conversions
        conversionFactor = self.conversions[(self.service, self.PID)]

        # split into bytes
        byteBuffer = []
        totalBytes = 0
        for convert in conversionFactor:
            totalBytes += convert["length"]
        words = self.rawData.to_bytes(totalBytes, "big")  # convert to bytes array

        if len(conversionFactor):  # pull whole number as the value
            byteBuffer.append(self.rawData)
        else:
            # split by bytes into a list
            for convert in conversionFactor:
                start = convert["start"]
                end = start + convert["length"]
                byteBuffer.append(int.from_bytes(words[start:end], "big"))
        self.dataList = []

        # convert each word into the needed values
        for i, word in enumerate(byteBuffer):
            if conversionFactor[i]["mult"]:
                mult = conversionFactor[i]["mult"]
            else:
                mult = 1.0
            if conversionFactor[i]["add"]:
                add = 0.0
            else:
                add = 0.0
            self.dataList.append(word * mult + add)

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
                time = pytz.utc.localize(
                    (time + datetime.timedelta(seconds=microsecs)).time()
                )
                self.specialData = time
            elif self.PID == 0x11:  # convert to date object
                buffStr = str(int(self.dataList[0]))
                if len(buffStr) < 6:  # if leading 0s are stripped pad them back
                    # because you know no-one apparently is competent enough to log things
                    padding = 6 - len(buffStr)
                    buffStr = "0" * padding + buffStr
                date = datetime.datetime.strptime(buffStr, "%d%m%y").date()
                self.specialData = date
            elif self.PID in [
                0xA,
                0xB,
            ]:  # convert the lat and long to decimals of degrees
                self.dataList[0] = self.dataList[0] / LAT_LONG_CONVERT_FACTOR

    @classmethod
    def getRawConversion(cls, cur):
        try:
            cls.conversions
        except AttributeError:
            cur.execute(
                """
                Select "OBD_Service", "PID", "byteStart", "wordLength","Multiplier","adder"
                from "Parameter_Byte"
                Order by "OBD_Service" ASC, "PID" ASC, "byteStart" ASC
                """
            )
            cls.conversions = {}
            for row in cur:
                pointer = (row[0], row[1])
                payLoad = {
                    "start": row[2],
                    "length": row[3],
                    "mult": row[4],
                    "add": row[5],
                }
                if pointer not in cls.conversions:
                    cls.conversions[pointer] = []
                cls.conversions[pointer].append(payLoad)


def testClockDrift():
    for file in glob.glob("log_data/*.CSV"):
        test = dataLog(file)
        test.parseCSV()
        test.testClockDrift()


def parseFilesBatch(filesToRead):
    logs = []
    for fh in filesToRead:
        if os.path.isfile(fh) and os.stat(fh).st_size > 0:
            logs.append(dataLog(fh))
    print("All log files parsed")
    byDate = sorted(logs, key=lambda log: log.start)
    tripLegs = []
    startLog = True
    trip = tripLeg()
    size = len(byDate)
    for i, log in enumerate(byDate):
        if startLog:
            trip = tripLeg()
            tripLegs.append(trip)
            trip.setStart(log.start)
            startLog = False
        if i == (size - 1) or not log.checkEngineRunningAtEnd():
            startLog = True
            trip.setEnd(log.end)
        trip.addLogFile(log)
    print("Logs combined into trip legs")
    return tripLegs


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


class AmbiguousTripLog(Exception):
    def __init__(self, date, startOdom, endOdom):
        super().__init__()
        self.date = date
        self.start = startOdom
        self.end = endOdom

    def __str__(self):
        return "Trip log entry ambiguous for the date:{} with the odometer range: {}-{}".format(
            self.date, self.start, self.end
        )
