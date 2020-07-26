from carPointer import *
import glob
import logParser
import os
import psycopg2 as psql

CONVERT = False


def findNewLogs(car, dbConn):
    logBase = LOG_BASES[car.VIN]
    loggingFile = os.path.join(logBase, "Parsed.txt")
    if not os.path.exists(loggingFile):
        with open(loggingFile, "w"):
            os.utime(loggingFile)
    with open(loggingFile, "r") as fh:
        filesParsed = {}
        for row in fh:
            filesParsed[row] = 1
        filesToRead = []
        for fileName in glob.glob(os.path.join(logBase, "*/[0-9]*.CSV")):
            if fileName not in filesParsed:
                filesToRead.append(fileName)
    tripLegs = logParser.parseFilesBatch(filesToRead)
    car.getTripsFromGoogleDrive(dbConn)
    car.addTripLegs(tripLegs)
    car.matchUpLogsAndData()
    filesRead = car.writeToDB(dbConn, CONVERT)
    car.clean()
    del tripLegs
    with open(loggingFile, "a") as fh:
        for fileName in filesRead:
            fh.writelines(fileName)


try:
    dbConn = psql.connect("dbname=carData user=mgale")
    for car in cars:
        findNewLogs(car, dbConn)
finally:
    dbConn.close()
