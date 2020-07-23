from carPointer import *
import glob
import logParser
import os
import psycopg2 as psql



def findNewLogs(car, dbConn):
    logBase = LOG_BASES[car.VIN]
    loggingFile = os.path.join(logBase, "Parsed.txt")
    if not os.path.exists(loggingFile):
        with open(loggingFile, 'w'):
            os.utime(loggingFile)
    with open(os.path.join(logBase, "Parsed.txt"),"r") as fh:
        filesParsed = {}
        for row in fh:
            filesParsed[row] = 1
        filesToRead = []
        for fileName in glob.glob(os.path.join(logBase,"*/*")):
            if fileName not in filesParsed:
                filesToRead.append(fileName)
    tripLegs = logParser.parseFilesBatch(filesToRead)
    car.getTripsFromGoogleDrive(dbConn)
    car.addTripLegs(tripLegs)
    car.matchUpLogsAndData()
    car.writeToDB(dbConn)
    car.clean()
    del tripLegs
    #TODO track success for these files,
    #TODO write the new ones out to the file


try:
    dbConn = psql.connect("dbname=carData user=mgale")
    for car in cars:
        findNewLogs(car, dbConn)
finally:
    dbConn.close()

