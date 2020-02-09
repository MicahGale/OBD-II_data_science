#!/usr/bin/env python3


import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import numpy as np
import pandas as pd

import sheetsPointer

COLUMNS_TO_SCRAPE = [
    "Date",
    "Start Odometer",
    "End Odometer",
    "Start time",
    "End Time",
    "Destination/ purpose",
    "Driver",
    "Road Condition",
    "Category",
]


def getAuthCreds():
    """ 
    Return Oath Credentials based on Google guide:

    <https://developers.google.com/sheets/api/quickstart/python#step_3_set_up_the_sample>
    """
    creds = None
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists("token.pickle"):
        with open("token.pickle", "rb") as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("token.pickle", "wb") as token:
            pickle.dump(creds, token)
    return creds


def getTripRange(sheetHandle, workbook, range):
    return (
        sheetHandle.values()
        .get(
            spreadsheetId=workbook,
            range="{}!{}".format(sheetsPointer.TRIP_SHEET, range),
        )
        .execute()
    )


def getTripRanges(sheet, workbook, columnMap, minRow, maxRow):
    ranges = []

    for column, letter in columnMap.items():
        ranges.append(
            "{0}!{1}{2}:{1}{3}".format(sheetsPointer.TRIP_SHEET, letter, minRow, maxRow)
        )
    result = (
        sheet.values()
        .batchGet(spreadsheetId=workbook, ranges=ranges)
        .execute()
    )

    return result


def getTrips(credentials, minMileage=0):
    # TODO check VIN
    #TODO handle mileage units
    workbook = sheetsPointer.SHEET_ID
    service = build("sheets", "v4", credentials=credentials)
    sheet = service.spreadsheets()
    # First get the column headers

    result = getTripRange(sheet, workbook, "1:1")

    values = result.get("values", [])
    columnMap = {}
    # Get columns of the headers
    for i, column in enumerate(values[0]):
        if column.strip() in COLUMNS_TO_SCRAPE:
            columnMap[column] = chr(ord("A") + i)

    result = getTripRange(
        sheet, workbook, "{}:{}".format(columnMap["Date"], columnMap["Start Odometer"])
    )
    values = result.get("values", [])

    minRow = -1
    maxRow = 0
    # find the rows of interest
    # those with a high-enough mileage, but with valid date inputs
    for i, row in enumerate(values):
        if i > 0:  # skip headers
            if row[0] != "":
                # if min Row not set
                if minRow == -1:
                    if float(row[1].replace(",", "")) > minMileage:
                        minRow = i + 1
                maxRow = i + 1
            else:
                break
    if minRow > 0:
        result = getTripRanges(sheet, workbook, columnMap, minRow, maxRow)
        values = result.get("valueRanges", [])
        valueDict = {}
        for i, key in enumerate(columnMap):
            buffer=[""]*(maxRow - minRow +1)
            for j, row in enumerate(values[i].get("values", [])[1:]):
                if(len(row)>=1):
                    buffer[j] = row[0]
            valueDict[key]=buffer
        return pd.DataFrame(valueDict)



