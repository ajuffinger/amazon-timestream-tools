import boto3
from botocore.config import Config
import math
import numpy as np
import signal
import threading
import time         

##################################################
## Create a timestream write client.    ##########
##################################################
def createWriteClient(region, profile = None, credStr = None):
    if profile == None and credStr == None:
        print("Using credentials from the environment")

    print("Connecting to timestream ingest in region: ", region)
    config = Config(read_timeout = 335, connect_timeout = 20, retries = {'max_attempts': 10})
    if profile != None:
        session = boto3.Session(profile_name = profile)
        client = session.client(service_name = 'timestream-write',
                            region_name = region, config = config)
    else:
        session = boto3.Session()
        client = session.client(service_name = 'timestream-write',
                            region_name = region, config = config)
    return client

def writeRecords(client, databaseName, tableName, commonAttributes, records):
    return client.write_records(DatabaseName = databaseName, TableName = tableName,
        CommonAttributes = (commonAttributes), Records = (records))

##################################################
## DDL Functions for database.            ########
##################################################
def describeDatabase(client, databaseName):
    return client.describe_database(DatabaseName = databaseName)

def createDatabase(client, databaseName):
    return client.create_database(DatabaseName = databaseName)

def deleteDatabase(client, databaseName):
    return client.delete_database(DatabaseName=databaseName)

def databaseExists(client, databaseName):
    databases = getDatabaseList(client)
    if databaseName in databases:
        return True
    return False

def getDatabaseList(client):
    databases = list()
    nextToken = None
    while True:
        if nextToken == None:
            result = client.list_databases()
        else:
            result = client.list_databases(NextToken = nextToken)
        for item in result['Databases']:
            databases.append(item['DatabaseName'])
        nextToken = result.get('NextToken')
        if nextToken == None:
            break

    return databases

##################################################
## DDL functions for tables.              ########
##################################################
def describeTable(client, databaseName, tableName):
    return client.describe_table(DatabaseName = databaseName, TableName = tableName)

def createTable(client, databaseName, tableName, retentionProperties):
    return createTable(client, databaseName, tableName, retentionProperties, {}, [])

def createTable(client, databaseName, tableName, retentionProperties, magneticStoreProperties, tags):
    return client.create_table(DatabaseName=databaseName, TableName=tableName, RetentionProperties=retentionProperties, MagneticStoreWriteProperties=magneticStoreProperties, Tags=tags)

def deleteTable(client, databaseName, tableName):
    return client.delete_table(DatabaseName=databaseName, TableName=tableName)

def tableExists(client, databaseName, tableName):
    tables = getTableList(client, databaseName)
    if tableName in tables:
        return True
    return False

def getTableList(client, databaseName):
    tables = list()
    nextToken = None
    while True:
        if nextToken == None:
            result = client.list_tables(DatabaseName = databaseName)
        else:
            result = client.list_tables(DatabaseName = databaseName, NextToken = nextToken)
        for item in result['Tables']:
            tables.append(item['TableName'])
        nextToken = result.get('NextToken')
        if nextToken == None:
            break
    return tables



##################################################
## Record Helper Functions.               ########
##################################################

def appendDimension(name, value, array):
    if isinstance(value, str):
        array.append({'Name': name, 'Value': value})
    elif not np.isnan(value):
        array.append({'Name': name, 'Value': str(value)})

def appendMultiValue(name, value, type, array):
    if not np.isnan(value):
        array.append({'Name': name, 'Value': str(value), 'Type' : type})


def estimatedWriteSize(common, records):
    # function returns an estimate for number of bytes to write.
    if len(records) > 100:
        raise('>100 records and common attributes - this write will fail.')
        
    totsize = 0
    comsize = 0
    if len(common) > 0:
        comsize = estimatedWriteSize({}, [common])
        totsize += comsize
    
    for record in records:
        payload = 0
        if 'Time' in record:
            payload += 8

        if 'Dimensions' in record:
            dimensions = record['Dimensions'];
            for dimension in dimensions:
                payload += len(dimension['Name'])
                payload += len(dimension['Value'])
        
        if 'MeasureName' in record:
            payload += len(record['MeasureName'])
            
        if 'MeasureValue' in record:
            datatype = 'DOUBLE'
            if 'MeasureValueType' in record:
                datatype = record['MeasureValueType']
            if 'MeasureValueType' in common:
                datatype = record['MeasureValueType']
                
            if datatype in ["DOUBLE","BIGINT", "BOOLEAN", "TIMESTAMP"]:
                payload += 8
            else: 
                payload += len(record['MeasureValue'])
        # Handle Multi Measures.
        if 'MeasureValues' in record:
            measures = record['MeasureValues']
            for measure in measures:
                payload += len(measure['Name'])
                if measure['Type'] in ["DOUBLE","BIGINT", "BOOLEAN", "TIMESTAMP"]:
                    payload += 8
                else: 
                    payload += len(measure['Value'])
        totsize += payload
    return totsize


def writeBulkSync(bulk, dryrun = False):
    client = bulk['client']
    databaseName = bulk['databaseName']
    tableName = bulk['tableName']
    common = bulk['common']
    records = bulk['records']
    bulkid = bulk['bulkid']
    callback = bulk['callback']
    if 'dryrun' in bulk:
        dryrun = bulk['dryrun']
    
    batch = []
    kbwrite = 0
    apicall = 0
    upscount = 0
    errcount = 0
    for record in records:
        batch.append(record)
        if len(batch) > 99:
            # deal with batch of 100 records.
            rs = estimatedWriteSize(common, batch)
            kbwrite += math.ceil(rs / 1000)
            
            try:
                if not dryrun:
                    result = writeRecords(client, databaseName, tableName, common, batch)
                    upscount += result['RecordsIngested']['Total']
            except client.exceptions.RejectedRecordsException as err:
                errcount += len(err.response["RejectedRecords"])
                upscount += len(batch) - len(err.response["RejectedRecords"])
            except Exception as err:
                raise err
            finally:
                apicall += 1
                batch = []
    # deal with the rest of the data
    if len(batch) > 0:
        rs = estimatedWriteSize(common, batch)
        kbwrite += math.ceil(rs / 1000)

        try:
            if not dryrun:
                result = writeRecords(client, databaseName, tableName, common, batch)
                upscount += result['RecordsIngested']['Total']
        except client.exceptions.RejectedRecordsException as err:
            errcount += len(err.response["RejectedRecords"])
            upscount += len(batch) - len(err.response["RejectedRecords"])
        except Exception as err:
            raise err
        finally:
            apicall += 1
            batch = []
    callback(bulkid, upscount, errcount, apicall, kbwrite)

    
##################################################
## Asynch Helper Functions.               ########
##################################################

threadqueue = []
threadloop = True
threadlock = threading.Lock()
threads = []

def signalHandler(sig, frame):
    stopAsyncWriter()

def stopAsyncWriter():
    global threadloop
    threadloop = False

def joinAsyncWriter():
    if len(threads) > 0:
        while len(threadqueue) > 0:
            time.sleep(0.5)
        stopAsyncWriter()
        for thread in threads:
            thread.join()

        time.sleep(0.5)
    
def startAsyncWriter(threadcnt = 3):
    global threadloop
    threadloop = True

    global threads
    for threadId in range(threadcnt):
        thread = threading.Thread(name='TS Async Writer:{}'.format(threadId + 1), target=asyncwriter)
        thread.start()
        threads.append(thread)

def writeBulkAsync(bulk):
    if not threadloop:
        startAsyncWriter()
    threadqueue.append(bulk)

def asyncQueueLength():
    return len(threadqueue)

        
def asyncwriter():
    # print(threading.current_thread().getName(), 'started...')
    global threadqueue
    global threadlock
    global threadloop
    
    while threadloop:
        with threadlock:
            if len(threadqueue) > 0:
                bulk = threadqueue.pop(0)
            else:
                bulk = {}
        # now if we have bulk, process it.         
        if len(bulk) > 0:
            writeBulkSync(bulk)
        else:
            time.sleep(0.05)
    #print(threading.current_thread().getName(), 'Exiting')
    
    
# dimensions = []
# appendDimension('pid', '89', dimensions)
# appendDimension('fid', '99', dimensions)
# appendDimension('aid', '7.7', dimensions)

# multivalue = []
# appendMultiValue('impression', 1.0, 'DOUBLE', multivalue)
# appendMultiValue('revenue', 2, 'DOUBLE', multivalue)

# commonA = {}
# recordA = {
#     'Dimensions': dimensions,
#     'MeasureName': 'imp_rev_sr_cpm',
#     'MeasureValues': multi,
#     'MeasureValueType': 'MULTI',
#     'Time': 121212,
#     'TimeUnit': 'MILLISECONDS'
# }
# sa1 = writeRecordSize(commonA, [recordA])
# sa2 = writeRecordSize(commonA, [recordA, recordA])


# commonB = {
#     'Dimensions': dimensions,
#     'MeasureName': 'imp_rev_sr_cpm',
#     'MeasureValueType': 'MULTI',
#     'TimeUnit': 'MILLISECONDS'
# }
# recordB = {
#     'Time': 121212,
#     'MeasureValues': multi,
# }

# sb1 = writeRecordSize(commonB, [recordB])
# sb2 = writeRecordSize(commonB, [recordB, recordB])
# print("1x A: {}, 2x A: {}, 1x B: {}, 2x B: {}".format(sa1,sa2, sb1, sb2))
