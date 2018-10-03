import sys
import requests
import json
import platform
import os
import pymssql

from datetime import datetime

# Globals
debug = False
rgfile = 'resgrp.txt'
db_VnetLatencyTbl = 'Tbl_VnetLatency'
credSQL = './connectionstring.txt'
isLinux = 1
hasAccelNet = 0
TestTool = "SockPerf"

def checkAndPrepArgs():
  """checkAndPrepArgs checks that the arguments provided when calling the script are valid.
  It also assigns some default values for optional parameters (if not provided by the user).
  """
  params = {}
  global hasAccelNet
  if len(sys.argv) < 6:
    print('Not enough arguments provided. \n')
    print('\n***USAGE: The following parameters are required:')
    print('  AN|NoAN MsgSize vmSender vmReceiver filePath placementMode')
    print(' Optional string: the receiver\'s name')
    print('Actual number of arguments provided:', len(sys.argv))
    print('  Argument List provided:', str(sys.argv))
    print('Example:', sys.argv[0], 'AN 4 vmone vmtwo sockperf.out AvSet')
    print('\n*** EXITING ***')
    sys.exit(1)
  else:
    test_AN = sys.argv[1]
    if  test_AN == "AN": hasAccelNet = 1 ## Otherwise default is zero
    params['AccelNetOn'] = hasAccelNet
    params['Msg_Size'] = sys.argv[2]  
    params['vmSender'] = sys.argv[3]
    params['vmReceiver'] = sys.argv[4]
    params['filename'] = sys.argv[5]
    params['placement'] = sys.argv[6]
    
    return params

def parseSockPerfOutput(filename, result, percentile, finalResult, params):
  """parseSockPerfOutput parses the output of SockPerf, extracts the relevant data and
  then adds it to the result[] dictionary
  """
  with open(filename, 'r') as file:
    for line in file:
      if line.startswith('sockperf:'): # All output from sockperf starts with "sockperf:"
        params['TestTool'] = 'SockPerf'
        if line.startswith('sockperf: ---> '): 
          if line.startswith('sockperf: ---> percentile'):
            percentiles = line.split(' ')
            percentile[percentiles[3]] = percentiles[-1].strip() 
          else:
            if line.startswith('sockperf: ---> <MAX>'):
              percentile['Max_Latency_usec'] = line.split(' ')[-1].strip()
            else:
              percentile['Min_Latency_usec'] = line.split(' ')[-1].strip()
        if 'observations' in line:
          result['Observations'] = line.split(' ')[2]
        else:
          if line.startswith('sockperf: [Valid Duration] '): ## RunTime, SentMessages and ReceivedMessages
            removeEquals = line.split('=')
            removeSemiColon = []
            for item in removeEquals:
              removeSemiColon += item.split(';')
            result['RunTime'] = removeSemiColon[1].strip()[:-4]
            result['SentMessages'] = removeSemiColon[3].strip()
            result['ReceivedMessages'] = removeSemiColon[5].strip()
            
          if 'avg-lat' in line:
            ## avg-lat results slightly change the output is there results are >99 usecs
            ## This is due to sockperf format reserving 7 positions for the output
            ## as follows: avg-lat=%7.3lf
            ## Source: https://github.com/Mellanox/sockperf/blob/31a0b54b26e4619b79d1296ad608e03075e9e255/src/client.cpp
            if len(line.split('= ')) > 1: ## If avg-lat =<99 usecs there'll be space after '= '
              result['Avg_Latency_usec'] = line.split('= ')[1].split(' (')[0]
            else: ## If avg-lat are >99 usecs there'll be no space after '='
              result['Avg_Latency_usec'] = line.split('=')[5].split(' ')[0]
            # Common fields
            result['std_Deviation'] = line.split('std-dev=')[1].split(')')[0]
            result['isFullRTT'] = False

          # avg-rtt is used instead of avg-lat when selecting full rtt results
          if 'avg-rtt' in line:
            ## avg-rtt results slightly change the output is there results are >99 usecs
            ## This is due to sockperf format reserving 7 positions for the output
            ## as follows: avg-lat=%7.3lf
            ## Source: https://github.com/Mellanox/sockperf/blob/31a0b54b26e4619b79d1296ad608e03075e9e255/src/client.cpp
            if len(line.split('= ')) > 1: ## If avg-rtt =<99 usecs there'll be space after '= '
              result['Avg_Latency_usec'] = line.split('= ')[1].split(' (')[0]
            else: ## If avg-rtt are >99 usecs there'll be no space after '='
              result['Avg_Latency_usec'] = line.split('=')[5].split(' ')[0]
            # Common fields
            result['std_Deviation'] = line.split('std-dev=')[1].split(')')[0]
            result['isFullRTT'] = True

      else:
        if (debug): print('Unrecognized input: {0}\n'.format(line))

  if (debug): print('SockPerf results: {0}\n'.format(finalResult))

def prepSql(result, percentile, params):
  """prepSql standardizes column names (from sockperf to what we have in the DB)
  """
  #### SQL
  ## First we start reworking the key names for the percentiles before calling genSql()
  sqlFields = {\
  'VMSize': params['VMSize'], 'IsLinux': params['IsLinux'], \
  'OS_Distro': params['OS_Distro'], 'AccelNetOn': params['AccelNetOn'], \
  'Msg_Size': params['Msg_Size'], 'Region': params['Region'], \
  'OS_SKU': params['OS_SKU'], 'PatchVersion': params['PatchVersion'], \
  'ResourceGroup': params['ResourceGroup'], 'vmId': params['vmId'], \
  'PlacementMode': params['PlacementMode'], \
  'vmSender': params['vmSender'], 'vmReceiver': params['vmReceiver'], \
  'Avg_Latency_usec': result['Avg_Latency_usec'], \
  'Max_Latency_usec': percentile['Max_Latency_usec'], \
  'P59s_usec': percentile['99.999'],'P49s_usec': percentile['99.990'], \
  'P39s_usec': percentile['99.900'],'P99_usec': percentile['99.000'], \
  'P90_usec': percentile['90.000'],'P50_usec': percentile['50.000'], \
  'P25_usec': percentile['25.000'], \
  'Min_Latency_usec': percentile['Min_Latency_usec'], \
  'Iterations': result['Observations'], 'Duration': result['RunTime'], 'TestTool': params['TestTool'], \
  'isFullRTT': result['isFullRTT'], 'std_Deviation': result['std_Deviation'], \
  'RawDataFile': validateAndPrepFileForUpload(params['filename']) }
  if (debug): print('SQL statement parameters: {0}\n'.format(sqlFields))
  return sqlFields

def gather_metadata(params):
  """gather_metadata uses the metadata API to learn some attributes from the VM.
  """
  params['PatchVersion'] = platform.release()

  try:
    #Data gathered from the HOST node
    url = 'http://169.254.169.254/metadata/instance?api-version=2017-04-02'
    headers = {'Metadata': 'true'}
    r = requests.get(url, headers=headers)
    jtext = json.loads(r.text)
    params['Region'] = jtext['compute']['location']
    params['VMSize'] = jtext['compute']['vmSize']
    params['OS_SKU'] = jtext['compute']['sku']
    params['vmId'] = jtext['compute']['vmId']
    params['OS_Distro'] = jtext['compute']['offer']
    
    if jtext['compute']['osType'] == "Linux":
      params['IsLinux'] = "True"
    else:
      params['IsLinux'] = "False"
  except:
    ## Metadata request failed...
    if (debug): print('Metadata request failed. Using empty parameters.\n')
    params['Region'] = ''
    params['VMSize'] = ''
    params['OS_SKU'] = ''
    params['vmId'] = ''
    params['OS_Distro'] = ''
    params['IsLinux'] = ''

def getRGName(params):
  """getRGName finds the resource group name
  """
  if os.path.isfile('./' + rgfile):
    with open('./' + rgfile, 'r') as rgf:
      resGrp = rgf.readline().strip()
  elif os.path.isfile('/usr/local/bin/' + rgfile):
    with open('/usr/local/bin/' + rgfile, 'r') as rgf:
      resGrp = rgf.readline().strip()
  elif os.path.isfile(rgfile):
    with open(rgfile, 'r') as rgf:
      resGrp = rgf.readline().strip()
  else:
    resGrp = "ResGrp.txt_NOT_FOUND_" + rgfile
  params['ResourceGroup'] = resGrp

def genSql(sqlFields):
  """genSql generates the sql query that will insert the data into the DB.
  """
  # Notes
  ## Table name: db_LatTbl
  ## Other values not gathered 
  dtime = datetime.now()
  ftdtime = dtime.strftime("%Y-%m-%d %H:%M:%S")
  sql = "insert into " + db_VnetLatencyTbl +\
  " (TestDate,VMSize,IsLinux,OS_Distro,AccelNetOn,PlacementMode," \
  "Msg_Size,Iterations,Duration_Sec,Avg_Latency_usec,std_Deviation,Max_Latency_usec," \
  "P59s_usec,P49s_usec,P39s_usec,P99_usec,P90_usec,P50_usec,P25_usec,Min_Latency_usec," \
  "Region,OS_SKU,PatchVersion,ResourceGroup,vmId,vmSender,vmReceiver,TestTool,isFullRTT,RawDataFile)" \
   "values('{0}','{1}','{2}','{3}','{4}','{5}'," \
   "'{6}','{7}','{8}','{9}','{10}','{11}'," \
   "'{12}','{13}','{14}','{15}'," \
   "'{16}','{17}','{18}','{19}'," \
   "'{20}','{21}','{22}','{23}'," \
   "'{24}','{25}','{26}','{27}','{28}','{29}')".format( \
str(ftdtime),sqlFields['VMSize'],sqlFields['IsLinux'],\
sqlFields['OS_Distro'],sqlFields['AccelNetOn'],sqlFields['PlacementMode'],sqlFields['Msg_Size'],\
sqlFields['Iterations'],sqlFields['Duration'],sqlFields['Avg_Latency_usec'],\
sqlFields['std_Deviation'],\
sqlFields['Max_Latency_usec'], sqlFields['P59s_usec'],sqlFields['P49s_usec'],\
sqlFields['P39s_usec'], sqlFields['P99_usec'], sqlFields['P90_usec'],\
sqlFields['P50_usec'], sqlFields['P25_usec'], sqlFields['Min_Latency_usec'],sqlFields['Region'],\
sqlFields['OS_SKU'],sqlFields['PatchVersion'],sqlFields['ResourceGroup'],\
sqlFields['vmId'],sqlFields['vmSender'],sqlFields['vmReceiver'],sqlFields['TestTool'],\
sqlFields['isFullRTT'],sqlFields['RawDataFile'])
  return sql

def sqlInsert(sqlStatement):
  """sqlInsert connects to the Sql DB and sends the query generated in genSql()
  """
  # Connect to the database
  if (debug): print("\n*** Getting Connection String ***\n")
  file = open(credSQL, "r")
  s = file.read().replace("\n","")
  connectionString = dict(item.split(":") for item in s.split(","))
  if (debug): print("\n*** Got Connection String ***")

  if (debug): print("\n*ConnStr:" + str(connectionString), "***\n")

  if (debug): print("\n*** Creating SQL Connect ***")
  conn = pymssql.connect(host=connectionString["host"], user=connectionString["user"],
          password=connectionString["password"], database=connectionString["database"])
  if (debug): print("\n*** Setting conn.cursor ***\n")
  cursor = conn.cursor()

  if (debug): print("Connecting to DB...")

  try:
    cursor.execute(sqlStatement)
    if (debug): print(sqlStatement)
  except TypeError as e:
    print("FAILED to execute SQL:\n" + sqlStatement)
    print(e)
    pass
  
  conn.commit()

def validateAndPrepFileForUpload(filename):
  """validateAndPrepFileForUpload checks the file size is lower than 6k
  before adding it to the Sql query by returning it as a string.
  This function is called from prepSql()
  """
  if (os.stat(filename).st_size <= 6144):
    with open(filename, 'r') as myfile:
      rawData = myfile.read()
  return rawData

def main():
  ## Initialize variables 
  result = {} 
  percentile = {}
  finalResult = { 'Summary': result, 'Details': percentile }
  credSQL = './connectionstring.txt'

  ## Check SQL creds file exists.
  if os.path.exists('./' + credSQL):
    credSQL = './' + credSQL
  elif os.path.isfile('/usr/local/bin/' + credSQL):
    credSQL = '/usr/local/bin/' + credSQL
  else:
    print("\n***Exiting due to missing required file: " + credSQL, "***\n")
    exit()

  params = checkAndPrepArgs() ## Get some data from arguments
  parseSockPerfOutput(params['filename'], result, percentile, finalResult, params) ## Parse SockPerf output and get the results
  gather_metadata(params) ## Gather some metadata about the VM, OS, etc
  getRGName(params) ## Get RG name from resgrp.txt file
  sqlFields = prepSql(result, percentile, params) ## Put all together ready to send generate the SQL statement
  sqlStatement = genSql(sqlFields) ## Generate the SQL statement
  print(sqlStatement)
  sqlInsert(sqlStatement) ## Insert data into DB
  

main()
