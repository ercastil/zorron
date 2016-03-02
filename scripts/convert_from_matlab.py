#!/usr/bin/env python

import scipy.io 
import sys
import os
import json
import csv
import numpy
import re
import datetime

def main(inputPath,outputPath):
	
	#json directory
	jsonDirectory = os.path.join( inputPath, 'json' )

	#matlab directory
	matlabDirectory = os.path.join( inputPath, 'mat' )

	fileNames = os.listdir( matlabDirectory )
	for fileName in fileNames:
		try:
			print fileName
			fileName = fileName.split('.')[0]
			processTable(fileName,jsonDirectory,matlabDirectory, outputPath)
		except:
			sys.stderr.write( ("ERROR: file %s processing failed\n") % ( fileName ) )	
			raise

def processTable(fileName,jsonDirectory,matlabDirectory,outputPath):

	#read content
	jsonContent   = readJsonFile( os.path.join( jsonDirectory, fileName + '.json' ) )
	matlabContent = readMatlabFile( os.path.join( matlabDirectory, fileName + '.mat' ) )

	#create directory
	tableDirectoryPath = os.path.join( outputPath, jsonContent['code'] )
	os.mkdir( tableDirectoryPath )

	#write content
	metaDataFilePath = os.path.join( tableDirectoryPath, 'metadata.json' )
	writeMetaDataFile(jsonContent,matlabContent,metaDataFilePath)

	dataFilePath = os.path.join( tableDirectoryPath, 'data.csv' )
	writeDataFile(matlabContent,dataFilePath)

def readJsonFile(inputPath):

	inputFile = open( inputPath, 'r' )	
	content = json.load( inputFile )
	inputFile.close()

	result = {
		   'code' : content['id'],
		   'name' : content['name'],
		   'project': content['project'],
		   'latitude' : content['lat'],
		   'longitude' : content['lon'],
		   'elevation' : content['alt']
		 }

	return result

def parseVariableFeatures(variableName):
	
	try:
		result = re.search( 'XXX' , variableName )

		if result is None:	
			result = re.search( '\d+' , variableName )
			value = result.group(0)
			end = result.end(0)
		else:
			value = 'XXX'
			end = result.end(0)

		if value == 'XXX':
			variableAltitude = 0.0
		else:
			variableAltitude = float( variableName[end-3:end] ) / 10

		variableType = variableName[0:end-3]		

		#statistic
		variableStatistic = 'mean'
		if len(variableName) > end:
			variableStatistic = variableName[end:]

		features = {
				'type' : variableType,
				'altitude' : variableAltitude,
				'statistic' : variableStatistic
			   }
	except:
		print variableName
		features = None
	
	return features

def readTime(array):

	matlabTimeArray = array['time']
	matlabTimeArray = numpy.reshape( matlabTimeArray, (matlabTimeArray.shape[0]) )

	matlabTimeArray = numpy.round( matlabTimeArray * 3600 ) / 3600
	timeArray = numpy.round( ( numpy.double(matlabTimeArray) - 719529 ) * 86400 )
	

	isodateArray = []
	for time in timeArray:
		time = datetime.datetime.fromtimestamp(time)
		isodateArray.append( time.isoformat() )	

	return isodateArray

def readMatlabFile(inputPath):
	
	content = scipy.io.loadmat( inputPath )

	array = content['obs']
	internalArray = array[0][0]
	variableArray = internalArray['vars']

	#table
	tableCode 	= internalArray['stcode'][0]
	name 		= internalArray['stname'][0]
	latitude 	= internalArray['lat'][0][0]
	longitude 	= internalArray['lon'][0][0]
	altitude 	= internalArray['alt'][0][0]

	table = {
			'code' : tableCode,
			'name' : name,
			'latitude' : latitude,
			'longitude' : longitude,
			'elevation' : altitude
		}

	#identify variables
	variableNames = [ varItem[0] for varItem in variableArray[0] ]

	variables = []
	tableData = []
	
	#read time
	timeArray = readTime( internalArray )
	tableData.append( timeArray )

	#read variable data
	count = 1
	for name in variableNames:
		
		features = parseVariableFeatures( name )

		if features is None:
			continue
		
		features['code'] = 'v%d' % ( count )
		data = internalArray[name]
		array = numpy.reshape( data, (data.shape[0] ) )

		variables.append( features )
		tableData.append( array )

		count = count + 1
	
	tableData = numpy.array( tableData ).transpose()

	result = {
		   'table' : table,
		   'variables' : variables,
		   'data' : tableData
		 }

	return result

def writeMetaDataFile(jsonContent,matlabContent,outputPath):
	
	outputFile = open( outputPath, 'w' )
	elevation = jsonContent['elevation']
	latitude = jsonContent['latitude']
	longitude = jsonContent['longitude']
	
	if elevation is None:
		elevation = float( matlabContent['table']['elevation'] )
	
	if latitude is None:
		latitude = float( matlabContent['table']['latitude'] )
		
	if longitude is None:
		longitude = float( matlabContent['table']['longitude'] )
		
	metaData  = {	
			'code' 		: jsonContent['code'],
			'name'		: matlabContent['table']['name'],
			'project'	: jsonContent['project'],
			'latitude'	: jsonContent['latitude'],
			'longitude'	: jsonContent['longitude'],
			'elevation'	: elevation,
			'variables'	: matlabContent['variables']
		    }
	json.dump( metaData, outputFile, indent=4 )
	outputFile.close()

def writeDataFile(matlabContent,outputPath):
	
	tableData = matlabContent['data']
	outputFile = open( outputPath, 'w' )
	writer = csv.writer(outputFile)	
	writer.writerows(tableData)
	outputFile.close()

#read parameters
inputDirectoryPath = sys.argv[1]
outputDirectoryPath = sys.argv[2]
main( inputDirectoryPath, outputDirectoryPath )
