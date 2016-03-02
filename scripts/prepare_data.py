#!/usr/bin/env python

"""
ZORRO-N - Meteorological Time Series DataBase Engine
Copyright (C) 2014 - Ernesto Castillo Navarrete

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""


import sys,traceback
import csv
import json
import os
import datetime

def prepare_metadata(headerArray,metaDataInputPath,outputPath):

	inputFile = open( metaDataInputPath, 'r' )	
	inputMetaData = json.load(inputFile)
	inputFile.close()

	outputMetaData = {
				'code' 		: inputMetaData['id'],
				'name' 		: inputMetaData['name'],
				'latitude' 	: inputMetaData['location']['lat'],
				'longitude'	: inputMetaData['location']['lon'],
				'elevation'	: inputMetaData['location']['elev'],
				'group'		: inputMetaData['group'],
				'info' 		: inputMetaData
			 }
	
	outputMetaData['variables'] = []
	count = 0
	for row in headerArray:
		
		variableType  		= row[1]
		variableStatistic 	= row[2]
		variableAltitude	= row[3]

		variableCode  		= 'v%d' % ( count )
		count = count + 1


		variableMetaData = {
					'code'		: variableCode,
					'name' 		: variableCode,
					'type'		: variableType,
					'statistic' 	: variableStatistic,
					'altitude'	: float(variableAltitude)
				   }

		outputMetaData['variables'].append( variableMetaData )
		

	outputFile = open(outputPath, 'w')
	json.dump(outputMetaData,outputFile, indent=4)
	outputFile.close()

	return inputMetaData

def prepare_data(metaData,headerArray,dataInputPath,outputPath):

	inputFile = open( dataInputPath, 'r' )
	reader = csv.reader( inputFile )
	dataArray = []
	for row in reader:
		dataArray.append( row )
	inputFile.close()

	outputArray = []
	for dataRow in dataArray:
		
		outputRow = []
		
		year 	= int( dataRow[0] )
		month 	= int( dataRow[1] ) 
		day 	= int( dataRow[2] )
		hour	= int( dataRow[3] )
		minute	= int( dataRow[4] )
		second  = int( dataRow[5] )
		
		time = datetime.datetime( year, month, day, hour, minute, second )
		timestamp = time.isoformat()
		outputRow.append( timestamp )

		for headerRow in headerArray:

			index = int(headerRow[0]) - 1
			outputRow.append( dataRow[index] )

		outputArray.append( outputRow )
	
	outputFile = open( outputPath, 'w' )
	writer = csv.writer( outputFile )
	writer.writerows( outputArray )
	outputFile.close()

	
def prepare_table(inputDirectoryPath,outputDirectoryPath):
	
	tableName = os.path.basename( inputDirectoryPath )
	
	#header
	headerInputPath = os.path.join( inputDirectoryPath, '%s.head.txt' % ( tableName ) )
	inputFile = open( headerInputPath, 'r' )
	reader = csv.reader( inputFile )
	headerArray = []
	reader.next()
	for row in reader:
		headerArray.append( row )
		
	inputFile.close()

	#metadata
	inputPath = os.path.join( inputDirectoryPath, '%s.json' %( tableName ) )
	outputPath = os.path.join( outputDirectoryPath, 'metadata.json' )
	metaData = prepare_metadata(headerArray,inputPath,outputPath)

	#data
	dataInputPath = os.path.join( inputDirectoryPath, '%s.txt' %( tableName )  )
	outputPath = os.path.join( outputDirectoryPath, 'data.csv' )

	prepare_data(metaData,headerArray,dataInputPath,outputPath)

def prepare_database(inputDirectoryPath,outputDirectoryPath):

	fileNames = os.listdir( inputDirectoryPath )

	for fileName in fileNames:

		inputPath = os.path.join( inputDirectoryPath, fileName )
		if not os.path.isdir( inputPath ):
			continue
		outputPath = os.path.join( outputDirectoryPath, fileName )
		os.mkdir(outputPath)
		try:
			prepare_table(inputPath,outputPath)
		except:
			print fileName
			traceback.print_exc(sys.stderr)
			continue

if __name__ == '__main__':

	inputDirectory 		= sys.argv[1]
	outputDirectory		= sys.argv[2]

	prepare_database( inputDirectory, outputDirectory )

