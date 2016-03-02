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


from dateutil.parser import parse
import calendar
import parser
import time
import numpy
import datetime
import math
import pandas
import os
import re

class ZNFileRemover:

	def __init__(self,directoryPath, pattern, timeInterval):

		self.directoryPath = directoryPath
		self.pattern	   = pattern
		self.timeInterval  = timeInterval

	def remove(self):
		
		fileNames = os.listdir(self.directoryPath)
		now = datetime.datetime.now()
		for fileName in fileNames:
			if fileName.find( self.pattern ) >= 0:
				filePath = os.path.join( self.directoryPath, fileName )
				timeStamp = os.path.getctime( filePath )
				creationDateTime = datetime.datetime.fromtimestamp( timeStamp )
				deltaTime = now - creationDateTime
				if deltaTime > self.timeInterval:
					os.remove( filePath )

def nanoseconds2excel(nanoseconds):

	return nanoseconds / 1e9 / 86400.0 + 25569.0

def timestamp2excel(timestamp):

	nanoseconds = ( calendar.timegm( timestamp.timetuple() ) + 1e-6 * timestamp.microsecond ) * 1e9
	return nanoseconds2excel( nanoseconds )

def findSamplingFrequency(dataFrame):

	#find sampling frequency
	index = dataFrame.index.astype(numpy.int64)
	if len(index) == 1:
        	return None
    	deltas= index[1:] - index[:-1]
	samplingFrequencySeconds = (int) ( numpy.median( deltas ) / 1e9 )

	return samplingFrequencySeconds

def parseStatisticFunction(functionParameters):

	name = functionParameters['name']

	if name == 'percentile':

		level = functionParameters['level']
		function = lambda x : numpy.percentile( x, level )	

	elif name == 'count':
		
		function = lambda x : x.count()
	
	elif name == 'fraction':

		function = lambda x : x.count() / float(len(x)) * 100

	else:
		functionName = 'numpy.%s' % ( name )
		code = parser.expr( functionName ).compile()
		function = eval(code)

	return function	

def createDataFrame(dataArray,variableNames=None ):

	timeArray = dataArray[:,0].astype(numpy.int64)
	indexArray = pandas.DatetimeIndex( data=timeArray )
	valuesArray = dataArray[:,1:]
	dataFrame = pandas.DataFrame( index=indexArray, data=valuesArray, dtype=float )
	if variableNames is not None:
		dataFrame.columns = variableNames

	return dataFrame


def extractDataArray(dataFrame):

	timeStampArray = dataFrame.index.astype(numpy.int64)
	indexArray = numpy.array( [ timeStampArray ],dtype=numpy.int64 )
	valuesArray = dataFrame.values
	if len(valuesArray.shape) == 1:
		valuesArray = numpy.array( [ valuesArray ]).T
	
 	dataArray = numpy.concatenate( ( indexArray.T, valuesArray ), axis = 1)

	return dataArray

def selectValidValues(dataFrame):

	if len(dataFrame)==1:
		return dataFrame.values

	nan = float(dataFrame)
	values = [nan] * len(dataFrame.columns)

	i = 0
	for k,column in dataFrame.iteritems():
		for value in column:
			if value != nan:
				values[i] = value
		i = i + 1
		
	return values

def resolveDuplicatedEntries(dataArray):
	
	#create dataframe
	dataFrame = createDataFrame(dataArray)

	#group by index
	#TODO:select valid values
	grouped = dataFrame.groupby(dataFrame.index).first()

	#extract data array
	newDataArray = extractDataArray(grouped)

	return newDataArray

def replaceNan(dataArray):

	#replace nan for none
	numberColumns = len(dataArray[0])
	numberRows = len(dataArray)

	for r in range(numberRows):
		for c in range(1,numberColumns):
			if math.isnan( dataArray[r][c] ):
				dataArray[r][c] = None
