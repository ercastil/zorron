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


import time
import datetime
import pandas
import parser
import numpy
import json
import types
import math
from dateutil import relativedelta
from dateutil.parser import parse

import zorron.util as util
import zorron.encoding as encoding

from zorron.filter import ZNTableFilter,ZNVariableFilter,ZNTimeFilter
from zorron.transform import ZNTransformHandler
from zorron.resampling import ZNResampler
from zorron.computation import ZNComputationHandler

class ZNRequestHandler:

	def __init__(self):

		self.dataRequestHandler = ZNDataRequestHandler()
		self.metaDataRequestHandler = ZNMetaDataRequestHandler()
	
	def handle(self,dataBase,request):

		if request['type'] == 'data':

			result = self.dataRequestHandler.handle( dataBase, request )

		elif request['type'] == 'metaData':

			metaData = dataBase.metaData
			result = self.metaDataRequestHandler.handle( metaData, request )

		return result


class ZNMetaDataRequestHandler:


	def handle(self,metaData,request):

		result = None
		name = request['name']			
		
		startTime = time.time()

		if name == 'allMetaData':
			result = metaData

		elif name == 'tableMetaData':

			tableCode = request['tableCode']
			result = metaData['tables'][tableCode]

		elif name == 'tableCodes':

			result = metaData['tables'].keys()

		elif name == 'variableTypeUnion':

			tableCodes = request['tableCodes']
			result = self.variableTypeUnion( metaData, tableCodes )

		elif name == 'variableTypeIntersection':

			tableCodes = request['tableCodes']
			result = self.variableTypeIntersection( metaData, tableCodes )
		
		elif name == 'variableAltitudeUnion':

			tableCodes 		= request['tableCodes']
			variableType 		= request['variableType']
			variableStatistic 	= request['variableStatistic']

			result = self.variableAltitudeUnion( metaData, tableCodes, variableType, variableStatistic )

		elif name == 'variableAltitudeIntersection':

			tableCodes 		= request['tableCodes']
			variableType 		= request['variableType']
			variableStatistic 	= request['variableStatistic']

			result = self.variableAltitudeIntersection( metaData, tableCodes, variableType, variableStatistic )
		computationTime = time.time() - startTime

		answer = {
				"computationTime" : computationTime,
				"result"          : result
			}

		return answer
	
	def variableTypeUnion(self,metaData,tableCodes):

		variableTypeUnion = set()
		for tableCode in tableCodes:
			variableMetaData = metaData['tables'][tableCode]['variables']
			for varMetaData in variableMetaData:
				variableTypeUnion.add( varMetaData['type'] )	
	
		return list(variableTypeUnion)

	def variableTypeIntersection(self,metaData,tableCodes):
		
		variableTypeSets =[]
		for tableCode in tableCodes:	
			
			variableTypeSet = set()
			variableMetaData = metaData['tables'][tableCode]['variables']
			for varMetaData in variableMetaData:
				variableTypeSet.add( varMetaData['type'] )

			variableTypeSets.append( variableTypeSet )	

		variableTypeIntersection = set.intersection( *variableTypeSets )

		return list(variableTypeIntersection)

	def variableAltitudeUnion(self,metaData,tableCodes,variableType,variableStatistic):

		variableAltitudeUnion = set()
		for tableCode in tableCodes:
			variableMetaData = metaData['tables'][tableCode]['variables']
			for varMetaData in variableMetaData:

				if varMetaData['type'] == variableType and \
				varMetaData['statistic'] == variableStatistic:
					variableAltitudeUnion.add( varMetaData['altitude'] )
					
		return list(variableAltitudeUnion)


	def variableAltitudeIntersection(self,metaData,tableCodes,variableType,variableStatistic):

		variableAltitudeSets =[]
		for tableCode in tableCodes:	
			
			variableAltitudeSet = set()
			variableMetaData = metaData['tables'][tableCode]['variables']
			for varMetaData in variableMetaData:

				if varMetaData['type'] == variableType and \
				varMetaData['statistic'] == variableStatistic:

					variableAltitudeSet.add( varMetaData['altitude'] )

			variableAltitudeSets.append( variableAltitudeSet )	

		variableAltitudeIntersection = set.intersection( *variableAltitudeSets )

		return list(variableAltitudeIntersection)

class ZNDataRequestHandler:

	def __init__(self):

		self.tableFilter 		= ZNTableFilter()
		self.variableFilter		= ZNVariableFilter()
		self.timeFilter			= ZNTimeFilter()
		self.transformHandler		= ZNTransformHandler()
		self.resampler			= ZNResampler()
		self.computationHandler 	= ZNComputationHandler()

		self.transforms = None
	
	def handle(self,dataBase,request):

		metaData = dataBase.metaData
		
		#set transforms
		self.transforms = metaData['transform']['variables'].keys()

		answer = {}

		startTime = time.time()

		#FILTER BY TABLE
		tableParameters = None
		if 'table' in request:
			tableParameters = request['table']
		tableCodes = self.tableFilter.filter( metaData, tableParameters )

		#SPLIT VARIABLE PARAMETERS BY TYPE
		originalVariableParameters  = None
		transformVariableParameters = None
		if 'variable' in request:
			 originalVariableParameters, transformVariableParameters = self._splitVariableParameters( request['variable'] )

		#FILTER BY VARIABLE
		idTuples = self.variableFilter.filter( metaData, tableCodes, originalVariableParameters )
		variableSeriesArray = self._extractSeries( dataBase, idTuples )

		#PERFORM TRANSFORM
		transformParameters = None
		if 'transform' in request:
			transformParameters = request['transform']
		transformSeriesArray = self.transformHandler.handle( dataBase, tableCodes, transformVariableParameters, transformParameters  )

		#MERGE SERIES
		seriesArray = variableSeriesArray + transformSeriesArray

		#CALCULATE TIME BOUNDS
		timeBounds = self._calculateTimeBounds( seriesArray, request )

		#CALCULATE TIME INTERVAL
		timeInterval = self._calculateTimeInterval( seriesArray, timeBounds, request )
		answer['interval'] = timeInterval

		#ROUND TIME BOUNDS
		if timeInterval is not None:
			if timeInterval['code'] != 'C':
 				timeBounds = self._roundTimeBounds( timeBounds, timeInterval )

		#ITERATE 
		seriesResults = []
		for series in seriesArray:

			seriesMetaData = series['metaData']
			seriesData   = series['data']
			
			#FILL MISSING DATA
			seriesData = self._fillMissingData( seriesData, timeBounds, timeInterval )
			
			#FILTER BY TIME
			timeParameters = None
			if 'time' in request:
				timeParameters = request['time']
			seriesData = self.timeFilter.filter( seriesData, timeParameters )

			if len( seriesData ) > 0:
				
				seriesMetaData['firstTimeStamp'] = seriesData.index[0]
				seriesMetaData['lastTimeStamp']  = seriesData.index[-1]
 				series = {
						'metaData' : seriesMetaData,
						'data' : seriesData
					}

				seriesResults.append( series )

		#RESAMPLING
		if 'resampling' in request:
			resamplingResult = self.resampler.resample( seriesResults, request['resampling'] , timeInterval )
			seriesResults = resamplingResult
			
		#PERFORM COMPUTATION
		if 'computation' in request:
			computationParameters = request['computation']
			seriesResults = self.computationHandler.handle( seriesResults, computationParameters )

		computationTime = time.time() - startTime

		answer['result']          = seriesResults
		answer['computationTime'] = computationTime

		return answer
	
	def _calculateTimeBounds( self, seriesArray, request ):
		
		#trivial case
		if len(seriesArray) == 0:
			return None

		#compute bounds time bounds from series array
		lowerBounds = []
		upperBounds = []

		for series in seriesArray:
			lowerBounds.append( series['data'].index[0] )
			upperBounds.append( series['data'].index[-1] )

		lowerBound = min( lowerBounds )
		upperBound = max( upperBounds )

		#set time bounds from request
		if 'time' in request:

			#if 'offset' in request['time']:

			#	offset = datetime.timedelta(seconds=request['time']['offset']
			#	upperBound = datetime.datetime.now()
			#	lowerBound = upperBound - offset
				
			if 'lowerBound' in request['time']:
				lowerBound = parse( request['time']['lowerBound'] )
				lowerBound = datetime.datetime( 
								lowerBound.year ,
								lowerBound.month,
								lowerBound.day,
								lowerBound.hour,
								lowerBound.minute,
								lowerBound.second
							)
			

			if 'upperBound' in request['time']:
				upperBound = parse( request['time']['upperBound'] )
				upperBound = datetime.datetime( 
								upperBound.year ,
								upperBound.month,
								upperBound.day,
								upperBound.hour,
								upperBound.minute,
								upperBound.second
							)
		
		
		return [ lowerBound, upperBound ]
	
	def _calculateTimeInterval( self, seriesArray, timeBounds, request ):

		#trivial case
		timeInterval = None
		if len(seriesArray) == 0:
			return timeInterval

		#without resampling
		timeInterval = { "code" : "C" }

		#with resampling
		if 'resampling' in request:

			resamplingParameters = request['resampling']
			
			#fixed interval
			if resamplingParameters['interval']['type'] == 'fixed':
				timeInterval = resamplingParameters['interval']

			#adaptive interval
			else:
				timeInterval = self.resampler.calculateAdaptiveInterval( seriesArray, timeBounds, resamplingParameters['interval']['seriesLength'] )

		return timeInterval
	
	def _roundTimeBounds(self, timeBounds, timeInterval ):

		if timeBounds is None or timeInterval['code'] == 'C':
			return timeBounds
		
		lowerBound = timeBounds[0]
		upperBound = timeBounds[1]
	
		timeUnits = timeInterval['units']
		timeCode  = timeInterval['code']

		minute = 0
		second = 0
		
		if timeCode == 'MS':

			#at the beginning of the first month
			day = 1
			hour = 0

			lowerBound = datetime.datetime( 
							lowerBound.year, 
							lowerBound.month,
							day,
							hour,
							minute,
							second
						       )

			deltaTime = relativedelta.relativedelta( upperBound, lowerBound )
			numberMonths = deltaTime.years * 12 + deltaTime.months
			if deltaTime.days > 0 or deltaTime.hours > 0 or deltaTime.minutes > 0:
				numberMonths = numberMonths + 0.3
			numberIntervals = numberMonths / timeUnits
			totalMonths = int( math.ceil( numberIntervals ) * timeUnits )
			newDeltaTime = relativedelta.relativedelta( months=totalMonths )
			upperBound = lowerBound + newDeltaTime

			timeBounds = [ lowerBound, upperBound ]
			return timeBounds

		if timeCode == 'A':

			#at the beginning of the first year
			month = 1
			day = 1
			hour = 0
			
			lowerBound = datetime.datetime( 
							lowerBound.year, 
							month,
							day,
							hour,
							minute,
							second
						       )

			deltaTime = relativedelta.relativedelta( upperBound, lowerBound )
			numberYears = deltaTime.years + deltaTime.months / 12.0 + deltaTime.days / 365.0 + deltaTime.hours / 8760.0
			numberIntervals = numberYears / timeUnits
			totalYears = int( math.ceil( numberIntervals ) * timeUnits )
			newDeltaTime = relativedelta.relativedelta( years = totalYears )
			upperBound = lowerBound + newDeltaTime
		
			timeBounds = [ lowerBound, upperBound ]
			return timeBounds

		if timeCode == 'Min':
			
			#at the beginning of the first hour
			lowerBound = datetime.datetime( 
							lowerBound.year, 
							lowerBound.month,
							lowerBound.day,
							lowerBound.hour,
							minute,
							second
						       )

			
			deltaTime = upperBound - lowerBound
			intervalSeconds = timeUnits * 60 
			

		elif timeCode == 'H':

			#at the beginning of the first hour
			lowerBound = datetime.datetime( 
							lowerBound.year, 
							lowerBound.month,
							lowerBound.day,
							lowerBound.hour,
							minute,
							second
						       )

			deltaTime = upperBound - lowerBound
			intervalSeconds = timeUnits * 3600

		elif timeCode == 'D':

			#at the beginning of the first day
			hour = 0
			lowerBound = datetime.datetime( 
							lowerBound.year, 
							lowerBound.month,
							lowerBound.day,
							hour,
							minute,
							second
						       )

			deltaTime = upperBound - lowerBound
			intervalSeconds = timeUnits * 86400

		elif timeCode == 'W':
			
			#at the beginning of the first week
			hour = 0
			lowerBound = datetime.datetime( 
							lowerBound.year, 
							lowerBound.month,
							lowerBound.day,
							hour,
							minute,
							second
						       )
			weekday = lowerBound.weekday()
			numberDays = weekday % 7
			deltaTime = datetime.timedelta( numberDays )
			lowerBound = lowerBound - deltaTime
			deltaTime = upperBound - lowerBound
			intervalSeconds = timeUnits * 604800

		numberIntervals = deltaTime.total_seconds() / float( intervalSeconds )
		totalSeconds = math.ceil( numberIntervals ) * intervalSeconds
		newDeltaTime = datetime.timedelta(0,totalSeconds)
		upperBound = lowerBound + newDeltaTime

		timeBounds = [ lowerBound, upperBound ]

		return timeBounds
	
	def _splitVariableParameters(self,parameters):
		
		originalVariableParameters = {}
		transformVariableParameters = None

		#case : type
		if 'type' in parameters:

			#split types
			variableTypes = None
			if type(parameters['type']) == types.ListType:
				variableTypes = parameters['type']
			else:
				variableTypes = [ parameters['type'] ]

			del parameters['type']

			originalVariableTypes = []
			transformVariableTypes = []
			for variableType in variableTypes:
				if variableType in self.transforms:
					transformVariableTypes.append( variableType )
				else:
					originalVariableTypes.append( variableType )
				
			originalVarLen = len(originalVariableTypes)
			transformVarLen = len(transformVariableTypes)

			#case: only original variables
			if  originalVarLen > 0 and transformVarLen == 0:
			
				originalVariableParameters = parameters.copy()
				originalVariableParameters['type'] = originalVariableTypes
			
			#case: only transform variables
			elif originalVarLen == 0 and transformVarLen > 0:

				transformVariableParameters = parameters.copy()
				transformVariableParameters['type'] = transformVariableTypes
			#case: both
			elif originalVarLen > 0 and transformVarLen > 0:

				originalVariableParameters = parameters.copy()
				originalVariableParameters['type'] = originalVariableTypes

				transformVariableParameters = parameters.copy()
				transformVariableParameters['type'] = transformVariableTypes

		#NO TYPE
		else:
			originalVariableParameters = parameters.copy()

		return originalVariableParameters,transformVariableParameters
	
	def _fillMissingData(self,series, timeBounds, timeInterval ):

		samplingFrequency = util.findSamplingFrequency( series )	
		if samplingFrequency is None:
			newSeries = series
		else:
			#filter by time bounds
			filteredSeries = series[ timeBounds[0] : timeBounds[1] ]
			
			if len( filteredSeries ) > 0:
				
				startTime = filteredSeries.index[0]
				endTime   = filteredSeries.index[-1]

				if timeInterval['code'] != 'C':

					#augment series time span

					#start 
					deltaTime = filteredSeries.index[0] - timeBounds[0]
					numberIntervals = deltaTime.total_seconds() / samplingFrequency
					seconds = math.floor( numberIntervals ) * samplingFrequency
					deltaTime = datetime.timedelta(0,seconds)
					startTime = filteredSeries.index[0] - deltaTime

					#end
					deltaTime = timeBounds[1] - filteredSeries.index[-1]
					numberIntervals = deltaTime.total_seconds() / samplingFrequency
					seconds = math.floor( numberIntervals ) * samplingFrequency
					deltaTime = datetime.timedelta(0,seconds)
					endTime   = filteredSeries.index[-1] + deltaTime
					deltaTime = datetime.timedelta(0,samplingFrequency)
					endTime   = endTime - deltaTime

				#reindex series
				newTimeIndex = pandas.tseries.index.date_range( start=startTime, end=endTime, freq='%dS' % ( samplingFrequency ) )
				newSeries = filteredSeries.reindex( newTimeIndex )
			else:
				newSeries = filteredSeries
			
		return newSeries

	def _extractSeries( self, dataBase, idTuples ):
		
		seriesArray = []
		for idTuple in idTuples:

			tableCode = idTuple[0]
			tableName = dataBase.metaData['tables'][tableCode]['name']
			variablesIndexArray = idTuple[1]
			
			variableMetaDataArray = dataBase.metaData['tables'][tableCode]['variables']
			dataFrame = dataBase.data.dataFrames[tableCode]

			if len(dataFrame) == 0:
				continue

			for k in range( len(variablesIndexArray) ):
				
				variableIndex = variablesIndexArray[k]
				variableMetaData = variableMetaDataArray[variableIndex]

				seriesMetaData = {
							'tableCode' : tableCode,
							'tableName' : tableName,
							'variableType' : variableMetaData['type'],
							'variableStatistic' : variableMetaData['statistic'],
							'variableAltitude' : variableMetaData['altitude'],
						 }

				seriesData = dataFrame[dataFrame.columns[variableIndex]]
				if len(seriesData) > 0:

					seriesEntry = { 
							'metaData' : seriesMetaData,
							'data' : seriesData
							}

					seriesArray.append( seriesEntry )
		
		return seriesArray
