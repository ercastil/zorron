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


import types
import json
import numpy
import pandas
import math
import pkg_resources 

class ZNTransformHandler:

	def __init__(self):

		self.transformVariables = None
		self.turbinePowerTransform = ZNTurbinePowerTransform()

	def handle(self,dataBase,tableCodes,variableParameters,transformParameters):

		self.transformVariables = dataBase.metaData['transform']['variables']

		transformSeriesArray = []	
		if variableParameters is None:
			return transformSeriesArray

		transformTupleArray = self._computeTransformTuples( dataBase, tableCodes, variableParameters )

		for transformTuple in transformTupleArray:

			transformId	 = transformTuple[0]
			tableCode   	 = transformTuple[1]
			variableMetaData = transformTuple[2]
			variableSet	 = transformTuple[3]
			
			tableMetaData = dataBase.metaData['tables'][tableCode]
			dataFrame  = dataBase.data.dataFrames[tableCode]

			seriesMetaData = {
						'tableCode' : tableCode,
						'tableName' : tableMetaData['name'],
						'variableType' : transformId,
						'variableStatistic' : variableMetaData[1],
						'variableAltitude' : variableMetaData[0]
					}

			seriesData = None
			if transformId == 'turbinePower':

				seriesData = self.turbinePowerTransform.transform( tableMetaData, dataFrame, variableSet, variableMetaData ,transformParameters['turbinePower'] )
				
			transformSeries = {
						'metaData' : seriesMetaData,
						'data' : seriesData
					}

			transformSeriesArray.append( transformSeries )
				

		return transformSeriesArray

	def _computeTransformTuples(self,dataBase,tableCodes,parameters):

		transformTupleArray = []

		transformIds = None
		if type(parameters['type']) == types.ListType:
			transformIds = parameters['type']
		else:
			transformIds = [ parameters['type'] ]

		for tableCode in tableCodes:

			for transformId in transformIds:

				transformVariables = self.transformVariables[transformId]

				variableMetaDataArray = dataBase.metaData['tables'][tableCode]['variables']

				variableSets = self._computeVariableSets( variableMetaDataArray, parameters, transformVariables )

				for variableMetaData,variableSet in variableSets.items():

					transformTuple = ( 
								transformId,
								tableCode,
								variableMetaData,
								variableSet
							)

					transformTupleArray.append( transformTuple )
						

		return transformTupleArray
	
	def _computeVariableSets(self,variableMetaDataArray, parameters, transformVariables ):

		variableSets = {}

		variableIndex = 0
		for variableMetaData in variableMetaDataArray:
			
			variableType = variableMetaData['type']
			if variableType in transformVariables:
				
				verified = self._verifyParameters( variableMetaData, parameters )	
				if verified:
					
					key = ( variableMetaData['altitude'] , variableMetaData['statistic'] )

					if key in variableSets:
						
						variableSets[key][variableType] = variableIndex
					else:

						variableSets[key] = { variableType : variableIndex }	
					
			variableIndex = variableIndex + 1

		return variableSets
	
	def _verifyParameters(self,variableMetaData,parameters):

		verified = True

		for key,filterValue in parameters.items():
			
			if key == 'type':
				continue
			
			variableValue = variableMetaData[key]
		
			if type(filterValue) == types.ListType:
				verified = variableValue in filterValue
			else:	
				verified = variableValue == filterValue

			if not verified:
				break

		return verified
		
class ZNTransform:

	def transform(self,tableMetaData,dataFrame,variableSet,variableMetaData, transformParameters):
		pass

class ZNTurbinePowerTransform(ZNTransform):

	def __init__(self):

		path = pkg_resources.resource_filename( 'zorron','data/turbine_models.json' ) 
		inputFile = open(path,'r')
		self.turbineModels = json.load( inputFile )
		inputFile.close()
	
	def _findTemperatureAndPressureIndices( self, variableAltitude ,tableMetaData ):

		temperatureIndex = -1
		pressureIndex 	 = -1

		temperatureDelta = float('inf')
		pressureDelta    = float('inf')

		index = 0

		for variableMetaData in tableMetaData['variables']:

			if variableMetaData['statistic'] == 'mean':
			
				if variableMetaData['type'] == 'temp':
	
					d = math.fabs( variableMetaData['altitude'] - variableAltitude )
					if d < temperatureDelta:
						temperatureDelta = d
						temperatureIndex = index
	
				elif variableMetaData['type'] == 'pres':
	
					d = math.fabs( variableMetaData['altitude'] - variableAltitude )
					if d < pressureDelta:
						pressureDelta = d
						pressureIndex = index


			index = index + 1
			
		return temperatureIndex,pressureIndex
	
	def _computeDensity(self, tableMetaData, dataFrame, variableAltitude , transformParameters ):
		density = None
		#CASE: provided by user
		if 'density' in transformParameters:

			density = transformParameters['density']

		else:
			#CASE: computed by temperature and pressure 
			temperatureIndex,pressureIndex = self._findTemperatureAndPressureIndices( variableAltitude, tableMetaData )
			if temperatureIndex >= 0 and pressureIndex >= 0:

				temperature = dataFrame[ dataFrame.columns[temperatureIndex] ].values
				pressure    = dataFrame[ dataFrame.columns[pressureIndex] ].values

				temperature = temperature + 273.5
				pressure    = 100 * pressure

				R	    = 287.058
				density     =  pressure / ( R * temperature   )

			#CASE: obtained in metadata
			elif 'meanAirDensity' in tableMetaData['info']['location']:

				density = tableMetaData['info']['location']['meanAirDensity']

			#CASE: computed by linear function
			else:
				stationElevation = tableMetaData['elevation'] / 1000.0
				density = 1.2 - stationElevation * 0.1

		return density		

	def transform(self,tableMetaData,dataFrame,variableSet,variableMetaData,transformParameters):

		#parameters
		turbineId 	= transformParameters['turbineModel']
		turbineModel 	= self.turbineModels[turbineId]

		referenceDensity = turbineModel['referenceDensity']
		series 		 = dataFrame[ dataFrame.columns[variableSet['vels']] ]
		timeIndex 	 = series.index
		windVelocity     = series.values

		#density computation
		variableAltitude = variableMetaData[0]
		density = self._computeDensity( tableMetaData, dataFrame, variableAltitude , transformParameters )
			
		#computation
		F          = ( referenceDensity / density ) ** ( 1/3 )
		curve      = turbineModel['curve']
		velocities = curve['velocity']
		powers     = curve['power']

		power = numpy.interp( windVelocity/F, numpy.asarray(velocities), numpy.asarray( powers ) )
		power[ numpy.isnan( power ) ] = 0.0

		if transformParameters['plantFactor']:
			ratedPower = turbineModel['ratedPower']
			if 'ratedPower' in transformParameters:
				ratedPower = transformParameters['ratedPower']
				power = 100 * power / ratedPower

		turbinePower = pandas.Series( index = timeIndex, data = power )

		return turbinePower
