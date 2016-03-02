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


import os
import sys
import numpy
import gc

import itertools
import zorron.util as util
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

class ZNChartCreatorHandler:

	def __init__(self):

		self.seriesChartCreator 	= ZNSeriesChartCreator()
		self.fractionChartCreator 	= ZNFractionChartCreator()
		self.histogramChartCreator 	= ZNHistogramChartCreator()
		self.periodCycleChartCreator 	= ZNPeriodCycleChartCreator()
		self.heatMapChartChartCreator 	= ZNHeatMapChartCreator()

	def create(self,chart,parameters):

		figure = None

		if 'error' in parameters:
			return figure

		parameters = parameters['result']

		if chart == 'series':
			figure = self.seriesChartCreator.create(parameters)

		elif chart == 'fraction':
			figure = self.fractionChartCreator.create(parameters)

		elif chart == 'histogram':
			figure = self.histogramChartCreator.create(parameters)

		elif chart == 'dailyCycle' or chart == 'annualCycle':
			figure = self.periodCycleChartCreator.create(parameters)

		elif chart == 'heatMap':
			figure = self.heatMapChartCreator.create(parameters)

		return figure
		

class ZNSeriesChartCreator:

	def create(self,parameters):
		
		figure = plt.figure()

		years = mdates.YearLocator()
		months = mdates.MonthLocator()
		yearsFormat = mdates.DateFormatter('%Y')

		figure.gca().xaxis.set_major_locator(years)
		figure.gca().xaxis.set_major_formatter(yearsFormat)
		figure.gca().xaxis.set_minor_locator(months)

		for param in parameters:
			
			array = numpy.array( param['data'] )
			x = array[:,0]
			y = array[:,1]
			dates = util.timeStampToDateTimeArray(x/1000)
				
			plt.plot( dates, y,label="%s" %( param['tableCode']) )
			plt.legend()
			

		return figure

class ZNFractionChartCreator:

	def create(self,parameters):

		figure = plt.figure()
		
		years = mdates.YearLocator()
		months = mdates.MonthLocator()
		yearsFormat = mdates.DateFormatter('%Y')

		figure.gca().xaxis.set_major_locator(years)
		figure.gca().xaxis.set_major_formatter(yearsFormat)
		figure.gca().xaxis.set_minor_locator(months)

		for param in parameters:
			
			array = numpy.array( param['data'] )
			x = array[:,0]
			y = array[:,1]
			dates = util.timeStampToDateTimeArray(x/1000)
				
			plt.plot( dates, y )

		return figure


class ZNHistogramChartCreator:

	def create(self,parameters):
		
		figure = plt.figure()
		for param in parameters:
		
			array = numpy.array( param['data'] )
			x = array[:,0]
			y = array[:,1]
			
			x = [ a[0] for a in x]
			plt.plot(x,y,label="%s" %( param['tableCode']))
			plt.legend()
		
		return figure
	
class ZNPeriodCycleChartCreator:

	def create(self,parameters):

		figure = plt.figure()

		for param in parameters:

			array = numpy.array( param['data'] )
			x = array[:,0]
			y = array[:,1]

			plt.plot(x,y,label="%s" %( param['tableCode']))
			plt.legend()
		
		return figure

class ZNHeatMapChartCreator:

	def create(self,parameters):
		return figure

class ZNChartGenerator:
    
	def __init__(self):
		
		self.chartCreatorHandler = ZNChartCreatorHandler()

	def generate(self,client,setSize,interval,variableStatistic,chartType,outputDirectory):

		count = 0
		
		#get metadata
		client.connect()
		metaData = client.getMetaData()
		client.close()
		tablesMetaData = metaData['tables']

		#table combinations
		tablesCodes = tablesMetaData.keys()
		tableCodeCombinations = self._generateTableCodeCombinations( tablesCodes, setSize )
		for tableCodeCombination in tableCodeCombinations:
			
			#common variables
			commonVariablesGroups = self._generateCommonVariablesGroups(tablesMetaData,tableCodeCombination,variableStatistic)

			for commonVariablesGroup in commonVariablesGroups:

				self._generateChart( client, commonVariablesGroup, interval, variableStatistic,chartType,outputDirectory)

				count = count + 1

	
	def _generateChart(self,client,commonVariablesGroup,interval,variableStatistic,chartType,outputDirectory):

		#daily cycle
		if chartType == 'dailyCycle' and \
		( interval != 'C' and interval != 'H' ):
			return

		#annual cycle
		if chartType == 'annualCycle' and interval == 'A':
			return

		#request data
		request = self._buildUpRequest( commonVariablesGroup, interval, chartType )
		client.connect()
		answer = client.requestData( request )
		client.close()

		#plot chart
		figure = self.chartCreatorHandler.create( chartType, answer )
		if figure is None:
			print commonVariablesGroup 
			return

		variableType = request['variable']['type']
		variableAltitude = request['variable']['altitude']
		fileName = '%s_%s_%s_%f.png' % ( commonVariablesGroup['tablesCodes'], variableType, variableStatistic,variableAltitude )
		outputPath = os.path.join( outputDirectory, fileName )
		try:
			figure.savefig( outputPath, format='png' )
			figure.clf()
			plt.close()
		except:
			return

	
	def _generateTableCodeCombinations(self,tablesCodes,setSize):

		tableCodeCombinations = itertools.combinations( tablesCodes, setSize )
		
		return tableCodeCombinations
	
	def _generateCommonVariablesGroups(self,tablesMetaData,tableCodeCombination,variableStatistic):
		commonVariablesGroups = []

		#variable type intersection
		variableTypeIntersection = self._findVariableTypeIntersection( tablesMetaData, tableCodeCombination, variableStatistic )
	
		#variable altitude intersection
		for variableType in variableTypeIntersection:
		
			variableAltitudeIntersection = self._findVariableAltitudeIntersection( tablesMetaData, tableCodeCombination,variableType, variableStatistic)	

			#create groups
			for variableAltitude in variableAltitudeIntersection:
				
				variableGroup = {
						  'tablesCodes' : list(tableCodeCombination),
						  'variableType' : variableType,
						  'variableStatistic' : variableStatistic,
						  'variableAltitude' : variableAltitude
						}

				commonVariablesGroups.append( variableGroup )
	
		return commonVariablesGroups
	
	def _findVariableTypeIntersection(self,tablesMetaData,tableCodeCombination,variableStatistic):
		variableTypeSets =[]
		for tableCode in tableCodeCombination:	
			
			variableTypeSet = set()
			variableMetaData = tablesMetaData[tableCode]['variables']
			for varMetaData in variableMetaData:
				if varMetaData['statistic'] == variableStatistic:
					variableTypeSet.add( varMetaData['type'] )

			variableTypeSets.append( variableTypeSet )	

		variableTypeIntersection = set.intersection( *variableTypeSets )

		return variableTypeIntersection
	
	def _findVariableAltitudeIntersection(self,tablesMetaData,tableCodeCombination,variableType,variableStatistic):

		variableAltitudeSets =[]
		for tableCode in tableCodeCombination:	
			
			variableAltitudeSet = set()
			variableMetaData = tablesMetaData[tableCode]['variables']
			for varMetaData in variableMetaData:
				if varMetaData['statistic'] == variableStatistic and \
				varMetaData['type'] == variableType:
					variableAltitudeSet.add( varMetaData['altitude'] )

			variableAltitudeSets.append( variableAltitudeSet )	

		variableAltitudeIntersection = set.intersection( *variableAltitudeSets )
		return variableAltitudeIntersection

	def _buildUpRequest(self,commonVariablesGroup,interval,chartType):

		request = {
				'type' : 'data',
				'table' : { 
						'code' : commonVariablesGroup['tablesCodes'] 
					},
				'variable' : {  
						'statistic' : commonVariablesGroup['variableStatistic'],
                        'altitude' : commonVariablesGroup['variableAltitude'],
						'type' : commonVariablesGroup['variableType']
					}
			}

		if interval != 'C':

			request['resampling'] =  {
						 'interval' : interval,
						 'statistic' : { 'name' : 'mean' }
						}

	
		if chartType == 'histogram':
			
			request['computation'] = {
						   'name' : 'histogram',
						   'bins' : 100
						 }

		elif chartType == 'dailyCycle':

			request['computation'] = {
						   'name' : 'dailyCycle',
						   'statistic': { 'name' : 'mean' }
						 }


		elif chartType == 'annualCycle':

			request['computation'] = {
						   'name' : 'annualCycle',
						   'statistic': { 'name' : 'mean' }
						 }

		
		return request
