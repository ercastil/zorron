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


import numpy
import pandas
import parser
import sys

import zorron.util as util

class ZNComputationHandler:

	def __init__(self):

		self.aggregated	            = ZNAggregatedComputation()
		self.histogram 		    = ZNHistogramComputation()
		self.windRose		    = ZNWindRoseComputation()
		self.cumulativeDistribution = ZNCumulativeDistributionComputation()
		self.annualCycle	    = ZNAnnualCycleComputation()
		self.dailyCycle 	    = ZNDailyCycleComputation()
		self.annualDailyCycle	    = ZNAnnualDailyCycleComputation()
		self.standardSummary	    = ZNStandardSummaryComputation()
		self.genericExpression	    = ZNGenericExpressionComputation()
	
	def handle(self, series, parameters ):
		
		result = None
		if len(series)==0:
			return result

		name = parameters['name']

		if  name == 'aggregated':
			result = self.aggregated.compute( series, parameters )
		elif name == 'histogram':
			result = self.histogram.compute( series, parameters )	
		elif name == 'windRose':
			result = self.windRose.compute( series, parameters )	
		elif name == 'cumulativeDistribution':
			result = self.cumulativeDistribution.compute( series, parameters )
		elif name == 'annualCycle':
			result = self.annualCycle.compute( series, parameters )
		elif name == 'dailyCycle':
			result = self.dailyCycle.compute( series, parameters )
		elif name == 'annualDailyCycle':
			result = self.annualDailyCycle.compute( series, parameters )
		elif name == 'standardSummary':
			result = self.standardSummary.compute( series, parameters )
		elif name == 'genericExpression':
			result = self.genericExpression.compute( series, parameters )

		return result

class ZNComputation:

	def compute(self,seriesArray,parameters):

		self._processSharedData( seriesArray, parameters )

		result = []
		for series in seriesArray:
			if series is None:
				result.append( None )
				continue

			computation = self._compute( series['data'], parameters )
			seriesResult = {
				  	'metaData' : series['metaData'],
				  	'data' : computation
					}
			result.append( seriesResult )

		return result
	
	def _compute(self,serie,parameters):
		pass
	
	def _processSharedData(self,seriesArray,parameters):
		pass

class ZNHistogramComputation(ZNComputation):

	def _processSharedData(self,seriesArray,parameters):

		if 'min' in parameters and 'max' in parameters:
			return

		minimum = sys.float_info.max
		maximum = sys.float_info.min

		for series in seriesArray:
			
			min = series['data'].min()
			if min < minimum:
				minimum = min
			max = series['data'].max()
			if max > maximum:
				maximum = max
				
		if 'min' not in parameters:
			parameters['min'] = minimum

		if 'max' not in parameters:
			parameters['max'] = maximum

	def _compute(self,series,parameters):
		
		result = []

		a = parameters['min']
		b = parameters['max']

		if a >= b:
			return result

		series = series.dropna()
		if len(series) == 0:
			return result
		
		(histogram,bins) = numpy.histogram( series.values,range=(a,b),bins=parameters['bins'],density=parameters['density'])

		result = []
		for i in range(len(histogram)):
			label = [ bins[i] , bins[i+1] ]
			result.append( [ label, float( histogram[i] ) ] )	

		return result

class ZNAggregatedComputation(ZNComputation):

	def compute(self,seriesInputArray,parameters):
		
		seriesArray = []
		count = 0
		for seriesInput in seriesInputArray:
			if seriesInput['data'] is None:
				continue
			name = 'v%d' % ( count ) 
			seriesInput['data'].name = name
			seriesArray.append( seriesInput['data'] )
			count = count + 1
			
		dataFrame = pandas.concat( seriesArray, axis=1 )

		function = util.parseStatisticFunction( parameters['statistic'] )
		series = dataFrame.apply( function , axis=1)

		result = {
			   'type' : 'aggregated',
			   'data': series
			  }

		return result


class ZNCumulativeDistributionComputation(ZNComputation):

	def _processSharedData(self,seriesArray,parameters):

		if 'min' in parameters and 'max' in parameters:
			return

		minimum = sys.float_info.max
		maximum = sys.float_info.min

		for series in seriesArray:
			
			min = series['data'].min()
			if min < minimum:
				minimum = min
			max = series['data'].max()
			if max > maximum:
				maximum = max
				
		if 'min' not in parameters:
			parameters['min'] = minimum

		if 'max' not in parameters:
			parameters['max'] = maximum

	def _compute(self,series,parameters):

		series = series.dropna()

		a = parameters['min']
		b = parameters['max']
		
		(histogram,bins) = numpy.histogram( series.values,range=(a,b),bins=parameters['bins'])
		cumulativeSum = numpy.cumsum( histogram )

		result = []
		for i in range(len(cumulativeSum)):
			label = [ bins[i] , bins[i+1] ]
			result.append( [ label, cumulativeSum[i] ] )	

		return result

class ZNAnnualCycleComputation(ZNComputation):

	def _compute(self,series,parameters):
		
		series = series.dropna()
		grouped = series.groupby( lambda x : x.month )
		statisticFunction = util.parseStatisticFunction( parameters['statistic'] )
		series = grouped.aggregate( statisticFunction )
		
		#fill missing values
		months = range(1,13)
		missingMonths = list( set(months) - set(series.index.tolist()) )
		numberMissingMonths = len(missingMonths)
		if  numberMissingMonths > 0:
			values = [float('nan')] * numberMissingMonths
			temporalSeries = pandas.Series( data=values, index=missingMonths )	
			series = series.append( temporalSeries ).sort_index()

		return series
		

class ZNDailyCycleComputation(ZNComputation):

	def _compute(self,series,parameters):

		series = series.dropna()
		grouped = series.groupby( lambda x : x.hour )
		statisticFunction = util.parseStatisticFunction( parameters['statistic'] )
		series = grouped.aggregate( statisticFunction )
		
		#fill missing values
		hours = range(0,24)
		missingHours = list( set(hours) - set(series.index.tolist()) )
		numberMissingHours = len(missingHours)
		if  numberMissingHours > 0:
			values = [float('nan')] * numberMissingHours
			temporalSeries = pandas.Series( data=values, index=missingHours )	
			series = series.append( temporalSeries ).sort_index()

		return series

class ZNAnnualDailyCycleComputation(ZNComputation):
	
	def _compute(self,series,parameters):
		
		series = series.dropna()
		grouped = series.groupby( lambda x : (x.month,x.hour) )
		statisticFunction = util.parseStatisticFunction( parameters['statistic'] )
		series = grouped.aggregate( statisticFunction )
		
		result = []
		index = series.index
		values = series.values
		for i in range(len(series)):
			result.append( [index[i],values[i]] )

		return result

class ZNGenericExpressionComputation(ZNComputation):

	def _compute(self,series,parameters):
		
		expression = parameters['expression']
		variableDictionary = locals()
		variableDictionary['x'] = series
		code = parser.expr( expression ).compile()
		series = eval(code)

		return series

class ZNWindRoseComputation(ZNComputation):

	def compute(self,seriesArray,parameters):

		#find (direction,velocity) pairs
		pairs = []
		dictionary = {}
		for series in seriesArray:
			metaData = series['metaData']
			key = ( 
					metaData['tableCode'],
					metaData['variableAltitude'],
					metaData['variableStatistic']
				      )
			if metaData['variableType'] == 'vels':
				if key in dictionary:
					direction = dictionary[key]
					pairs.append( [direction,series] )
					del dictionary[key]
				else:
					dictionary[key]  = series
			elif metaData['variableType'] == 'dirv':
				if key in dictionary:
					velocity = dictionary[key]
					pairs.append( [series,velocity] )
					del dictionary[key]
				else:
					dictionary[key]  = series

		#compute histogram2d
		result = []
		for pair in pairs:
			computation = self._compute( pair, parameters )
			metaData = pair[0]['metaData'].copy()
			metaData['variableType'] = 'windRose'
			seriesResult = {
					'metaData' : metaData,
					'data' : computation
				       }

			result.append( seriesResult )

		return result
			
	def _compute(self,seriesPair,parameters):

		direction 	= seriesPair[0]['data'].values
		directionBins 	= parameters['directionBins']

		delta = 360 / ( 2 * directionBins )
		direction = ( direction + delta ) % 360

		velocity 	= seriesPair[1]['data'].values
		velocityBins 	= parameters['velocityBins']

		directionMin 	= 0
		directionMax	= 360

		if 'velocityMin' in parameters:
			velocityMin = parameters['velocityMin']
		else:
			velocityMin	= numpy.nanmin( velocity )

		if 'velocityMax' in parameters:
			velocityMax = parameters['velocityMax']
		else:
			velocityMax	= numpy.nanmax( velocity )

		density = True
		if 'density' in parameters:
			density = parameters['density']

		histogram = numpy.histogram2d( 
						direction, 
						velocity, 
						bins= [ directionBins, velocityBins ],
						range = [ [ directionMin, directionMax ],
						          [ velocityMin, velocityMax ]
							],
						normed = density
					)

		directionBinEdges = histogram[1]
		directionBinEdges = ( directionBinEdges - delta ) % 360

		result = {
				'histogram' : histogram[0].transpose(),
				'directionBinEdges' : directionBinEdges,
				'velocityBinEdges' : histogram[2]
			 }

		return result

class ZNStandardSummaryComputation(ZNComputation):

	def _compute(self,series,parameters):

		timeIndex = series.index.astype( numpy.int64 )
		start		= timeIndex[0]
		end		= timeIndex[-1]
		min		= series.min()
		max		= series.max()
		mean		= series.mean() 
		stdev		= series.std()
		fraction 	= series.count() / float(len(series))

		result = {
			   'start' : start,
			   'end' : end,
			   'min' : min,
			   'max' : max,
			   'mean' : mean,
			   'stdev' : stdev,
			   'fraction' : fraction
			}
		
		return  result
