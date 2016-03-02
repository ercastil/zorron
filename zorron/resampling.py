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


import zorron.util as util

class ZNResampler:

	def __init__(self):

		self._defineStandardIntervals()
	
	def _defineStandardIntervals(self):

		self.standardIntervals = []

		minute = 60
		hour   = minute * 60
		day    = hour * 24
		week   = day * 7
		month  = week * 4.5
		
		#10-20-30 minutes
		for i in range(1,4):
			seconds = i * 10 * minute
			interval = { 'units' :  i * 10, 'code' : 'Min' }
			self.standardIntervals.append( (seconds,interval) )

		#hours
		hours = [ 1,2,3,4,6,12]
		for i in hours:
			seconds = i * hour
			interval 	= { 'units': i, 'code' : 'H' }
			self.standardIntervals.append( (seconds,interval) )

		#days
		days = [ 1,2,3,4,5]
		for i in days:
			seconds = i * day
			interval 	= { 'units' : i, 'code' : 'D' }
			self.standardIntervals.append( (seconds,interval) )

		#weeks
		weeks = [ 1,2 ]
		for i in weeks:
			seconds = i * week
			interval 	= { 'units' : i, 'code' : 'W' }
			self.standardIntervals.append( (seconds,interval) )

		#months
		months = [ 1,2,3,4,6]
		for i in months:
			seconds = i * month
			interval 	= { 'units' : i, 'code' : 'MS' }
			self.standardIntervals.append( (seconds,interval) )

		#year
		seconds = 12 * month
		interval    = { 'units' : 1, 'code' : 'A' }
		self.standardIntervals.append( (seconds,interval) )

	def resample( self,seriesArray,resamplingParameters, timeInterval ):
		
		resampledSeriesArray = []
		if len(seriesArray) == 0:
			return resampledSeriesArray

		#statistic
		resamplingFunction = util.parseStatisticFunction(resamplingParameters['statistic'])

		#resample	
		resampledSeriesArray = []
		for series in seriesArray:
			timeIntervalString = '%d%s' % ( timeInterval['units'], timeInterval['code'] )
			resampledSeriesData = series['data'].resample( timeIntervalString, how=resamplingFunction )
			if len(resampledSeriesData) > 0:
				resampledSeries = {
							'metaData' 	: series['metaData'],
							'data'  : resampledSeriesData
						}
				resampledSeriesArray.append( resampledSeries )

		#fraction filter
		if 'minimumFraction' in resamplingParameters and  resamplingParameters['statistic']['name'] != 'fraction':
			self._filterByFraction( seriesArray,resampledSeriesArray, timeIntervalString, resamplingParameters['minimumFraction'] )

		return resampledSeriesArray
	
	def _filterByFraction(self,seriesArray,resampledSeriesArray ,interval,minimumFraction):

		#compute fraction
		parameters = { 'name' : 'fraction'}
		fractionFunction = util.parseStatisticFunction(parameters)

		fractionSeriesArray = []  
		for series in seriesArray:
			fractionSeries = series['data'].resample( interval, how=fractionFunction )
			fractionSeriesArray.append( fractionSeries )

		#filter by minimum fraction
		nan = float('nan')
		for i in range(len(seriesArray)):

			fractionValues  = fractionSeriesArray[i].values

			for k in range(len(fractionValues)):

				if fractionValues[k] < minimumFraction:
					resampledSeriesArray[i]['data'].values[k] = nan
	
		return

	def calculateAdaptiveInterval( self, seriesArray, timeBounds, seriesLength ):

		interval = None
		
		#compute maximum sampling frequency
		samplingFrequencies = []
		for series in seriesArray:
			seriesData = series['data']
			samplingFrequency = util.findSamplingFrequency( seriesData )
			samplingFrequencies.append( samplingFrequency )

		maximumSamplingFrequency = max( samplingFrequencies )

		#compute adaptive interval in seconds
		deltaTime = timeBounds[1] - timeBounds[0]
		totalSeconds =  deltaTime.total_seconds()
		adaptiveInterval = int( totalSeconds / float( seriesLength ) )
		
		#adjust adaptive interval
		if adaptiveInterval < maximumSamplingFrequency:
			adaptiveInterval = maximumSamplingFrequency

		#approximate to one of the standard interval
		interval  = self._approximateToStandardInterval( adaptiveInterval )

		return interval
	
	def _approximateToStandardInterval(self,seconds ):

		#less than 1 year
		interval = None	
		for standardInterval in self.standardIntervals:
			if seconds <= standardInterval[0]:
				interval = standardInterval[1]
				break

		#2 years
		if interval is None:
			interval = { 'units' : 2, 'code' : 'A' }
	
		return interval
