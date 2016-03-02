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
import numpy

import zorron.timefilter as timefilter
import zorron.util as util

class ZNTableFilter:

	def filter(self,metaData,tableFilter):
	
		if tableFilter is None:

			tableCodes = metaData['tables'].keys()
		else:
			tableCodes = []
			tableMetaDataArray = metaData['tables']

			for tableCode,tableMetaData in tableMetaDataArray.items():
				
				selected = True			
				for key,filterValue in tableFilter.items():

					value = tableMetaData[key]
					if type(filterValue) == types.ListType:
						selected = value in filterValue
					else:
						selected = value == filterValue

					if not selected:
						break

				if selected:
					tableCodes.append( tableCode )
			
		return tableCodes

class ZNVariableFilter:

	def filter(self,metaData,tableCodes,variableFilter):

		idTuples = []

		for tableCode in tableCodes:

			variableMetaDataArray = metaData['tables'][tableCode]['variables']

			variableIndexArray = []
			index = 0

			#default value
			if variableFilter is None:

				variableIndexArray = range(len(variableMetaDataArray))

			elif len(variableFilter) > 0:

				for variableMetaData in variableMetaDataArray:

					selected = True
					for key,filterValue in variableFilter.items():

						value = variableMetaData[key]	
						if type(filterValue) == types.ListType:
							selected = value in filterValue
						else:
							selected = value == filterValue

						if not selected:
							break
				
					if selected:
						variableIndexArray.append( index )

					index = index + 1

			if len(variableIndexArray) > 0:
				idTuples.append( ( tableCode, variableIndexArray ) )
		
		return idTuples

class ZNTimeFilter:
	
	def filter(self,series, timeFilter ):

		#default value
		if timeFilter is None:
			return series
	
		booleanIndexes = []

		#years
		years = []
		if 'years' in timeFilter:
			years = timeFilter['years']
		years = numpy.array( years )
		
		#months
		months = []
		if 'months' in timeFilter:
			months = timeFilter['months']
		months = numpy.array( months )
		
		#hours
		hours = []
		if 'hours' in timeFilter:
			hours = timeFilter['hours']
		hours = numpy.array( hours )

		periodIndex = timefilter.filterByTime( series.index.values, years, months, hours )
		booleanIndexes.append( periodIndex )
		
		size = len(series)
		defaultIndex = [True]*size
		resultIndex = defaultIndex
		for booleanIndex in booleanIndexes:
			resultIndex = numpy.logical_and( resultIndex, booleanIndex )						
		filteredSeries = series[ resultIndex ]

		return filteredSeries
