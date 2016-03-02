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

import zorron.util as util

class ZNDataEngine:

	def createData(self):
		pass

	def createTable(self,tableMetaData,data):

		pass

	def getTableData(self,data,tableId):

		pass
	
class ZNPandasData:

	def __init__(self):
		self.dataFrames = {}

class ZNPandasDataEngine(ZNDataEngine):

	def createData(self):
		return ZNPandasData()

	def createTable(self,tableMetaData,data,dataArray):
	
		variablesNames  = []
		for variable in tableMetaData['variables']:
			variablesNames.append( '%s' % ( variable['code'] ) )

		dataFrame = util.createDataFrame( dataArray, variablesNames)	

		data.dataFrames[ tableMetaData['code'] ] = dataFrame

	def removeTable(self,data,tableCode):

		del data.dataFrames[tableCode]
	
	def getTableData(self,data,tableCode,firstPosition=None, lastPosition=None ):

		dataFrame = data.dataFrames[tableCode]

		if firstPosition == None and lastPosition == None:
			firstPosition = 0
			lastPosition = len(dataFrame) - 1
			
		dateTimeArray = dataFrame.index[firstPosition:lastPosition+1].astype(numpy.int64)
		indexArray = numpy.array( [ dateTimeArray ] )
		dataArray = numpy.concatenate( ( indexArray.T, dataFrame.values[firstPosition:lastPosition+1] ), axis = 1)
		return dataArray
	
	def updateTableData(self,data,tableCode,dataArray):
		
		#create new dataframe
		dataFrame = data.dataFrames[tableCode]
		variablesNames = dataFrame.columns
		updateDataFrame = util.createDataFrame( dataArray,variablesNames )
		
		#calculate intersection and differencefrom zorron.util import parseDateTime
		existingValuesIndex = dataFrame.index.intersection( updateDataFrame.index )
		newValuesIndex = updateDataFrame.index.diff( existingValuesIndex )

		#update existing values if not nan
		dataFrame.update( updateDataFrame.ix[existingValuesIndex] )

		#concatenate new values
		newDataFrame = pandas.concat( [ dataFrame, updateDataFrame.ix[newValuesIndex] ] )

		#sort rows
		newDataFrame = newDataFrame.sort()

		#update new dataframe
		data.dataFrames[ tableCode ] = newDataFrame

		#release memory
		del dataFrame
		del updateDataFrame
