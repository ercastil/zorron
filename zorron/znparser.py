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


import csv
import numpy
import os
import pandas
import dateutil

import zorron.util as util

class ZNCSVParser:

	def parseTableDataCollection(self,directoryPath):

		dataArrays = {}

		fileNames = os.listdir( directoryPath )

		for fileName in fileNames:

			tableId = int(fileName.split('.')[0])
			inputPath = os.path.join( directoryPath, fileName )
			dataArray = self.parseTableData( inputPath )
			dataArrays[tableId] = dataArray
			
		return dataArrays
	
	def parseTableData(self,inputPath):

		dataFrame = pandas.read_csv( inputPath, index_col = 0, header=None, date_parser=dateutil.parser.parse )			
		dataArray = util.extractDataArray( dataFrame )

		return dataArray
