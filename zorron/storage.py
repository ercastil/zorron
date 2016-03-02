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
import numpy
import csv
import tables

import zorron.util as util

class ZNStorageEngine:

	def createStorage(self):
		pass	

	def createTable(self,tableMetaData,inMemoryData,storage,outputDirectory):
		pass

class ZNPyTablesStorage:

	def __init__(self):

		self.hdf5Files  = {}
	
	def __del__(self):
		
		for hdf5File in self.hdf5Files.values():
			hdf5File.close()

class ZNPyTablesStorageEngine(ZNStorageEngine):
	
	def createStorage(self):
		return ZNPyTablesStorage()
	
	def loadTableStorage(self,storage,tableCode, inputDirectory):

		inputPath = os.path.join( inputDirectory, 'data.h5f' )
		hdf5File = tables.open_file( inputPath, mode='a' )
		storage.hdf5Files[ tableCode ] = hdf5File

	def createTable(self,tableMetaData,dataArray,storage,outputDirectory):

		outputPath = os.path.join(outputDirectory,'data.h5f')
		hdf5File = tables.open_file( outputPath, 'w' )

		#create table
		columns = { 'timestamp' : tables.FloatCol() }
		for variable in tableMetaData['variables']:
			columns[ '%s' % ( variable['code'] ) ] = tables.FloatCol()

		table = hdf5File.create_table( hdf5File.root, 'table', columns )

		tableCode = tableMetaData['code']
		storage.hdf5Files[tableCode] = hdf5File

		#load data
		self.appendTableData( tableCode, dataArray, storage )
	
	def removeTable(self,storage,tableCode):

		hdf5File = storage.hdf5Files[tableCode]		
		hdf5File.close()
		del storage.hdf5Files[tableCode]
		
	def getTableData(self,storage,tableCode):

		hdf5File = storage.hdf5Files[tableCode]
		table = hdf5File.get_node( hdf5File.root, 'table' )

		numberRows = table.nrows
		numberColumns = len(table.cols)
		dataArray = numpy.zeros( shape = ( numberRows, numberColumns ) )
		table.read(out=dataArray) 
				
		return dataArray
			
	def deleteTableData(self,storage,tableCode,firstIndexRow,lastIndexRow):
		
		hdf5File = storage.hdf5Files[tableCode]
		table = hdf5File.get_node( hdf5File.root, 'table' )

		if firstIndexRow == 0 and lastIndexRow == table.nrows - 1:
			columnsDescription = table.coldescrs
			hdf5File.remove_node( hdf5File.root, 'table')
			hdf5File.flush()
			hdf5File.create_table( hdf5File.root, 'table', columnsDescription )		
		else:
			table.remove_rows(firstIndexRow,lastIndexRow+1)

		hdf5File.flush()
	
	def appendTableData(self,tableCode,dataArray,storage):

		hdf5File = storage.hdf5Files[tableCode]	
		table = hdf5File.get_node( hdf5File.root, 'table')
		
		table.append( dataArray )

		hdf5File.flush()
