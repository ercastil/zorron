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


import json
import os
import shutil

import numpy
import tables
import pandas

import zorron.util as util

from zorron.model import ZNDataBase
from zorron.znparser import ZNCSVParser
from zorron.metadata import ZNMetaDataLoader
from zorron.data import ZNPandasDataEngine
from zorron.storage import ZNPyTablesStorageEngine
from zorron.request import ZNRequestHandler
from zorron.format import ZNFormatHandler
from zorron.error import ZNMissingFieldError

class ZNManager:

	def createDataBase( self, metaData, outputDirectory ):
		pass
	
	def removeDataBase( self, dataBase ):
		pass
	
	def loadDataBase( self, inputDirectory ):
		pass

	def createTable(self, dataBase, metaData, dataArray=None ):
		pass
	
	def removeTable(self, dataBase, tableCode ):
		pass

	def updateTable( self, dataBase, tableCode, dataArray ):
		pass
	
	def requestData( self, dataBase, request ):
		pass

class ZNDefaultManager(ZNManager):

	def __init__(self,temporaryDirectory='/tmp'):

		metaDataLoader 	= ZNMetaDataLoader()
		dataEngine     	= ZNPandasDataEngine()
		storageEngine  	= ZNPyTablesStorageEngine()

		self.creator  		= ZNCreator( dataEngine, storageEngine )
		self.remover    	= ZNRemover( dataEngine, storageEngine )
		self.loader   		= ZNLoader( metaDataLoader, dataEngine, storageEngine )
		self.updater  		= ZNUpdater( dataEngine, storageEngine )
		self.requestHandler 	= ZNRequestHandler()
		self.formatHandler	= ZNFormatHandler(temporaryDirectory)
	
	def createDataBase( self, metaData, outputDirectory ):
		dataBase = self.creator.createDataBase( metaData, outputDirectory )
		return dataBase
	
	def removeDataBase( self, dataBase ):
		self.remover.removeDataBase( dataBase )
		pass
	
	def loadDataBase( self, inputDirectory ):
		dataBase = self.loader.loadDataBase( inputDirectory )
		dataBase.metaData['rootDirectory'] = os.path.abspath( inputDirectory )
		return dataBase
	
	def createTable( self, dataBase, metaData, dataArray=None ):

		self.creator.createTable( dataBase, metaData, dataArray )
	
	def updateTable( self, dataBase, tableCode, dataArray ):

		self.updater.updateTable( dataBase, tableCode, dataArray )
	
	def removeTable( self, dataBase, tableCode ):
		self.remover.removeTable( dataBase, tableCode )
	
	def requestData( self, dataBase, request  ):
		answer = self.requestHandler.handle( dataBase, request ) 
		answer = self.formatHandler.handle( dataBase, request, answer )
		return answer

class ZNCreator:

	def __init__(self,dataEngine,storageEngine,):

		self.dataEngine 	= dataEngine
		self.storageEngine	= storageEngine

	def createDataBase(self,metaData,outputDirectory ):

		#containers
		data 	= self.dataEngine.createData()
		storage = self.storageEngine.createStorage()

		#copy metadata file
		outputPath = os.path.join( outputDirectory, 'metadata.js' )
		outputFile = open( outputPath, 'w' )
		json.dump( metaData, outputFile, indent=4 )
		outputFile.close()

		#add table dictionary
		metaData['tables'] = {}

		return ZNDataBase( metaData, data, storage )

	def createTable(self,dataBase, metaData, dataArray = None):

		#verify if have a respective: 
		#code, latitude, longitude and elevation
		requiredFields = [ 	
					'code',
					'latitude',
					'longitude',
					'elevation'
				]
		for field in requiredFields:
			if field not in metaData:
				missingFieldError = ZNMissingFieldError(field)	
				raise missingFieldError 

		#add to database metadata
		tableCode = metaData['code']
		dataBase.metaData['tables'][tableCode] = metaData

		#create directory
		outputRootDirectory = dataBase.metaData['rootDirectory']
		outputDirectory = os.path.join( outputRootDirectory, '%s' % ( tableCode ) )
		os.mkdir( outputDirectory )

		#copy metadata
		outputPath = os.path.join( outputDirectory, 'metadata.js' )
		outputFile = open( outputPath, 'w' )
		json.dump( metaData, outputFile, indent=4 )
		outputFile.close()

		#check and resolve duplicate entries
		dataArray = util.resolveDuplicatedEntries(dataArray)

		#create data structure in memory
		self.dataEngine.createTable(metaData,dataBase.data,dataArray)

		#create data structure in disk
		self.storageEngine.createTable(metaData,dataArray,dataBase.storage,outputDirectory)

class ZNLoader:

	def __init__(self,metaDataLoader,dataEngine,storageEngine):

		self.metaDataLoader 	= metaDataLoader
		self.dataEngine 	= dataEngine
		self.storageEngine 	= storageEngine

	def loadDataBase(self,inputDirectory):

		#metadata
		metaData = self.metaDataLoader.load( inputDirectory ) 	

		#data
		data = self.dataEngine.createData()	

		#storage
		storage = self.storageEngine.createStorage()

		#database
		dataBase = ZNDataBase( metaData, data, storage )

		#iterate subdirectories
		fileNames = os.listdir(inputDirectory)
		for fileName in fileNames:
			inputPath = os.path.join( inputDirectory, fileName )	
			if os.path.isfile(inputPath):
				continue
			self.loadTable(dataBase,inputPath)

		return dataBase
	
	def loadTable(self,dataBase,inputDirectory):

		#load metadata
		inputPath = os.path.join( inputDirectory, 'metadata.js' )
		tableMetaData = self.metaDataLoader.loadFile( inputPath )

		tableCode = tableMetaData['code']

		#load storage
		self.storageEngine.loadTableStorage(dataBase.storage,tableCode,inputDirectory)

		#load data
		dataArray = self.storageEngine.getTableData(dataBase.storage,tableCode)
		
		#data
		self.dataEngine.createTable( tableMetaData, dataBase.data, dataArray )
		
class ZNUpdater:
	
	def __init__(self,dataEngine,storageEngine):
		
		self.dataEngine = dataEngine
		self.storageEngine = storageEngine

	def updateDataBase(self,dataBase,dataArrays):
		
		#update tables
		tablesMetaData = dataBase.metaData['tables']

		for tableMetaData in tablesMetaData:
			tableCode = tableMetaData['code']
			self.updateTableData( tableCode, dataBase, dataArrays[tableCode] )
	
	def updateTable(self, dataBase, tableCode , dataArray ):
		
		tableMetaData = dataBase.metaData['tables'][tableCode]

		#compute row range to be updated
		data = dataBase.data
		dataFrame = data.dataFrames[tableCode]
		firstIndexValue = numpy.int64( dataArray[0][0] )
		timeIndexArray = dataFrame.index.astype(numpy.int64)
		firstPosition  = self._computeFirstPosition( timeIndexArray,firstIndexValue)
		lastPosition = len(dataFrame) - 1
		
		#update data
		self.dataEngine.updateTableData( data, tableCode, dataArray )

		#update storage
		storage = dataBase.storage
		
		if firstPosition <= lastPosition:
			self.storageEngine.deleteTableData( storage, tableCode, firstPosition,lastPosition )

		dataFrame = data.dataFrames[tableCode]
		lastPosition = len(dataFrame) - 1
		dataArray = self.dataEngine.getTableData( data, tableCode, firstPosition,lastPosition )
		self.storageEngine.appendTableData( tableCode, dataArray, storage  )

	def _computeFirstPosition(self,array,value):

		firstPosition = 0
		for a in array:
			if value <= a:
				break
			firstPosition = firstPosition + 1

		return firstPosition	

class ZNRemover:

	def __init__(self,dataEngine,storageEngine):

		self.dataEngine 	= dataEngine
		self.storageEngine 	= storageEngine
	
	def removeTable(self,dataBase,tableCode):

		#meta data
		del dataBase.metaData['tables'][tableCode]

		#data
		self.dataEngine.removeTable( dataBase.data, tableCode )

		#storage
		self.storageEngine.removeTable( dataBase.storage, tableCode )

		#directory
		rootDirectory = dataBase.metaData['rootDirectory']
		path = os.path.join( rootDirectory, tableCode )
		shutil.rmtree( path )

class ZNRequester:

	def requestDataBase(self,dataBase,fromTime=None,toTime=None):
		
		dataArrays = {}
		tablesMetaData = dataBase.metaData['tables']

		for tableMetaData in tablesMetaData:
			tableCode = tableMetaData['code']
			dataArray = self.requestTableData( dataBase, tableCode, fromTime, toTime )	
			dataArrays[tableId] = dataArray

		return dataArrays
	
	def requestTableData(self,dataBase,tableCode,fromTime,toTime):
		
		data = dataBase.data
		dataFrame = data.dataFrames[tableCode]
		if fromTime is None and toTime is None:
			subDataFrame = dataFrame
        	else:
			subDataFrame = dataFrame[ ( dataFrame.index >= fromTime ) & ( dataFrame.index <= toTime ) ]

		dataArray = subDataFrame.to_records().tolist()
		
		return dataArray
