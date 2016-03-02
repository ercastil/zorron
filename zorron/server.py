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
import Pyro4
import json
import traceback
import zorron.util as util
import zorron.encoding as encoding

from zorron.manager import ZNDefaultManager
from zorron.znparser import ZNCSVParser


class ZNServer( object ):

	def __init__(self,dataBaseDirectory,temporaryDirectory):
		
		self.manager = ZNDefaultManager(temporaryDirectory)
		print "Loading Database ..."
		self.dataBase = self.manager.loadDataBase( dataBaseDirectory )
		print "...done."
	
	def getMetaData(self):

		return self.dataBase.metaData
	
	def getMetaDataTable(self,code):

		return self.dataBase.metaData['tables'][code]
	
	def getTableCodes(self):

		return self.dataBase.metaData['tables'].keys()

	def createTable(self,metaData,dataArray=None):

		if not dataArray is None:
			dataArray = numpy.array( dataArray )
		self.manager.createTable( self.dataBase, metaData, dataArray )
	
	def removeTable(self,tableName):

		self.manager.removeTable( self.dataBase, tableName )

	def updateTable(self,tableCode,dataArray):

		dataArray = numpy.array( dataArray )
		self.manager.updateTable( self.dataBase, tableCode, dataArray )

	def requestData(self, request ):
		
		try:
			result = self.manager.requestData( self.dataBase, request )	

		except:
			stackTrace = traceback.format_exc()
			result = { "error" : stackTrace }

		return result

class ZNClient:

	def __init__(self,host=None,port=None,name=None):

		self.host = host 
		self.port = port
		self.name = name
		self.parser = ZNCSVParser()

	def connect(self,host=None,port=None,name=None ):

		if host is None:
			host = self.host
			port = self.port
			name = self.name

		uri = 'PYRO:%s@%s:%s' % ( name, host, port )
		self.serverProxy = Pyro4.Proxy( uri )
	
	def close(self):

		self.serverProxy._pyroRelease()
	
	def getMetaData(self):
		
		metaData = self.serverProxy.getMetaData()

		return metaData
	
	def getMetaDataTable(self,code):

		metaData = self.serverProxy.getMetaDataTable(code)

		return metaData
	
	def getTableCodes(self):

		tableCodes = self.serverProxy.getTableCodes()	

		return tableCodes
	
	def createTable(self,metaData,dataArray=None):

		if not dataArray is None:
			util.replaceNan( dataArray )
			dataArray = dataArray.tolist()

		self.serverProxy.createTable( metaData, dataArray )
	
	def removeTable(self,tableCode):
		
		self.serverProxy.removeTable( tableCode )
	
	def updateTable(self, tableCode, dataArray ):

		dataArray = dataArray.tolist()
		self.serverProxy.updateTable( tableCode, dataArray )

	def requestData(self, request ):
		
		result = self.serverProxy.requestData( request )
		return result
