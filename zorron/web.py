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
import cherrypy
import os

from zorron.manager import ZNDefaultManager

class ZNWebServer(object):
	
	def __init__(self,dataBaseDirectory,temporaryDirectory):
		
		self.manager = ZNDefaultManager(temporaryDirectory)
		print "Loading Database ..."
		self.dataBase = self.manager.loadDataBase( dataBaseDirectory )
		print "...done."
	
	@cherrypy.expose
	def handleRequest(self, request ):
		
		request = json.loads( request )
		result = self.manager.requestData( self.dataBase, request )	

		answer = None
		if request['outputFormat']['type'] == 'json':

			answer = result['result']

		elif request['outputFormat']['type'] == 'excel':

			filePath = result['result']
			fileName = os.path.basename( filePath )
			fileUrl = 'http://127.0.0.1:8081/tmp/%s' % ( fileName )
			answer = fileUrl

			#FILE = open( filePath, 'r' )
			#os.unlink( filePath );
			#computationName = request['computation']['name']
			#fileName = '%s_data.xlsx' % ( computationName )
			#answer = cherrypy.lib.static.serve_fileobj( FILE, 
			#					    content_type="application/octet-stream", 
			#					    disposition= "attachment", 
			#					    name= fileName )

		return answer
