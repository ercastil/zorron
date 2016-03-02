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
import pkg_resources 

class ZNMetaDataLoader:

	def load(self,inputDirectory ):

		#load database metadata
		inputPath = os.path.join( inputDirectory, 'metadata.json' )
		metaData = self.loadFile( inputPath ) 
		metaData['rootDirectory'] = os.path.abspath( inputDirectory )
		metaData['tables'] = {}

		#load table  metadata 
		fileNames = os.listdir(inputDirectory)
		for fileName in fileNames:
			inputPath = os.path.join( inputDirectory, fileName )	
			inputPath = os.path.join( inputPath, 'metadata.js' )
			print inputPath
			if not os.path.isfile(inputPath):
				continue
			tableMetaData = self.loadFile( inputPath )
			tableCode = tableMetaData['code']
			metaData['tables'][tableCode] = tableMetaData 
		
		#load statistics
		inputPath = pkg_resources.resource_filename( 'zorron','data/statistics.json' ) 
		metaData['statistic'] = self.loadFile( inputPath )

		#load variable 
		inputPath = pkg_resources.resource_filename( 'zorron','data/variable_types.json' ) 
		metaData['variable'] = self.loadFile( inputPath )

		#load transform variables
		metaData['transform'] = {}
		inputPath = pkg_resources.resource_filename( 'zorron','data/transform_variables.json' ) 
		metaData['transform']['variables'] = self.loadFile( inputPath )

		#load turbine models
		metaData['transform']['models']  = {}
		inputPath = pkg_resources.resource_filename( 'zorron','data/turbine_models.json' ) 
		metaData['transform']['models']['turbine_models'] = self.loadFile( inputPath )

		return metaData
	
	def loadFile(self,inputPath):

		inputFile = open(inputPath,'r')	
		metaData = json.load(inputFile)		
		inputFile.close()

		return metaData
