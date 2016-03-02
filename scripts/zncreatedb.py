#!/usr/bin/env python

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



from optparse import OptionParser
import sys,traceback

parser = OptionParser()

parser.add_option("-M","--metadata-file",help="The database metadata file")
parser.add_option("-I","--input-directory",help="The input data directory")
parser.add_option("-O","--output-directory",help="The output directory")

(options,args) = parser.parse_args()

import os
import time

from zorron.metadata import ZNMetaDataLoader
from zorron.znparser import ZNCSVParser
from zorron.manager import ZNDefaultManager

def createTable( metaDataLoader, parser, manager, dataBase, inputDirectoryPath, outputDirectoryPath ):

	inputPath = os.path.join( inputDirectoryPath, 'metadata.json' )
	metaData = metaDataLoader.loadFile( inputPath )

	inputPath = os.path.join( inputDirectoryPath, 'data.csv' )
	dataArray = parser.parseTableData( inputPath )

	manager.createTable( dataBase, metaData , dataArray )

metaDataLoader 	= ZNMetaDataLoader()
parser	       	= ZNCSVParser()
manager		= ZNDefaultManager() 

try:
	metaData = metaDataLoader.loadFile(options.metadata_file)
	rootDirectory = os.path.abspath(options.output_directory)
	metaData['rootDirectory'] = rootDirectory

	dataBase = manager.createDataBase( metaData, options.output_directory )

	totalTime = 0
	fileNames = os.listdir( options.input_directory )

	for fileName in fileNames:	
		inputPath = os.path.join(  options.input_directory, fileName )
		if os.path.isdir(inputPath):

			print fileName
			start = time.time()
			createTable(metaDataLoader,parser,manager,dataBase,inputPath,options.output_directory)
			deltaTime = time.time() - start

			print deltaTime
			totalTime = totalTime + deltaTime

	print totalTime

except:
	raise
