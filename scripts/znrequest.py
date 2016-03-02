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
import sys
import time
import json

parser = OptionParser()

parser.add_option("-H","--host",help="The hostname",default='localhost')
parser.add_option("-P","--port",help="The port",default=1069)
parser.add_option("-I","--input-file",help="The request input file")
parser.add_option("-R","--request",help="The request string")
parser.add_option("-O","--output-file",help="The output file")
parser.add_option("-S","--server-name",help="The server name",default="znserver")

(options,args) = parser.parse_args()

import json
import zorron.util as util
from zorron.metadata import ZNMetaDataLoader
from zorron.znparser import ZNCSVParser
from zorron.server import ZNClient
import csv

metaDataLoader 	= ZNMetaDataLoader()
parser 		= ZNCSVParser()
client 		= ZNClient()

try:
	client.connect( options.host, options.port, options.server_name )

	#input
	if options.request is not None:

		request = options.request
	
	else:
		request = metaDataLoader.loadFile( options.input_file )

	result = client.requestData( request )

	client.close()

	#output
	if options.output_file is not None:

		outputFile = open( options.output_file, 'w' )
		json.dump(result,outputFile,indent=4)
		outputFile.close()

	else:
		result = json.dumps(result)
		sys.stdout.write(result)	
except:

	raise
