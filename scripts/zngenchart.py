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


#!/usr/bin/env python

from optparse import OptionParser
import sys

#options
parser = OptionParser()

parser.add_option("-H","--host",help="The hostname")
parser.add_option("-P","--port",help="The port")
parser.add_option("-O","--output-directory",help="The input metadata file")

(options,args) = parser.parse_args()

from zorron.metadata import ZNMetaDataLoader
from zorron.znparser import ZNCSVParser
from zorron.server import ZNClient

metaDataLoader = ZNMetaDataLoader()
parser	       = ZNCSVParser()

client 	= ZNClient()

try:
	client.connect( options.host, options.port, options.server_name )

	print "loading metadata..."
	metaData = metaDataLoader.loadFile( options.metadata_file )
	print "...metadata loaded"

	print "loading data..."
	dataArray = parser.parseTableData( options.data_file )
	print "...data loaded"

	print "creating table..."
	client.createTable( metaData , dataArray )
	print "...table created"

	client.close()

except:
	raise
