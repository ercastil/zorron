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
import sys,traceback

parser = OptionParser()

parser.add_option("-H","--host",help="The hostname")
parser.add_option("-P","--port",help="The port")
parser.add_option("-O","--output-directory",help="The output directory")
parser.add_option("-S","--statistic",help="The variable statistic")
parser.add_option("-N","--server-name",help="The server name",default="znserver")

(options,args) = parser.parse_args()

import os

from zorron.server import ZNClient
from zorron.graphics import ZNChartGenerator
client = ZNClient(options.host, options.port, options.server_name )
chartGenerator = ZNChartGenerator()

try:

	#intervals 	= [ 'H','D','W','M','A' ]	
	#sizes	  	= [ 1, 2, 3, 5, 7, 11, 13, 17 ]
	#chartTypes 	= [ 'series', 'histogram', 'dailyCycle', 'annualCycle' ]

	intervals 	= [ 'C' ]
	sizes	  	= [ 1 ]
	chartTypes 	= [ 'series' ]


	for size in sizes:

		sizeDirectory = os.path.join( options.output_directory, '%d' % ( size ) )
		os.mkdir( sizeDirectory )

		for chartType in chartTypes:

			chartTypeDirectory = os.path.join( sizeDirectory, '%s' % ( chartType ) )
			os.mkdir( chartTypeDirectory )

			for interval in intervals:

				#daily cycle
				if chartType == 'dailyCycle' and \
				( interval != 'C' and interval != 'H' ):
					continue

				#annual cycle
				if chartType == 'annualCycle' and interval == 'A':
					continue

				intervalDirectory = os.path.join( chartTypeDirectory, '%s' % ( interval ) )
				os.mkdir( intervalDirectory )

				chartGenerator.generate( client, size, interval, options.statistic, chartType, intervalDirectory )

except:
	raise
