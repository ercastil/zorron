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

import sys

from optparse import OptionParser
parser = OptionParser()

parser.add_option("-H","--host",help="The hostname")
parser.add_option("-P","--port",help="The port")
parser.add_option("-R","--request",help="The request string")
parser.add_option("-S","--server-name",help="The server name",default="znserver")

(options,args) = parser.parse_args()

import json
import zorron.util as util
from zorron.server import ZNClient
import csv

try:
	client 	= ZNClient()
	client.connect( options.host, options.port, options.server_name )

	request = json.loads( options.request )
	answer = client.requestData( request )
	sys.stdout.write( answer['result'] )
	client.close()
except:
	raise
