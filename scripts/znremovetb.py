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
parser.add_option("-S","--server-name",help="The server name")
parser.add_option("-N","--table-name",help="The table name")

(options,args) = parser.parse_args()

from zorron.server import ZNClient

client 	= ZNClient()

try:
	client.connect( options.host, options.port, options.server_name )
	client.removeTable( options.table_name )
	client.close()

except:
	raise
