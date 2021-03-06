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
import os
import datetime
from optparse import OptionParser
from apscheduler.scheduler import Scheduler
import Pyro4

from zorron.server import ZNServer
from zorron.util import ZNFileRemover

#options
parser = OptionParser()

parser.add_option("-H","--host",help="The host to bind",default='localhost')
parser.add_option("-P","--port",help="The port to listen",default=1069)
parser.add_option("-D","--database-directory",help="The directory where database is located")
parser.add_option("-S","--server-name",help="The name of the server",default='znserver')
parser.add_option("-T","--temporary-directory",help="The directory where the temporary output files will be created",default="/tmp")

(options,args) = parser.parse_args()

options.database_directory  = os.path.abspath( options.database_directory )
options.temporary_directory = os.path.abspath( options.temporary_directory )

#scheduler
scheduler = Scheduler()
scheduler.start()
pattern = 'znserver.xlsx'
timeInterval = datetime.timedelta(days=1)
fileRemover = ZNFileRemover(options.temporary_directory,pattern,timeInterval)
scheduler.add_interval_job( fileRemover.remove, days=1)

#server
znServer = ZNServer( options.database_directory , options.temporary_directory )
daemon = Pyro4.Daemon(host=options.host,port=int(options.port))
daemon.register( znServer, options.server_name )
daemon.requestLoop()
