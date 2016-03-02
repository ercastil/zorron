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


import zorron.config as config
import shutil

from zorron.manager import ZNDefaultManager
from zorron.request import ZNRequestHandler
from zorron.format import ZNExcelFormatter

manager = ZNDefaultManager()
dataBaseDirectory = '/home/ach/opt/zorron/teniente'
database = manager.loadDataBase( dataBaseDirectory )

requestHandler = ZNRequestHandler()
formatter = ZNExcelFormatter()

outputDirectory = '/home/ach/opt/zorron/data/teniente_excel'

for tableCode in dataBase.tables.keys():
	
	request = { 
			"type" : "data",
			"table" : { "code" : tableCode } 
		}
	answer = requestHandler.handle( dataBase, request )
	result = formatter.formatDataBase( dataBase, outputDirectory )
