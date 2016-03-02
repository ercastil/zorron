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
import numpy
import pandas
import math

import zorron.util as util
import zorron.numpyencoding as numpyencoding

class ExtendedEncoder(json.JSONEncoder):

	def default(self,obj):

		if type(obj) == pandas.core.series.Series:

			if type( obj.index ) == pandas.tseries.index.DatetimeIndex:
				result = numpyencoding.encodeTimeSeriesToJSON( obj.index.values, obj.values )
			else:

				index = numpy.array ( [ obj.index.tolist() ] )
				values = numpy.array( [ obj.values.tolist() ] )
				result = numpy.concatenate( (index.T,values.T), axis=1 ).tolist()

		elif type(obj) == pandas.core.series.TimeSeries:
			result = numpyencoding.encodeTimeSeriesToJSON( obj.index.values, obj.values )

		elif type(obj) == numpy.ndarray:
			result = obj.tolist()

		elif type(obj) == pandas.tslib.Timestamp:
			result = obj.isoformat()

		elif type(obj) == numpy.int64:
			result = int(obj)

		elif type(obj) == numpy.float64:
			result = float(obj)

		else:
			print type(obj)
			return json.JSONEncoder.default(self,obj)

		return  result
