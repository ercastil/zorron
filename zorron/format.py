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


import excelwriter
import tempfile
import pandas
import numpy
import os
import time
import json

import zorron.util as util
import zorron.excelwriter as excelwriter
import zorron.encoding as encoding

class ZNFormatHandler:

	def __init__(self,temporaryDirectory):

		self.jsonFormatter  = ZNJSONFormatter()
		self.excelFormatter = ZNExcelFormatter(temporaryDirectory)

	def handle(self,dataBase,request,answer):

		formatType = request['outputFormat']['type']
	
		output = None
		if formatType == 'json':
			output = self.jsonFormatter.format( dataBase,answer, request )
		elif formatType  == 'excel':
			output = self.excelFormatter.format( dataBase,answer, request )

		return output

class ZNJSONFormatter:

	def format(self,dataBase,answer,request):

		addMetaData = True
		if 'computation' in request:
			if 'aggregated' in request['computation']['name']:
				addMetaData = False	

		if request['type'] == 'metaData':
			addMetaData = False
	
		#adding metadata
		if addMetaData:
			seriesResult = answer['result']
			newResults = []
			for result in seriesResult:

				metaData = result['metaData']
				data     = result['data']

				entry = {
					  'tableCode' : metaData['tableCode'],
					  'tableName' : metaData['tableName'],
					  'firstTimeStamp' : metaData['firstTimeStamp'],
					  'lastTimeStamp' : metaData['lastTimeStamp'],
					  'variableType' : metaData['variableType'],
					  'variableStatistic' : metaData['variableStatistic'],
					  'variableAltitude' : metaData['variableAltitude'],
					  'interval' : answer['interval'],
					  'data' : data
					 }

				newResults.append( entry )
			answer['result'] = newResults

		#computation time
		computationTime = answer['computationTime']
		del answer['computationTime']

		#json encoding
		start = time.time()
		#TODO: clean encoding
		jsonString = json.dumps(answer,cls=encoding.ExtendedEncoder)
		jsonString = jsonString.replace('nan','null')
		jsonString = jsonString.replace('NaN','null')
		jsonString = jsonString.replace('"$*','')
		jsonString = jsonString.replace('*$"','')

		formattingTime = time.time() - start

		newAnswer = {
				"formattingTime" : formattingTime,
				"computationTime" : computationTime,
				"result" : jsonString
			}
	
		return newAnswer

class ZNExcelFormatter:

	def __init__(self,temporaryDirectory):

		self.temporaryDirectory = temporaryDirectory
		self.excelWriter = excelwriter.Writer()
		self.statisticIndex = None
		self.typeIndex = None
		
		## MF: Added several new codes and removed all underscore characters
		#self.statisticIndex = {
		#			'std' : 'DESVIACION ESTANDARD',
		#			'min' : 'MINIMA',
		#			'max' : 'MAXIMA',
		#			'sum' : 'SUMA',
		#			'sig' : 'DESVIACION ESTANDAR',
		#			'hr' :  'HORA DE OCURRENCIA',
		#			'mean' : 'PROMEDIO'
		#		}

                ## MF: Added several new codes and removed all underscore characters
		#self.typeIndex = {
		#		'bvlt' : {"name": 'VOLTAJE BATERIA', "unit": "V"},
		#		'rdni' : {"name": 'RADIACION DIRECTA NORMAL', "unit": "kW/m2"},
		#		'temp' : {"name": 'TEMPERATURA', "unit": "Celcius"},
		#		'tint' : {"name": 'TEMPERATURA INTERNA', "unit": "Celcius"},
		#		'vels' : {"name": 'VELOCIDAD VIENTO', "unit": "m/s"},
		#		'rglb_ta' : {"name": 'RADIACION SOLAR GLOBAL EN SEGUIMIENTO', "unit": "kW/m2"},
		#		'dirv' : {"name": 'DIRECCION VIENTO', "unit": "Grados"},
		#		'rnet' : {"name": 'RADIACION NETA', "unit": "kW/m2"},
		#		'velv' : {"name": 'VELOCIDAD VECTORIAL', "unit": "m/s"},
		#		'hrel' : {"name": 'HUMEDAD RELATIVA', "unit": "%"},
		#		'rglb' : {"name": 'RADIACION SOLAR GLOBAL HORIZONTAL', "unit": "kW/m2"},
		#		'rdif_ta' : {"name": 'RADIACION SOLAR DIFUSA SEGUIMIENTO', "unit": "kW/m2"},
		#		'pres' : {"name": 'PRESION ATMOSFERICA', "unit": "hPa"},
		#		'rdif' : {"name": 'RADIACION SOLAR DIFUSA', "unit": "kW/m2"},
#		#		'pacm' : {"name": 'PRECIPITACION ACUMULADA (mm)', "unit": "mm"},
		#		'prec' : {"name": 'PRECIPITACION', "unit": "mm"},
		#		'snwh' : {"name": 'ALTURA NIEVE', "unit": "m"},
#		#		'uvel' : {"name": 'VELOCIDAD ZONAL', "unit": "m/s"},
#		#		'vvel' : {"name": 'VELOCIDAD MERIDIONAL', "unit": "m/s"},
		#		'wetb' : {"name": 'TEMPERATURA BULBO HUMEDO',"unit": "Celcius"}
		#	}
		

	def format(self, dataBase, answer, request ):

		#TODO
		self.statisticIndex = dataBase.metaData['statistic']
		self.typeIndex      = dataBase.metaData['variable']

		if request['type'] == 'metaData':
			return answer

		if len( answer['result']) == 0:
			newAnswer = {
						"formattingTime" : 0,
						"computationTime" : answer['computationTime'],
						"result" : None
						}
			return newAnswer

		#generate temporary outputPath
		outputPath = tempfile.mktemp( 'znserver', dir=self.temporaryDirectory )				
		outputPath = outputPath + '.xlsx'

		startTime = time.time()

		#series
		if 'computation' not in request:

			self._formatSeries( answer, outputPath, request )			
		
		#computation
		else:
			computation = request['computation']['name']

			if computation == 'standardSummary':
				self._formatStandardSummary( answer, outputPath, request )
			elif computation == 'histogram':
				self._formatHistogram( answer, outputPath, request )
			elif computation == 'cumulativeDistribution':
				self._formatHistogram( answer, outputPath, request )
			elif computation == 'annualCycle':
				self._formatAnnualCycle( answer, outputPath, request )
			elif computation == 'dailyCycle':
				self._formatDailyCycle( answer, outputPath, request )
			elif computation == 'windRose':
				self._formatWindRose( answer, outputPath, request )

		endTime = time.time()
		totalTime = endTime - startTime

		newAnswer = {
				"formattingTime" : totalTime,
				"computationTime" : answer['computationTime'],
				"result" : outputPath
			    }

		return newAnswer

	def _formatRequestSheet(self,request,answer):
	
		#columns
		columns = [
				excelwriter.Column( 'ITEM CONSULTA', 'S'),
				excelwriter.Column( 'PROPIEDAD', 'S'),
				excelwriter.Column( 'VALOR', 'S')
			]

		#data
		dataArray = []
		dataArray.append( ('','','') )

		#TIME
		dataArray.append( ( 'FILTRO TIEMPO' , '', '' ) )
		lowerBound 	= 'Ninguna'
		upperBound 	= 'Ninguna'
		years 		= 'Todos'
		months 		= 'Todos'
		hours 		= 'Todas'

		if 'time' in request:	

			if 'lowerBound' in request['time']:
				lowerBound = request['time']['lowerBound']

			if 'upperBound' in request['time']:
				upperBound = request['time']['upperBound']

			if 'years' in request['time']:
				years = str(request['time']['years'])

			if 'months' in request['time']:
				months = str(request['time']['months'])

			if 'hours' in request['time']:
				hours = str(request['time']['hours'])
		
		dataArray.append( ( '', 'COTA INFERIOR', lowerBound ) )
		dataArray.append( ( '', 'COTA SUPERIOR', upperBound ) )
		dataArray.append( ( '', 'ANOS', years ) )
		dataArray.append( ( '', 'MESES', months ) )
		dataArray.append( ( '', 'HORAS', hours ) )

		#RESAMPLING
		dataArray.append( ( 'RESAMPLEO', '', '' ) )

		interval = 'Completo'

		if 'resampling' in request:

			#interval
			if request['resampling']['interval']['type'] == 'adaptive':
				item = answer['interval']
				interval = '%d%s' %( item['units'], item['code'] )
			dataArray.append( ( '', 'INTERVALO', interval ) )

			#statistic
			statistic = request['resampling']['statistic']['name']
			dataArray.append( ( '', 'ESTADISTICA', statistic ) )

			if statistic == 'percentile':
				dataArray.append( ( '', 'NIVEL', request['resampling']['statistic']['level'] ) )
		else:
			dataArray.append( ( '', 'INTERVALO', interval ) )


		#COMPUTATION

		if 'computation' in request:

			dataArray.append( ( 'COMPUTO', '', '' ) )

			param = request['computation']
			computation = param['name']

			
			if computation == 'histogram':

				#histogram ( min, max, bins, density ) 
				name	= 'Histograma'
				minimum = 'Ninguno'
				maximum = 'Ninguno'
				unit    = 'Frecuencia'

				if 'min' in param:
					minimum = param['min']
				
				if 'max' in param:
					maximum = param['max']

				if param['density']:
					unit = 'Densidad'

				dataArray.append( ( '', 'NOMBRE', name ) )
				dataArray.append( ( '', 'MINIMO', minimum ) )
				dataArray.append( ( '', 'MAXIMO', maximum ) )
				dataArray.append( ( '', 'INTERVALOS', param['bins'] ) )
				dataArray.append( ( '', 'UNIDAD', unit ) )
		
			elif computation == 'annualCycle':

				#annualCycle ( statistic )
				name = 'Ciclo Anual'
				statistic = param['statistic']['name']

				dataArray.append( ( '', 'NOMBRE', name ) )
				dataArray.append( ( '', 'ESTADISTICA', statistic ) )
				if statistic == 'percentile':
					dataArray.append( ( '', 'NIVEL', param['statistic']['level']  ) )


			elif computation == 'dailyCycle':

				#dailyCycle ( statistic )
				name = 'Ciclo Diario'
				statistic = param['statistic']['name']

				dataArray.append( ( '', 'NOMBRE', name ) )
				dataArray.append( ( '', 'ESTADISTICA', statistic ) )
				if statistic == 'percentile':
					dataArray.append( ( '', 'NIVEL', param['statistic']['level'] ) )


			elif computation == 'windRose':

				#windRose ( directionBins, velocityBins, velocityMin, velocityMax, density )
				
				name    = 'Rosa de los Vientos'
				unit    = 'Densidad'
				velocityMin = 'Ninguno'
				velocityMax = 'Ninguno'
				
				if 'velocityMin' in param:
					velocityMin = param['velocityMin']
				
				if 'velocityMax' in param:
					velocityMax = param['velocityMax']
				
				if 'density' in param:
					if not param['density']:
						unit = 'Frecuencia'

				dataArray.append( ( '', 'NOMBRE', name ) )
				dataArray.append( ( '', 'INTERVALOS DIRECCION', param['directionBins'] )  )
				dataArray.append( ( '', 'INTERVALOS VELOCIDAD', param['velocityBins'] ) )
				dataArray.append( ( '', 'MINIMO VELOCIDAD', velocityMin ) )
				dataArray.append( ( '', 'MAXIMO VELOCIDAD', velocityMax ) )
				dataArray.append( ( '', 'UNIDAD', unit ) )

		dataArray = numpy.array( dataArray, dtype=('U100,U100,U100') )
		sheet = excelwriter.Sheet( 'CONSULTA', columns, dataArray )

		return sheet
				

	def _formatMetaDataSheet(self, answer, firstVariableColumn ):

		#columns
		columns = [  
				excelwriter.Column( 'COLUMNA' , 'S' ),
				excelwriter.Column( 'NOMBRE ESTACION', 'S' ),
				excelwriter.Column( 'TIPO VARIABLE', 'S' ),
				excelwriter.Column( 'ESTADISTICA VARIABLE', 'S' ),
				excelwriter.Column( 'ALTITUD VARIABLE (metros)', 'N' ),
				excelwriter.Column( 'PRIMERA MEDICION', 'T' ),
				excelwriter.Column( 'ULTIMA MEDICION', 'T' )
			]

		#data
		
		dataArray = []
		columnCodes = self.excelWriter.columnCodes
		i = firstVariableColumn
		for item in answer['result']:
			metaData = item['metaData']

			columnId	  = columnCodes[i]
			i = i + 1

			tableName 	  = metaData['tableName']

			variableType	  = metaData['variableType']
			variableUnit	  = 'NN'
			if variableType in self.typeIndex:
				temporalType = self.typeIndex[variableType]
				variableType = temporalType['name']
				variableUnit = temporalType['unit']

			variableStatistic = metaData['variableStatistic']
			if variableStatistic in self.statisticIndex:
				variableStatistic = self.statisticIndex[variableStatistic]

			variableAltitude  = metaData['variableAltitude']

			firstTimeStamp	  = metaData['firstTimeStamp']
			firstTimeStamp    = util.timestamp2excel( firstTimeStamp )

			lastTimeStamp     = metaData['lastTimeStamp']
			lastTimeStamp	  = util.timestamp2excel( lastTimeStamp ) 

			row = (
				columnId,
				tableName,
				'%s (%s)' % ( variableType, variableUnit) ,
				variableStatistic,
				variableAltitude,
				firstTimeStamp,
				lastTimeStamp
				)
			dataArray.append( row )

				
		dataArray = numpy.array( dataArray, dtype=('U3,U100,U100,U50,f8,f8,f8') )
		sheet = excelwriter.Sheet( 'METADATA', columns, dataArray )

		return sheet
	
	def _formatAndPackage(self, workbookName, request,answer, dataSheet, firstVariableColumn, outputPath ):

		requestSheet  = self._formatRequestSheet( request, answer )
		metaDataSheet = self._formatMetaDataSheet( answer, firstVariableColumn )
		
		sheets = [ 
				requestSheet,
				metaDataSheet,
				dataSheet 
			]
		workbook = excelwriter.Workbook( workbookName, sheets )
		self.excelWriter.write( workbook, outputPath )

	def _formatSeries(self ,answer, outputPath, request ):

		#data
		dataColumns = [ excelwriter.Column( 'TIEMPO', 'T' ) ]
		self._formatColumnNames( answer, dataColumns )

		seriesDict = {}
		i = 0
		for item in answer['result']:
			seriesDict[i] = item['data']
			i = i + 1		
		dataFrame = pandas.DataFrame( seriesDict )
		
		#timestamp
		timeIndex = numpy.array( [ dataFrame.index.astype( numpy.int64 ) / 1e9 ] )
		timeIndex = timeIndex / 86400.0 + 25569.0

		data = dataFrame.values

		dataArray = numpy.concatenate( ( timeIndex.T, data ), axis = 1 )

		#workbook
		dataSheet = excelwriter.Sheet( 'DATA', dataColumns, dataArray )

		workbookName = 'SERIES'
		firstVariableColumn = 1
		self._formatAndPackage( workbookName, request, answer, dataSheet, firstVariableColumn, outputPath ) 
	
	def _formatStandardSummary(self,answer, outputPath, request ):

		#columns
		columns = [
				excelwriter.Column( 'NOMBRE ESTACION', 'S' ),
				excelwriter.Column( 'TIPO VARIABLE', 'S' ),
				excelwriter.Column( 'ESTADISTICA VARIABLE', 'S' ),
				excelwriter.Column( 'ALTITUD VARIABLE', 'N' ),
				excelwriter.Column( 'PRIMERA MEDICION', 'T' ),
				excelwriter.Column( 'ULTIMA MEDICION', 'T' ),
				excelwriter.Column( 'MINIMO', 'N' ),
				excelwriter.Column( 'MAXIMO', 'N' ),
				excelwriter.Column( 'PROMEDIO', 'N' ),
				excelwriter.Column( 'DESVIACION ESTANDAR', 'N' ),
				excelwriter.Column( 'FRACCION DE DATOS', 'N' )
			      ]
	
		#data
		dataArray = []
		for item in answer['result']:
			
			variableType = item['metaData']['variableType']
			variableTypeStr = variableType
			if variableType in self.typeIndex:
				variableType = self.typeIndex[ variableType ]
				variableTypeStr = '%s (%s)' % ( variableType['name'], variableType['unit'] )
			

			variableStatistic = item['metaData']['variableStatistic']
			if variableStatistic in self.statisticIndex:
				variableStatistic = self.statisticIndex[ variableStatistic ]

			firstTimeStamp = item['data']['start'] 
			firstTimeStamp = util.nanoseconds2excel( firstTimeStamp )

			lastTimeStamp = item['data']['end']
			lastTimeStamp = util.nanoseconds2excel( lastTimeStamp )

			row = (	
				item['metaData']['tableName'],
				variableTypeStr,
				variableStatistic,
				item['metaData']['variableAltitude'],
				firstTimeStamp,
				lastTimeStamp,
				item['data']['min'],
				item['data']['max'],
				item['data']['mean'],
				item['data']['stdev'],
				item['data']['fraction']
			)

			dataArray.append( row )
	
		dataArray = numpy.array( dataArray , dtype= ('U100,U100,U50,f8,f8,f8,f8,f8,f8,f8,f8') )

		#workbook
		sheet = excelwriter.Sheet( 'DATOS', columns, dataArray )
		workbook = excelwriter.Workbook( 'RESUMEN_ESTANDAR', [ sheet ] ) 
		self.excelWriter.write( workbook, outputPath )

	def _formatColumnNames(self, answer, columns ):

		for item in answer['result']:

			metaData = item['metaData']

			tableName = metaData['tableName']

			variableType      = metaData['variableType']
			variableUnit      = 'NN'
			if variableType in self.typeIndex:
				tempType = self.typeIndex[ variableType ]
				variableType = tempType['name']
				variableUnit = tempType['unit']
			
			variableStatistic = metaData['variableStatistic']
			if variableStatistic in self.statisticIndex:
				variableStatistic = self.statisticIndex[variableStatistic]
			
			variableAltitude   = metaData['variableAltitude']

			variableName = 'Estacion %s - %s (%s) - %s - %s (metros)' % ( 

								tableName,
								variableType , 
								variableUnit,
								variableStatistic, 
								variableAltitude 
							)

			columns.append( excelwriter.Column( variableName, 'N' ) )


	def _formatHistogram(self,answer, outputPath , request ):

		#columns
		dataColumns = [ 
				excelwriter.Column( 'COTA_INFERIOR', 'N') ,
				excelwriter.Column( 'COTA_SUPERIOR', 'N' )
			]

		self._formatColumnNames( answer , dataColumns )

		#data
		boundArray = numpy.array( answer['result'][0]['data'] )[ :, 0]
		boundArray = numpy.array( boundArray.tolist() )

		seriesArray = []
		for item in answer['result']:
			series   = numpy.array( item['data'] )
			seriesArray.append( series[:,1] )

		seriesArray = numpy.array( seriesArray )
		dataArray = numpy.concatenate( ( boundArray, seriesArray.T ), axis = 1 )

		#workbook
		dataSheet = excelwriter.Sheet( 'DATA', dataColumns, dataArray )

		if request['computation']['name'] == 'histogram':
			workbookName = 'HISTOGRAMA'
		else:
			workbookName = 'DISTRIBUCION_ACUMULADA'
		firstVariableColumn = 2
		self._formatAndPackage( workbookName, request, answer, dataSheet, firstVariableColumn, outputPath ) 

	
	def _formatAnnualCycle(self,answer, outputPath , request ):
		
		#columns
		dataColumns = [ 
				excelwriter.Column( 'MES', 'N') 
			      ]
		self._formatColumnNames( answer, dataColumns )
		
		#data
		seriesArray = []
		for item in answer['result']:
			seriesArray.append( item['data'].values )

		indexArray = numpy.array( [ numpy.linspace(1,12,12) ] )
		seriesArray = numpy.array( seriesArray )

		dataArray = numpy.concatenate( ( indexArray.T, seriesArray.T ), axis = 1 )

		#workbook
		dataSheet = excelwriter.Sheet( 'DATA', dataColumns, dataArray )

		workbookName = 'CICLO_ANUAL'
		firstVariableColumn = 1
		self._formatAndPackage( workbookName, request, answer, dataSheet, firstVariableColumn, outputPath ) 

	
	def _formatDailyCycle(self,answer, outputPath , request ):

		#columns
		dataColumns = [ 
				excelwriter.Column( 'HORA', 'N') 
			      ]
		self._formatColumnNames( answer, dataColumns )

		#data
		seriesArray = []
		for item in answer['result']:
			print item['data']
			seriesArray.append( item['data'] )

		indexArray = numpy.array( [ numpy.linspace(0,23,24) ] )
		seriesArray = numpy.array( seriesArray )

		dataArray = numpy.concatenate( ( indexArray.T, seriesArray.T ), axis = 1 )

		#workbook
		dataSheet = excelwriter.Sheet( 'DATA', dataColumns, dataArray )

		workbookName = 'CICLO_DIARIO'
		firstVariableColumn = 2
		self._formatAndPackage( workbookName, request, answer, dataSheet, firstVariableColumn, outputPath ) 

	def _formatAnnualDailyCycle(self,answer, outputPath , request ):
		
		#columns
		dataColumns = [ 
				excelwriter.Column( 'MES', 'N') ,
				excelwriter.Column( 'HORA', 'N' )
			      ]
		self._formatColumnNames( answer, dataColumns )

		#data
		dataArray = []

		#workbook
		dataSheet = excelwriter.Sheet( 'DATA', dataColumns, dataArray )

		workbookName = 'CICLO_ANUAL_DIARIO'
		firstVariableColumn = 1
		self._formatAndPackage( workbookName, request, answer, dataSheet, firstVariableColumn, outputPath ) 

	def _formatWindRose(self,answer, outputPath , request ):
		
		#columns
		dataColumns = [ 
				excelwriter.Column( 'COTA INFERIOR DIRECCION', 'N') ,
				excelwriter.Column( 'COTA SUPERIOR DIRECCION', 'N') ,
				excelwriter.Column( 'COTA INFERIOR VELOCIDAD', 'N') ,
				excelwriter.Column( 'COTA SUPERIOR VELOCIDAD', 'N') 
			      ]
		self._formatColumnNames( answer, dataColumns )
				
		#data
		firstItemData = answer['result'][0]['data']

		directionBounds  = firstItemData['directionBinEdges']
		velocityBounds   = firstItemData['velocityBinEdges']

		numberVelocityBins = len( velocityBounds ) - 1
		numberDirectionBins = len( directionBounds ) - 1

		directionInferiorBounds = numpy.tile( directionBounds[:-1], ( numberVelocityBins, 1 ) ).T.flatten()
		directionSuperiorBounds = numpy.tile( directionBounds[1:], ( numberVelocityBins, 1 ) ).T.flatten()

		velocityInferiorBounds = numpy.tile( velocityBounds[:-1], numberDirectionBins )
		velocitySuperiorBounds = numpy.tile( velocityBounds[1:], numberDirectionBins )

		dataArray = [ 
				directionInferiorBounds, 
				directionSuperiorBounds, 
				velocityInferiorBounds, 
				velocitySuperiorBounds 
			]

		for item in answer['result']:
			histogramArray = item['data']['histogram'].transpose().flatten()
			dataArray.append( histogramArray )

		dataArray = numpy.array( dataArray ).T

		#write workbook
		dataSheet = excelwriter.Sheet( 'DATA', dataColumns, dataArray )

		workbookName = 'ROSA_VIENTO'
		firstVariableColumn = 4
		self._formatAndPackage( workbookName, request, answer, dataSheet, firstVariableColumn, outputPath ) 
		

	def formatDataBase(self,dataBase,outputDirectory):
		
		for tableCode,dataFrame in dataBase.data.dataFrames.items():
			outputPath = os.path.join( outputDirectory, '%s' % ( tableCode ) )
			self.formatTable( tableCode,dataFrame, dataBase.metaData, outputPath )
	
	def formatTable(self, tableCode, dataFrame, metaData,outputPath):
     
		numberVariables = len( dataFrame.columns )

		#metadata
		columnNames = [ 
				'TIPO VARIABLE',
				'ESTADISTICA VARIABLE',
				'ALTITUD VARIABLE (metros)'
			      ]

		columns = [ excelwriter.Column( 'ID', 'S' ) ]
		for name in columnNames:
			columns.append( excelwriter.Column( name, 'S' ) )

		variableMetaDataArray = metaData['tables'][tableCode]['variables']
		variableNames = []
		dataArray = []

		for c in range( numberVariables ):
			variableId = c + 1
			variableMetaData = variableMetaDataArray[c]
			variableType      = variableMetaData[ 'type' ]
			if variableType in typeIndex:
				variableType = self.typeIndex[ variableType ]

			variableStatistic = variableMetaData[ 'statistic' ]
			if variableStatistic in statisticIndex:
				variableStatistic = self.statisticIndex[ variableMetaData[ 'statistic' ] ]
			variableAltitude  = variableMetaData[ 'altitude' ]
			row = [ variableId, variableType, variableStatistic, variableAltitude ]
			variableName = '%s [%s] en %s metros' % ( variableType, variableStatistic, variableAltitude )
			variableNames.append( variableName )
			dataArray.append( row )

		dataArray = numpy.array( dataArray )
			
		metaDataSheet = excelwriter.Sheet( 'METADATA', columns, dataArray )
		
		#data
		columns1 = [ excelwriter.Column( 'TIEMPO', 'T' ) ]
		for i in range(numberVariables):
			columns1.append( excelwriter.Column( '%s' % ( variableNames[i] ) , 'N' ) )	

		#timestamp
		timeIndex = numpy.array( [ dataFrame.index.astype( numpy.int64 ) / 1e9 ] )

                # MF: Added + 1.0 to the timeIndex to get the right date in Excel
		timeIndex = timeIndex / 86400.0 + 25569.0 
		data = dataFrame.values

		dataArray = numpy.concatenate( ( timeIndex.T, data ), axis = 1 )

		dataSheet = excelwriter.Sheet( 'DATA', columns1, dataArray )
		sheets = [ metaDataSheet, dataSheet ]
		workbook = excelwriter.Workbook( 'RESULTADO', sheets )

		self.excelWriter.write( workbook, outputPath )

		return
