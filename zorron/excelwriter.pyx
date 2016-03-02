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


import tempfile
import shutil
import os
import string
from libc.stdio cimport *
import pkg_resources 

import numpy as np
cimport numpy as np


cdef extern from "math.h":

	bint isnan(double x)

cdef extern from "stdio.h":

	FILE *fopen( const char * filename, const char *mode )
	int fclose(FILE *)
	int fprintf( FILE* f, const char *format, ... )
	int fflush( FILE * )

class Column:

	def __init__(self,name,columnType):

		self.name = name
		self.type = columnType
	
	def __str__(self):
		
		return self.__repr__()
	
	def __repr__(self):
		
		return '<%s,%s>' % ( self.name, self.type )
		

class Sheet:

	def __init__(self, name, columns, dataArray ):
		
		self.id	      = None
		self.name      = name
		self.columns   = columns
		self.dataArray = dataArray
	
	def __str__(self):
		
		return self.__repr__()

	def __repr__(self):
	
		string = '%s\n' % ( self.columns )
		for row in self.dataArray:
			string = string + '%s\n' % ( row )

		return string

class Workbook:

	def __init__(self,name,sheets):

		self.name = name
		self.sheets = sheets

class Writer:

	def __init__(self):

		self.templatePath = pkg_resources.resource_filename( 'zorron','data/workbook.zip' )
		self.columnCodes = self._computeColumnCodes()

	def _computeColumnCodes(self):

		columnCodes = []	
		numberLetters = len(string.ascii_uppercase) 
		columnCodes = [ string.ascii_uppercase[i] for i in range(numberLetters) ]
		counter = 0
		for i in range(numberLetters):
			for j in range(numberLetters):
				columnCodes.append( '%s%s' % ( string.ascii_uppercase[i], string.ascii_uppercase[j] ) )	
				counter = counter + 1

		return columnCodes

	def write(self,workbook,outputFilePath):
		
		#create a temporary directory			
		tempDirectoryPath = tempfile.mkdtemp( 'zorron_excel' )
		tempPath = tempDirectoryPath + '/workbook'

		#set id sheet
		id = 1
		for sheet in workbook.sheets:
			sheet.id = id
			id = id + 1
		
		#unzip workbook template
		command = 'unzip -q %s -d %s ' % ( self.templatePath, tempDirectoryPath )
		os.system( command )

		#write metadata 
		self._writeContentTypes( workbook, tempPath )
		self._writeWorkbookRelationships( workbook, tempPath )
		self._writeWorkbook( workbook, tempPath )

		#write data
		for sheet in workbook.sheets:
			self._writeSheetFile( sheet, tempPath )	

		#compress
		os.chdir( tempPath )
		os.system( 'zip -q -r %s .' % ( outputFilePath ) )

		#remove temporary directory
		shutil.rmtree( tempDirectoryPath )
	
	def _writeContentTypes(self,workbook,outputDirectoryPath):

		cdef char* buffStr
		
		filePath = outputDirectoryPath + '/[Content_Types].xml'
		outputFile = fopen( filePath, "a" )

		for sheet in workbook.sheets:
			buff = '<Override PartName="/xl/worksheets/sheet%d.xml"  \
			       ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>\n' % ( sheet.id )
			buffStr = buff
			fprintf( outputFile, "%s", buffStr )

		buff = '</Types>'
		buffStr = buff
		fprintf( outputFile, "%s", buffStr )
		fclose( outputFile )


	def _writeWorkbookRelationships(self,workbook,outputDirectoryPath):

		cdef char *buffStr

		filePath = outputDirectoryPath + '/xl/_rels/workbook.xml.rels'
		outputFile = fopen( filePath, "a" )

		for sheet in workbook.sheets:
			buff = '<Relationship Id="rId%d" \
			       Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" \
			       Target="worksheets/sheet%d.xml"/>\n' % ( sheet.id, sheet.id )
			buffStr = buff
			fprintf( outputFile, "%s", buffStr )

		buff = '</Relationships>'
		buffStr = buff
		fprintf( outputFile, "%s", buffStr )
		fclose( outputFile )

	def _writeWorkbook(self,workbook,outputDirectoryPath):
		
		cdef char *buffStr
		
		filePath = outputDirectoryPath + '/xl/workbook.xml'
		outputFile = fopen( filePath, "a" )

		for sheet in workbook.sheets:
			buff = '<sheet name="%s" r:id="rId%d" sheetId="%d" />\n' % ( sheet.name, sheet.id , sheet.id )
			buffStr = buff
			fprintf( outputFile, "%s", buffStr )

		buff = '</sheets><calcPr calcId="0" iterate="1" /></workbook>'
		buffStr = buff
		fprintf( outputFile, "%s", buffStr )
		fclose( outputFile )
	
	def _unixEpochToExcel(self,double timestamp):

		cdef double delta = 25569.0
		cdef double daySeconds = 86400.0
		cdef double result

		result = timestamp  / daySeconds + delta

		return <float> result


	def _writeSheetFile(self,sheet,outputDirectoryPath):

		filePath = outputDirectoryPath + '/xl/worksheets/sheet%d.xml' % ( sheet.id )
		filePath_byte_string = filePath.encode("UTF-8")
		cdef char* outputFilePath = filePath_byte_string
		cdef FILE* outputFile
		cdef int numberRows
		cdef int numberColumns
		cdef int r
		cdef int c
		cdef double timestampValue
		cdef float excelValue
		cdef float numberValue
		cdef char* stringValue
		cdef char* headerStr
		cdef char* footerStr
		cdef char* columnCode
		cdef np.ndarray dataArray
		cdef unicode unicodeString
		cdef bytes   byteString
	
		outputFile = fopen( outputFilePath, "w" )

		dataArray     = sheet.dataArray
		numberRows    = dataArray.shape[0]
		numberColumns = len( dataArray[0] )

		#HEADER
		header = '<?xml version="1.0" encoding="UTF-8"?> \n\
			<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" \
			xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships" \
			xmlns:mc="http://schemas.openxmlformats.org/markup-compatibility/2006"  \
			xmlns:x14ac="http://schemas.microsoft.com/office/spreadsheetml/2009/9/ac" mc:Ignorable="x14ac" >\n'

		header = header + '<dimension ref="A1:%s%d"/>\n' % ( self.columnCodes[numberColumns-1], numberRows + 1 ) 

		header = header + '<sheetViews> \
    				   <sheetView tabSelected="1" workbookViewId="0"> \
      					<selection activeCell="A1" /> \
    				   </sheetView> \
  				   </sheetViews> \
  				  <sheetFormatPr baseColWidth="10" defaultColWidth="13.140625" defaultRowHeight="25.5" customHeight="1" x14ac:dyDescent="0.25" />\n'

		#column type
		writeColumns = False
		columns = '<cols>\n'
		i = 1
		for column in sheet.columns:
			if column.type == 'T':
				columns = columns + '<col min="%d" max="%d"  width="20" style="1" />\n' % ( i, i ) 
				writeColumns = True
			i = i + 1
		columns = columns + '</cols>\n'

		if writeColumns:
			header = header + columns

		headerStr = header
		fprintf( outputFile, "%s", headerStr )

		#BODY
		header = '<sheetData>'	
		headerStr = header
		fprintf( outputFile, "%s", headerStr )

		#COLUMN NAMES
		header = '<row r="%d" spans="1:%d" x14ac:dyDescent="0.25" >' % ( 1, numberColumns )
		headerStr = header
		fprintf( outputFile, "%s", headerStr )
		
		for c in range(numberColumns):
			column = sheet.columns[c]
			columnCode = self.columnCodes[c]
			unicodeString = unicode( column.name )
			byteString = unicodeString.encode('UTF-8')
			stringValue = byteString
			if column.type == 'T':
				fprintf( outputFile, "<c r=\"%s%d\" t=\"str\" s=\"1\" ><v>%s</v></c>" , columnCode, 1, stringValue  )
			else:
				fprintf( outputFile, "<c r=\"%s%d\" t=\"str\" ><v>%s</v></c>" , columnCode, 1, stringValue  )


		footer = '</row>'	
		footerStr = footer
		fprintf( outputFile, "%s", footerStr )
		
		#DATA
		for r in range(numberRows):
			
			header = '<row r="%d" spans="1:%d" x14ac:dyDescent="0.25" >' % ( r+2, numberColumns )
			headerStr = header
			fprintf( outputFile, "%s", headerStr )

			for c in range(numberColumns):
			
				columnType = sheet.columns[c].type
				columnCode = self.columnCodes[c]

				#FLOAT
				if columnType == 'N':
					numberValue = dataArray[r][c]
					if isnan( numberValue ):
						fprintf( outputFile,"<c r=\"%s%d\" t=\"n\" ><v></v></c>" , columnCode, r+2 )
					else:
						fprintf( outputFile,"<c r=\"%s%d\" t=\"n\" ><v>%f</v></c>" , columnCode, r+2, numberValue  )
				#STRING
				elif columnType == 'S':
					unicodeString = unicode( dataArray[r][c] )
					byteString = unicodeString.encode('UTF-8')
					stringValue = byteString
					fprintf( outputFile, "<c r=\"%s%d\" t=\"str\" ><v>%s</v></c>" , columnCode, r+2, stringValue  )

				#TIMESTAMP
				elif columnType == 'T':
					timestampValue = float(dataArray[r][c])
					fprintf( outputFile, "<c r=\"%s%d\" s=\"1\"><v>%f</v></c>" , columnCode, r+2, timestampValue  )
					

			footer = '</row>'	
			footerStr = footer
			fprintf( outputFile, "%s", footerStr )

		#FOOTER
		footer = '\n</sheetData> \
			    <pageMargins left="0.7" right="0.7" top="0.75" bottom="0.75" header="0.3" footer="0.3"/> \
  			    <pageSetup orientation="portrait" r:id="rId1"/></worksheet>'
		footerStr = footer
		fprintf( outputFile, "%s", footerStr )
				
		fclose( outputFile )
