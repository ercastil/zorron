#!/usr/bin/env python

import scipy.io 
import sys
import os

def main(inputPath):

	matlabDirectory = os.path.join( inputPath, 'mat' )
	variables = []

	fileNames = os.listdir( matlabDirectory )
	for fileName in fileNames:
		try:
			inputPath = os.path.join( matlabDirectory, fileName )
			readMatlabFile( inputPath, variables )
		except:
			sys.stderr.write( ("ERROR: file %s processing failed\n") % ( fileName ) )	
			raise

	for variable in variables:
		print '%s||%s' % ( variable[0], variable[1] )

def readMatlabFile(inputPath,variables):
	
	content = scipy.io.loadmat( inputPath )

	array = content['obs']
	internalArray = array[0][0]
	variableArray = internalArray['vars']

	tableCode 	= internalArray['stcode'][0]
	variableNames = [ varItem[0] for varItem in variableArray[0] ]

	variables.append( ( tableCode, variableNames ) )

#read parameters
inputDirectoryPath = sys.argv[1]
main( inputDirectoryPath )
