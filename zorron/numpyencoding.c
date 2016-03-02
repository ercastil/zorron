/*
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
*/


#include <Python.h>
#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION

#include <numpy/arrayobject.h>
#include <numpy/ndarraytypes.h>
#include <stdio.h>
#include <stdint.h>


//encode array
static PyObject *encodeDoubleArrayToJSON( PyObject *self, PyObject *args )
{
	PyArrayObject 	*array;
	double *dataPtr;

	PyArg_ParseTuple( args, "O!", &PyArray_Type, &array );
	
	//ARRAY ELEMENTS
	npy_intp *dimensions = PyArray_DIMS( array );
	int arraySize = (int) dimensions[0];
	dataPtr = (double*) PyArray_DATA( array );

	//CREATE JSON STRING
	int doubleLength     	= 16;
	int separatorLength 	= 1;
	int bracketsLength	= 2;
	int totalLength		= bracketsLength + arraySize * ( separatorLength + doubleLength );

	char *jsonBuffer = (char*)malloc(totalLength);
	char *jsonStringPtr = jsonBuffer;

	int stringLength;
	int i;
	for(i=0;i<arraySize;i++)
	{
		stringLength = sprintf(jsonStringPtr,"%f,",*dataPtr);
		jsonStringPtr = jsonStringPtr + stringLength;
		++dataPtr;
	}

	PyObject *jsonString = Py_BuildValue("s",jsonBuffer);

	return jsonString;
}

//encode time series to JSON
static PyObject *encodeTimeSeriesToJSON( PyObject *self,PyObject *args )
{
	PyArrayObject 	*timeStampArray;
	PyArrayObject 	*valuesArray;

	int64_t *timeStampPtr;
	double *valuesPtr;

	PyArg_ParseTuple( args, "O!O!", &PyArray_Type, &timeStampArray,&PyArray_Type, &valuesArray );
	
	//ARRAY FEATURES
	npy_intp *dimensions = PyArray_DIMS( timeStampArray );
	int arraySize = (int) dimensions[0];

	timeStampPtr = (int64_t*)PyArray_DATA( timeStampArray );
	valuesPtr = (double*)PyArray_DATA( valuesArray );

	//CREATE JSON STRING
	int timeStampLength 	= 13;
	int doubleLength     	= 16;
	int symbolsLength 	= 5;

	int bracketsLength	= 6;
	int totalLength		= bracketsLength + arraySize * ( symbolsLength + timeStampLength + doubleLength );

	char *jsonBuffer = (char*)malloc(totalLength);
	char *jsonStringPtr = jsonBuffer;
	
	int writtenBytes=0;
	int stringLength;
	int i;
	
	sprintf(jsonStringPtr,"$*[");
	jsonStringPtr = jsonStringPtr + 3;
	
	for(i=0;i<arraySize;i++)
	{
		stringLength = sprintf(jsonStringPtr,"[%ld,%f],",*timeStampPtr/1000000,*valuesPtr);
		jsonStringPtr = jsonStringPtr + stringLength;
		writtenBytes = writtenBytes + stringLength;

		++timeStampPtr;
		++valuesPtr;
	}
	
	jsonStringPtr = jsonBuffer + writtenBytes + 2;
	sprintf(jsonStringPtr,"]*$");
	jsonStringPtr = jsonStringPtr + 3;
	*jsonStringPtr = '\0';

	PyObject *jsonString = Py_BuildValue("s",jsonBuffer);

	free( jsonBuffer );

	return jsonString;
}

static PyMethodDef numpyencodingMethods[] =
{
	{ "encodeTimeSeriesToJSON", encodeTimeSeriesToJSON, METH_VARARGS, "xxx" },
	{ "encodeDoubleArrayToJSON", encodeDoubleArrayToJSON, METH_VARARGS, "xxx" },
	{ NULL, NULL, 0, NULL}
};

PyMODINIT_FUNC
initnumpyencoding(void)
{
	(void) Py_InitModule( "numpyencoding", numpyencodingMethods );
	import_array();
}
