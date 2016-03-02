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
#include <time.h>

#define true 1
#define false 0

inline int isIn( int date, PyArrayObject *dateArray )
{
	int selected = false;
	npy_intp *dimensions = PyArray_DIMS( dateArray );
	int arraySize = (int) dimensions[0];
	size_t *dataPtr = (size_t*) PyArray_DATA( dateArray );

	if(arraySize==0)
	{
		selected = true;
		return selected;
	}

	int i;
	for(i=0;i<arraySize;i++)
	{
		if(date==dataPtr[i])
		{
			selected=true;
			break;
		}
	}

	return selected;

}

static PyObject *filterByTime( PyObject *self, PyObject *args )
{
	//PARSE INPUT
	PyArrayObject 	*timeIndex;
	PyArrayObject 	*years;
	PyArrayObject 	*months;
	PyArrayObject 	*hours;

	PyArg_ParseTuple( args, "O!O!O!O!", &PyArray_Type, &timeIndex,&PyArray_Type,&years,&PyArray_Type,&months,&PyArray_Type,&hours );
	
	//ARRAY SIZE
	npy_intp *dimensions = PyArray_DIMS( timeIndex );
	int arraySize = (int) dimensions[0];
	int64_t *timeStampPtr = (int64_t*) PyArray_DATA( timeIndex );

	//CREATE OUTPUT
	PyArrayObject *selectionArray = PyArray_SimpleNew(1,dimensions,NPY_INT);
	int *selectionPtr = (int*) PyArray_DATA( selectionArray );

	//ITERATE TIMESTAMPS
	int i;
	int selected = false;
	struct tm *dateTimePtr;	
	time_t rawTime;

	time( &rawTime );

	for(i=0;i<arraySize;i++)
	{
		selected = false;
		
		//CONVERT TIMESTAMP
		rawTime = (time_t)( *timeStampPtr / 1e9 );
		dateTimePtr = gmtime(&rawTime);

		//YEARS
		if( !isIn(dateTimePtr->tm_year + 1900,years) )
			goto selection;
			
		//MONTHS
		if( !isIn(dateTimePtr->tm_mon+1,months) )
			goto selection;

		//HOURS
		if( !isIn(dateTimePtr->tm_hour,hours) )
			goto selection;

		selected = true;

		selection:

		*selectionPtr = selected;

		++timeStampPtr;
		++selectionPtr;
	}

	return selectionArray;
}

static PyMethodDef timefilterMethods[] =
{
	{ "filterByTime", filterByTime, METH_VARARGS, "xxx" },
	{ NULL, NULL, 0, NULL}
};

PyMODINIT_FUNC
inittimefilter(void)
{
	(void) Py_InitModule( "timefilter", timefilterMethods );
	import_array();
}
