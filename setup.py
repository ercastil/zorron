from setuptools import setup, find_packages
from distutils.extension import Extension
from Cython.Build import cythonize

ext_modules = [
		Extension( 'zorron.numpyencoding', [ 'zorron/numpyencoding.c' ] ),
		Extension( 'zorron.timefilter', [ 'zorron/timefilter.c' ] ),
	]

ext_modules +=  cythonize( [ Extension( 'zorron.excelwriter', [ 'zorron/excelwriter.pyx' ] )  ] )

setup(
	name = "zorron",
	version = "1.21",
	packages = find_packages(),
	scripts = [  	'scripts/zncreatedb.py',
			'scripts/zncreatetb.py',
			'scripts/znupdatetb.py',
			'scripts/znserver.py',
			'scripts/znwebserver.py',
			'scripts/znrequest.py',
			'scripts/znhandler.py' 
		],
#	install_requires = [
#				'setuptools',
#				'numpy>=1.7.1',
#				'scipy>=0.12.0',
#				'pandas>=0.12.0',
#				'tables>=3.0.0',
#				'Pyro4>=4.22'
#			   ],

	package_data = { 'zorron' : [ 'data/*' ] },
	ext_modules = ext_modules,

	author = "Ernesto Castillo N.",
	author_email = "ernesto.cast.nav@gmail.com",
	description = "Meteorological Time Series DataBase Framework",
	url = "http://zorron.multi-vergo.org",
	license = "GPL v3.0",
	platform = "GNU/Linux"
)
