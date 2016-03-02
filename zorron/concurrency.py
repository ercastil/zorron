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


import threading
import time

class ZNAction:

	def __init__(self,actionType,parameters):

		self.type = actionType
		self.parameters = parameters
	
	def __repr__(self):
		return self.type
	def __str__(self):
		return self.type


class ZNActionSyncronizationParameters:

	def __init__(self,condition,actionQueue,state,stop, activeProcessors, logFile ):

		self.condition   	= condition
		self.actionQueue 	= actionQueue
		self.stop	 	= stop
		self.state	 	= state
		self.activeProcessors   = activeProcessors
		self.logFile 		= logFile

class ZNActionProcessor(threading.Thread):

	def __init__(self,id,parameters):

		threading.Thread.__init__(self)
		self.id 	 = id
		self.parameters  = parameters
		self.requester   = None
		self.updater     = None
	
	def run(self):
		
		print "INICIANDO THREAD (%d)" % ( self.id )
		while True:	
			
			with self.parameters.condition:

				self.parameters.condition.acquire()
				print "%s (%d) - %s" % ( self.parameters.state, self.id , self.parameters.actionQueue )
				if self.parameters.stop:
					print "STOP (%d)" % ( self.id )
					self.parameters.condition.release()
					break

				if len( self.parameters.actionQueue ) == 0:
					print "ESPERANDO (%d)" % ( self.id )
					self.parameters.condition.wait()	

				elif self.parameters.state == 'REQUESTING':

					actionType = self.parameters.actionQueue[0].type	

					if actionType == 'REQUEST':

						action = self.parameters.actionQueue.pop()
						self.parameters.activeProcessors = self.parameters.activeProcessors + 1
						self.parameters.logFile.write( "EMPEZANDO REQUEST (%d)" % ( self.id ) )
						print "EMPEZANDO REQUEST (%d) - %s" % ( self.id, time.time() )

						self.parameters.condition.release()

						time.sleep(2)
						#self.requester.process( action )

						self.parameters.condition.acquire()
						self.parameters.logFile.write( "TERMINANDO REQUEST (%d)" % ( self.id ) )
						print "TERMINANDO REQUEST (%d) - %s" % ( self.id, time.time() )

						self.parameters.activeProcessors = self.parameters.activeProcessors - 1
						
					elif actionType == 'UPDATE':

						self.parameters.state = 'WAITING_REQUESTS'
						
				elif self.parameters.state == 'WAITING_REQUESTS':
				
					if self.parameters.activeProcessors == 0:

						self.parameters.state = 'UPDATING'
						
						action = self.parameters.actionQueue.pop()
						self.parameters.logFile.write( "EMPEZANDO UPDATE (%d)" % ( self.id ) )
						print "EMPEZANDO UPDATE (%d) - %s" % ( self.id , time.time() )

						self.parameters.condition.release()

						#self.updater.process( action )
						time.sleep(4)

						self.parameters.condition.acquire()
						self.parameters.logFile.write( "TERMINANDO UPDATE (%d)" % ( self.id ) )
						print  "TERMINANDO UPDATE (%d) - %s" % ( self.id , time.time() ) 

						self.parameters.state = 'REQUESTING'
						self.parameters.condition.notifyAll()

					self.parameters.condition.wait()

				elif self.parameters.state == 'UPDATING':

					print "ESPERANDO UPDATING (%d)" % ( self.id )
					self.parameters.condition.wait()
					self.parameters.logFile.write( "ESPERANDO UPDATING (%d)" % ( self.id ) )

				self.parameters.condition.release()

class ZNConcurrencyManager:
	
	def __init__(self,numberProcessors):
		
		condition = threading.Condition()
		actionQueue = []
		activeProcessors = 0
		state = 'REQUESTING'
		stop = False

		self.logFile = open( '/tmp/manager.log', 'w' )
		self.parameters = ZNActionSyncronizationParameters( 
									condition, 
									actionQueue, 
									state, 
									stop , 
									activeProcessors,
									self.logFile
								  )
		self.processors = []

		for i in range(numberProcessors):
			processor = ZNActionProcessor( i, self.parameters )
			self.processors.append( processor )
			self.processors[i].start()

	def enque(self,action):	
		
		with self.parameters.condition:

			self.parameters.condition.acquire()

			if self.parameters.stop == True:
				self.parameters.condition.release()
				return

			self.parameters.actionQueue.append( action )		
			self.parameters.condition.notify()
			self.parameters.condition.release()
		
	def finalize(self):

		with self.parameters.condition:

			self.parameters.condition.acquire()
			self.parameters.stop = True
			self.parameters.condition.notifyAll()
			self.parameters.condition.release()

		for processor in self.processors:
			processor.join()

		self.logFile.close()
