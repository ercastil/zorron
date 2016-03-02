class ZNError(Exception):

	def __str__(self):
		return repr(self.value)

class ZNMissingFieldError(ZNError):

	def __init__(self,field):
		self.value = "Field %s is missing" % ( field )
