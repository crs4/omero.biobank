"""
Interfaces for the application-side view of the object model.

Individual kb drivers should implement these interfaces.
"""


class KBError(Exception):
  pass


class Proxy(object):
  
  def __init__(self):
    raise NotImplementedError


class Study(object):

  def __init__(self):
    raise NotImplementedError
