"""
Interfaces for the application-side view of the object model.

Individual kb drivers should implement these interfaces.
"""

import sys
#--
#
# The actual KnowledgeBase front-end
#
#--
driver_table = { 'omero' : 'bl.vl.genotype.kb.drivers.omero' }

def KnowledgeBase(driver):
  try:
    __import__(driver_table[driver])
    driver_module = sys.modules[driver_table[driver]]
  except KeyError, e:
    print 'Driver %s is unknown' % driver
    assert(False)
  return driver_module.driver
