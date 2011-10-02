"""
Plumbing and duct tape
======================

.. todo::

   write doc of the bl/vl/utils module.

"""

import uuid


DEFAULT_PREFIX = 'V'
DEFAULT_DIGIT = '0'


def make_vid(prefix=DEFAULT_PREFIX, digit=DEFAULT_DIGIT):
  return '%s%s%s' % (prefix, digit, uuid.uuid4().hex.upper())
