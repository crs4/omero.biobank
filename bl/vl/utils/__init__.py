"""
Generic Utilities
=================

This package contains generic utilities used by other modules.
"""

import uuid


DEFAULT_PREFIX = 'V'
DEFAULT_DIGIT = '0'


def make_vid(prefix=DEFAULT_PREFIX, digit=DEFAULT_DIGIT):
  return '%s%s%s' % (prefix, digit, uuid.uuid4().hex.upper())


DEFAULT_VID_LEN = len(make_vid(prefix=DEFAULT_PREFIX, digit=DEFAULT_DIGIT))
