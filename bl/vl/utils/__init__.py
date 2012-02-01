# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
Generic Utilities
=================

Generic utilities used by other modules.
"""

import uuid, hashlib


DEFAULT_BUFSIZE = 16777216
DEFAULT_PREFIX = 'V'
DEFAULT_DIGIT = '0'


def make_vid(prefix=DEFAULT_PREFIX, digit=DEFAULT_DIGIT):
  return '%s%s%s' % (prefix, digit, uuid.uuid4().hex.upper())


DEFAULT_VID_LEN = len(make_vid(prefix=DEFAULT_PREFIX, digit=DEFAULT_DIGIT))


def compute_sha1(fname, bufsize=DEFAULT_BUFSIZE):
  sha1 = hashlib.sha1()
  with open(fname) as fi:
    s = fi.read(bufsize)
    while s:
      sha1.update(s)
      s = fi.read(bufsize)
  return sha1.hexdigest()
