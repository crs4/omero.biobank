# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
Generic Utilities
=================

Generic utilities used by other modules.
"""

import uuid, hashlib, logging


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


def get_logger(logger_label):
  logger = logging.getLogger(logger_label)
  return logger


# transform unicodes in list to strings
def decode_list(data):
    decoded = []
    for item in data:
        if isinstance(item, unicode):
            item = item.encode('utf-8')
        elif isinstance(item, list):
            item = decode_list(item)
        elif isinstance(item, dict):
            item = decode_dict(item)
        decoded,append(item)
    return decoded


# transform unicodes in dictionary to strings (both in keys and values)
def decode_dict(data):
    decoded = {}
    for key, val in data.iteritems():
        if isinstance(key, unicode):
            key = key.encode('utf-8')
        if isinstance(val, unicode):
            val = val.encode('utf-8')
        elif isinstance(val, list):
            val = decode_list(val)
        elif isinstance(val, dict):
            val = decode_dict(val)
        decoded[key] = val
    return decoded