# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
NumPy extensions.
"""

import numpy as np


def index_intersect(a1, a2):
  """
  Find indexes of elements common to arrays a1 and a2.

  a1 and a2 must have the same dtype and contain no duplicate
  elements. Return a tuple of two arrays that contain indexes of
  common elements with respect to a1 and a2.
  """
  assert a1.dtype == a2.dtype
  dtype = np.dtype([('item', a1.dtype), ('idx', np.int32)])
  b = np.empty(a1.size+a2.size, dtype)
  b['item'][:a1.size] = a1
  b['item'][a1.size:] = a2
  b['idx'][:a1.size] = np.arange(a2.size, a2.size+a1.size)
  b['idx'][a1.size:] = np.arange(a2.size)
  b.sort(order='item')
  mask = b[:-1]['item'] == b[1:]['item']
  return b[1:][mask]['idx'] - a2.size, b[mask]['idx']
