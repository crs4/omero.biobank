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
  if a1.dtype != a2.dtype:
    raise ValueError("arrays must be of the same type")
  for a in a1, a2:
    if a.size != np.unique(a).size:
      raise ValueError("arrays must contain no duplicate elements")
  dtype = np.dtype([('item', a1.dtype), ('idx', np.int32)])
  b = np.empty(a1.size+a2.size, dtype)
  b['item'][:a1.size] = a1
  b['item'][a1.size:] = a2
  b['idx'][:a1.size] = np.arange(a2.size, a2.size+a1.size)
  b['idx'][a1.size:] = np.arange(a2.size)
  b.sort(order='item')
  mask = b[:-1]['item'] == b[1:]['item']
  return b[1:][mask]['idx'] - a2.size, b[mask]['idx']

def argsort_split(a, kind='mergesort'):
  """
  Return a list of indices arrays that sort subsets of a in a strictly
  increasing order. The first corresponds to the list of the first
  appearance of unique values of a, the next to the list of first
  second appearance of items in a, and so on.

  .. code:: python
      a = array([1, 3, 2, 1, 0, 3, 0, 4, 3, 2])
      argsort_split(a)
      >>> [array([4, 0, 2, 1, 7]), array([6, 3, 9, 5]), array([8])]
      for idx in argsplit(a):
           print idx, a[idx]
      >>> [4 0 2 1 7] [0 1 2 3 4]
          [6 3 9 5] [0 1 2 3]
          [8] [3]
  """
  def split_index(idx):
    flag = np.hstack([[True], a[idx][1:] != a[idx][:-1]])
    return idx[flag], idx[~flag]
  result = []
  ia = a.argsort(kind=kind)
  ia1, ia2 = split_index(ia)
  result.append(ia1)
  while len(ia2) > 0:
    ia1, ia2 = split_index(ia2)
    result.append(ia1)
  return result
