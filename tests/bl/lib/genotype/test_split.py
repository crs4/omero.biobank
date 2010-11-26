import unittest
import time, os

import numpy as np

import bl.lib.genotype.pedigree as ped

class individual(object):
  def __init__(self, iid, sex, father=None, mother=None, genotyped=False):
    self.id = iid
    self.sex = sex
    self.father = father
    self.mother = mother
    self.genotyped = genotyped

def read_ped_file(pedfile):
  fin = open(pedfile)
  inds = {}
  for l in fin:
    l = l.strip()
    if len(l) == 0:
      continue
    fields = l.split()
    fam_label, label, father, mother, sex, genotyped = fields
    genotyped = genotyped != '0'
    inds[label] = individual(label, sex, father, mother, genotyped)

  for k in inds.keys():
    inds[k].father = inds[inds[k].father] if inds.has_key(inds[k].father) else None
    inds[k].mother = inds[inds[k].mother] if inds.has_key(inds[k].mother) else None
  return inds.values()

class split(unittest.TestCase):

  def compute_bit_complexity(self):
    os.chdir('data')
    fin = open('manifest.txt')
    for l in fin:
      l = l.strip()
      if len(l) == 0:
        continue
      cb, fname = l.split()
      cb = int(cb)
      family = read_ped_file(fname)
      cbn = ped.compute_bit_complexity(family)
      self.assertEqual(cb, cbn)

def suite():
  suite = unittest.TestSuite()
  suite.addTest(split('compute_bit_complexity'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))

