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
    fin = open(os.path.join('data', 'manifest.txt'))
    for l in fin:
      l = l.strip()
      if len(l) == 0:
        continue
      cb, fname = l.split()
      cb = int(cb)
      family = read_ped_file(os.path.join('data', fname))
      cbn = ped.compute_bit_complexity(family)
      self.assertEqual(cb, cbn)

  def assertEqualFamilies(self, fam_a, fam_b):
    self.assertEqual(len(fam_a), len(fam_b))
    self.assertEqual(set(fam_a), set(fam_b))

  def grow_family(self):
    fin = open(os.path.join('data', 'manifest.txt'))
    for l in fin:
      l = l.strip()
      if len(l) == 0:
        continue
      cb, fname = l.split()
      cb = int(cb)
      family = read_ped_file(os.path.join('data', fname))
      cbn = ped.compute_bit_complexity(family)
      founders, non_founders, couples, children = ped.analyze(family)
      for i in family:
        new_family = list(ped.grow_family(set([i]), children, cb))
        self.assertEqualFamilies(family, new_family)

  def propagate_family(self):
    fin = open(os.path.join('data', 'manifest.txt'))
    for l in fin:
      l = l.strip()
      if len(l) == 0:
        continue
      cb, fname = l.split()
      cb = int(cb)
      family = read_ped_file(os.path.join('data', fname))
      founders, non_founders, couples, children = ped.analyze(family)
      for i in family:
        new_family = list(ped.propagate_family(set([i]), children))
        self.assertEqualFamilies(family, new_family)

  def split_disjoint(self):
    family = read_ped_file(os.path.join('data', 'ped_soup.ped'))
    self.assertEqual(len(family), 7727)
    founders, non_founders, couples, children = ped.analyze(family)
    splits = ped.split_disjoint(family, children)
    self.assertEqual(sum(map(len, splits)), len(family))
    self.assertEqual(set(family), set([]).union(*(map(set, splits))))

def suite():
  suite = unittest.TestSuite()
  suite.addTest(split('compute_bit_complexity'))
  suite.addTest(split('grow_family'))
  suite.addTest(split('propagate_family'))
  suite.addTest(split('split_disjoint'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))

