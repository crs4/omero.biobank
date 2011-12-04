import os, unittest, time
import itertools as it
import numpy as np

import csv

from bl.vl.individual.pedigree import import_pedigree, analyze

class Ind(object):
  def __init__(self, label, father, mother, gender):
    self.id = label
    self.father = father if not father == 'None' else None
    self.mother = mother if not mother == 'None' else None
    self.gender = gender

class TestProxyCore(unittest.TestCase):
  def setUp(self):
    pass

  def tearDown(self):
    pass

  def test_import_pedigree(self):

    class Recorder(object):
      def __init__(self):
        pass
      def retrieve(self, i_label):
        return None
      def record(self, label, gender, father, mother):
        print 'recording %s[%s]' % (label, gender)
        return True
      def dump_out(self):
        print 'dumping out'

    def istream(fname):
      f = csv.DictReader(open(fname), delimiter='\t')
      for t in f:
        father = t['father'] if not t['father'] == 'None' else None
        mother = t['mother'] if not t['mother'] == 'None' else None
        yield Ind(t['label'], father, mother, t['gender'])
    import_pedigree(Recorder(), istream('individuals.tsv'))

  def test_analyze(self):
    Male, Female = ['male', 'female']
    founders = [
      Ind(0, None, None, Male),
      Ind(1, None, None, Female),
      Ind(2, None, None, Male),
      Ind(3, None, None, Female),
      ]
    outsiders = [
      Ind(100, None, None, Male),
      Ind(101, None, None, Female),
      ]
    non_founders = [
      Ind(4, founders[0], founders[1], Male),
      Ind(5, founders[0], founders[1], Female),
      Ind(6, founders[2], founders[3], Male),
      Ind(7, founders[2], founders[3], Female),
      Ind(8, outsiders[0], outsiders[1], Male),
      Ind(9, outsiders[0], outsiders[1], Female),
      ]
    couples = [(founders[0], founders[1]),
               (founders[2], founders[3]),
               (outsiders[0], outsiders[1])]

    family = founders + non_founders
    F, NF, D, C, CH = analyze(family)
    self.assertEqual(len(F), len(founders))
    self.assertEqual(set(F), set(founders))
    #
    self.assertEqual(len(NF), len(non_founders))
    self.assertEqual(set(NF), set(non_founders))
    #
    self.assertEqual(len(D), len(outsiders))
    self.assertEqual(set(D), set(outsiders))
    #
    self.assertEqual(len(C), len(couples))
    self.assertEqual(set(C), set(couples))
    #
    self.assertEqual(len(CH[0]), len(CH[1]))
    self.assertEqual(set(CH[0]), set(CH[1]))
    self.assertEqual(len(CH[2]), len(CH[3]))
    self.assertEqual(set(CH[2]), set(CH[3]))
    self.assertEqual(len(CH[100]), len(CH[101]))
    self.assertEqual(set(CH[100]), set(CH[101]))

def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestProxyCore('test_import_pedigree'))
  suite.addTest(TestProxyCore('test_analyze'))
  return suite

if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
