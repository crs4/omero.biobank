import os, unittest, time
import itertools as it
import numpy as np

import csv

from bl.vl.individual.pedigree import import_pedigree, analyze
from bl.vl.app.importer.individual import Ind, make_ind_by_label


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
    with open("individuals.tsv") as f:
      reader = csv.DictReader(f, delimiter='\t')
      records = [r for r in reader]
    by_label = make_ind_by_label(records)
    import_pedigree(Recorder(), by_label.itervalues())

  def test_analyze(self):
    Male, Female = ['male', 'female']
    founders = [
      Ind(0, Male, None, None),
      Ind(1, Female, None, None),
      Ind(2, Male, None, None),
      Ind(3, Female, None, None),
      ]
    outsiders = [
      Ind(100, Male, None, None),
      Ind(101, Female, None, None),
      ]
    non_founders = [
      Ind(4, Male, founders[0], founders[1]),
      Ind(5, Female, founders[0], founders[1]),
      Ind(6, Male, founders[2], founders[3]),
      Ind(7, Female, founders[2], founders[3]),
      Ind(8, Male, outsiders[0], outsiders[1]),
      Ind(9, Female, outsiders[0], outsiders[1]),
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
  suite.addTest(TestProxyCore('test_analyze'))
  suite.addTest(TestProxyCore('test_import_pedigree'))
  return suite

if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
