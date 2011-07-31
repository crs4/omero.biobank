import os, unittest, time
import itertools as it
import numpy as np

import csv

from bl.vl.individual.pedigree import import_pedigree

class TestProxyCore(unittest.TestCase):
  def setUp(self):
    pass

  def tearDown(self):
    pass

  def test_import_pedigree(self):
    class Ind(object):
      def __init__(self, label, father, mother, gender):
        self.id = label
        self.father = father if not father == 'None' else None
        self.mother = mother if not mother == 'None' else None
        self.gender = gender

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

def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestProxyCore('test_import_pedigree'))
  return suite

if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
