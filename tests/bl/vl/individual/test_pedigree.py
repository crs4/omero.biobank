import unittest, os, csv

import bl.vl.individual.pedigree as ped
from bl.vl.app.importer.individual import Ind, make_ind_by_label


D = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(D, "data")


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


class Individual(object):
  
  def __init__(self, iid, sex, father=None, mother=None):
    self.id = iid
    self.sex = sex
    self.father = father
    self.mother = mother

  def __hash__(self):
    return hash(self.id)
  
  def __eq__(self, obj):
    return self.id == obj.id

  def __ne__(self, obj):
    return not self.__eq__(obj)


def read_ped_file(pedfile):
  inds, genotyped_map = {}, {}
  with open(pedfile) as f:
    for l in f:
      fields = l.split()
      fam_label, label, father, mother, sex, genotyped = fields
      inds[label] = Individual(label, sex, father, mother)
      genotyped_map[label] = genotyped != '0'
  for label in inds:
    inds[label].father = inds.get(inds[label].father, None)
    inds[label].mother = inds.get(inds[label].mother, None)
  return inds.values(), genotyped_map


def manifest_reader(fn=os.path.join(DATA_DIR, 'manifest.txt')):
  with open(fn) as f:
    for l in f:
      if not l.isspace():
        cb, fname = l.split()
        cb = int(cb)
        yield cb, fname


class TestPedigree(unittest.TestCase):
  
  def setUp(self):
    pass

  def tearDown(self):
    pass

  def assertEqualFamilies(self, fam_a, fam_b):
    self.assertEqual(len(fam_a), len(fam_b))
    self.assertEqual(set(fam_a), set(fam_b))

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
    F, NF, D, C, CH = ped.analyze(family)
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

  def test_import_pedigree(self):
    with open(os.path.join(DATA_DIR, "individuals.tsv")) as f:
      reader = csv.DictReader(f, delimiter='\t')
      records = [r for r in reader]
    by_label = make_ind_by_label(records)
    ped.import_pedigree(Recorder(), by_label.itervalues())

  def test_compute_bit_complexity(self):
    for cb, fname in manifest_reader():
      family, genotyped = read_ped_file(os.path.join(DATA_DIR, fname))
      cbn = ped.compute_bit_complexity(family, genotyped)
      self.assertEqual(cb, cbn)

  def test_grow_family(self):
    for cb, fname in manifest_reader():
      family, genotyped = read_ped_file(os.path.join(DATA_DIR, fname))
      founders, non_founders, dangling, couples, children = ped.analyze(family)
      for i in family:
        new_family = ped.grow_family([i], children, genotyped, cb)
        self.assertEqualFamilies(family, new_family)
      for i in family:
        new_family = ped.grow_family([i], children, genotyped, max(0, cb-2))
        self.assertTrue(len(new_family) <= len(family))

  def test_propagate_family(self):
    for cb, fname in manifest_reader():
      family, genotyped = read_ped_file(os.path.join(DATA_DIR, fname))
      founders, non_founders, dangling, couples, children = ped.analyze(family)
      for i in family:
        new_family = ped.propagate_family([i], children)
        self.assertEqualFamilies(family, new_family)

  def test_split_disjoint(self):
    family, genotyped = read_ped_file(os.path.join(DATA_DIR, 'ped_soup.ped'))
    self.assertEqual(len(family), 7711)
    founders, non_founders, dangling, couples, children = ped.analyze(family)
    splits = ped.split_disjoint(family, children)
    self.assertEqual(sum(map(len, splits)), len(family))
    self.assertEqual(set(family), set().union(*map(set, splits)))

  def test_split_family(self):
    family, genotyped = read_ped_file(os.path.join(DATA_DIR, 'ped_soup.ped'))
    self.assertEqual(len(family), 7711)
    founders, non_founders, dangling, couples, children = ped.analyze(family)
    splits = ped.split_disjoint(family, children)
    fams = []
    max_complexity = ped.MAX_COMPLEXITY
    for f in splits:
      cbn = ped.compute_bit_complexity(f, genotyped)
      if cbn > max_complexity:
        subfs = ped.split_family(f, genotyped, max_complexity)
        subfs_i = set().union(*map(set, subfs))
        self.assertTrue(len(f) >= len(subfs_i))
        self.assertTrue(len(set(f) - subfs_i) >= 0)
        for s in subfs:
          self.assertTrue(
            ped.compute_bit_complexity(s, genotyped) <= max_complexity
            )


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestPedigree('test_analyze'))
  suite.addTest(TestPedigree('test_import_pedigree'))
  suite.addTest(TestPedigree('test_compute_bit_complexity'))
  suite.addTest(TestPedigree('test_grow_family'))
  suite.addTest(TestPedigree('test_propagate_family'))
  suite.addTest(TestPedigree('test_split_disjoint'))
  suite.addTest(TestPedigree('test_split_family'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
