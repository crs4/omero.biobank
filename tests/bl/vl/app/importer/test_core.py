# BEGIN_COPYRIGHT
# END_COPYRIGHT

import unittest, copy
from itertools import izip

import bl.vl.app.importer.core as core


class Args(object):
  pass


class TestRecordCanonizer(unittest.TestCase):

  FIELDS = ["study", "label", "gender", "father", "mother"]
  
  RECORDS = [dict(zip(FIELDS, r)) for r in [
    ["TEST01", "I001", "male", "None", "None"],
    ["TEST01", "I002", "female", "None", "None"],
    ["TEST01", "I003", "male", "I001", "I002"],
    ["TEST01", "I004", "female", "I001", "I002"],
    ["TEST02", "I005", "male", "I003", "I004"],
    ["TEST02", "I006", "male", "I003", "I004"],
    ]]

  def setUp(self):
    self.args = Args()
    self.args.study = "FOO"
    self.args.gender = "BAR"
    self.args.father = None
    self.records = copy.deepcopy(self.RECORDS[:])
    self.canonizer = core.RecordCanonizer(self.FIELDS, self.args)

  def test_canonize(self):
    for r in self.records:
      self.canonizer.canonize(r)
    self.__check_records()

  def test_canonize_list(self):
    self.canonizer.canonize_list(self.records)
    self.__check_records()

  def __check_records(self):
    for r1, r2 in izip(self.records, self.RECORDS):
      for f in "study", "gender":
        override = getattr(self.args, f)
        self.assertEqual(r1[f], override)
        self.assertNotEqual(r2[f], override)
      for f in "label", "father", "mother":
        self.assertEqual(r1[f], r2[f])


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestRecordCanonizer('test_canonize'))
  suite.addTest(TestRecordCanonizer('test_canonize_list'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
