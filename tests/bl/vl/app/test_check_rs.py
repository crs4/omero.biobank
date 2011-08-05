import unittest

from bl.vl.app.check_rs import mask_to_key, MaskTooShortError


N = 4
SEP = '|'
MASK_TO_KEY_PAIRS = [
  (('CACA',   ('T', 'G'), 'AACC'),   'CACA|AACC'),
  (('CACA',   ('T', 'G'), 'AACCA'),  'CACA|AACC'),
  (('ACACA',  ('T', 'G'), 'AACC'),   'CACA|AACC'),
  (('ACACA',  ('T', 'G'), 'AACCA'),  'CACA|AACC'),
  # L < N
  (('ACA',    ('T', 'G'), 'AACCA'),  'ACA|AACCA'),
  (('ACA',    ('T', 'G'), 'AACCAC'), 'ACA|AACCA'),
  # R < N
  (('ACACA',  ('T', 'G'), 'AAC'),    'ACACA|AAC'),
  (('AACACA', ('T', 'G'), 'AAC'),    'ACACA|AAC'),
  # symmetrical
  (('CACA',   ('A', 'T'), 'TGTG'),   'CACA|TGTG'),
  ]
SHORT_MASKS = [
  ('CACA',    ('T', 'G'), 'AAC'),
  ('ACA',     ('T', 'G'), 'AACC'),
  ('ACA',     ('T', 'G'), 'AAC'),
  ]


class TestMaskToKey(unittest.TestCase):

  def test_good(self):
    for i, (m, exp_k) in enumerate(MASK_TO_KEY_PAIRS):
      k = mask_to_key(m, N, sep=SEP)
      self.assertEqual(k, exp_k, "(%d): %s != %s" % (i+1, k, exp_k))

  def test_bad(self):
    for m in SHORT_MASKS:
      self.assertRaises(MaskTooShortError, mask_to_key, m, N)


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestMaskToKey('test_good'))
  suite.addTest(TestMaskToKey('test_bad'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
