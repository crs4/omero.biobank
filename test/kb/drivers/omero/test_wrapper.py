# BEGIN_COPYRIGHT
# END_COPYRIGHT

import unittest
import bl.vl.kb.drivers.omero.wrapper as wp


class Foo(wp.OmeroWrapper):
  
  OME_TABLE = 'Study'
  __fields__ = []

  def __init__(self, ome_obj, proxy):
    super(Foo, self).__init__(ome_obj, proxy)

  def get_bar(self):
    return self.bare_getattr('bar')
  
  def set_bar(self, v):
    self.bare_setattr('bar', v)


class Bar(wp.OmeroWrapper):
  
  OME_TABLE = 'Study'
  __fields__ = [('label', wp.STRING, wp.REQUIRED)]

  def __preprocess_conf__(self, conf):
    assert not 'label' in conf
    conf['label'] = 'foo'
    return super(Bar, self).__preprocess_conf__(conf)


class TestOmeroWrapper(unittest.TestCase):

  def test_bare_attrs(self):
    f = Foo(None, None)
    self.assertFalse(hasattr(f, 'bar'))
    f.set_bar(22)
    self.assertTrue(hasattr(f, 'bar'))
    self.assertEqual(f.get_bar(), 22)

  def test_recursive_preprocess_conf(self):
    b = Bar(None, None)
    b.configure({})


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestOmeroWrapper('test_bare_attrs'))
  suite.addTest(TestOmeroWrapper('test_recursive_preprocess_conf'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
