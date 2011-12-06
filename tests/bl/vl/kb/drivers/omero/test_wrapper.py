import unittest
from bl.vl.kb.drivers.omero.wrapper import OmeroWrapper


class TestOmeroWrapper(unittest.TestCase):
  def setUp(self):
    pass

  def tearDown(self):
    pass

  def test_bare_attrs(self):
    class Foo(OmeroWrapper):
      OME_TABLE = 'Study'
      __fields__ = []

      def __init__(self, ome_obj, proxy):
        super(Foo, self).__init__(ome_obj, proxy)

      def get_bar(self):
        return self.bare_getattr('bar')
      def set_bar(self, v):
        self.bare_setattr('bar', v)
    f = Foo(None, None)
    self.assertFalse(hasattr(f, 'bar'))
    f.set_bar(22)
    self.assertTrue(hasattr(f, 'bar'))
    self.assertEqual(f.get_bar(), 22)


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestOmeroWrapper('test_bare_attrs'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
