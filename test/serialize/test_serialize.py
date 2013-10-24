# BEGIN_COPYRIGHT
# END_COPYRIGHT

# pylint: disable=E1101

import os, unittest, logging, shutil, time

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)


from bl.vl.kb import KnowledgeBase as KB
from kb_object_creator import KBObjectCreator

import bl.vl.kb.serialize.deserialize as ds
from bl.vl.kb.serialize.serializer import Serializer
from bl.vl.kb.serialize.yaml_serializer import YamlSerializer
import bl.vl.kb.serialize.writers as writers
import uuid

OME_HOST = os.getenv("OME_HOST", "localhost")
OME_USER = os.getenv("OME_USER", "root")
OME_PASS = os.getenv("OME_PASS", "romeo")


YMLS_DIR='./ymls'

if os.path.exists(YMLS_DIR):
    shutil.rmtree(YMLS_DIR)
os.mkdir(YMLS_DIR)

class DummySerializer(Serializer):
  def __init__(self):
    super(DummySerializer, self).__init__(logger=logger)
    self.conf = None
  def serialize(self, oid, klass, conf, vid):
    self.conf = conf

class TestSerialize(KBObjectCreator):

  def __init__(self, name):
    super(TestSerialize, self).__init__(name)
    self.kill_list = []

  def setUp(self):
    self.kb = KB(driver='omero')(OME_HOST, OME_USER, OME_PASS)

  def tearDown(self):
    while self.kill_list:
      self.kb.delete(self.kill_list.pop())

  def check_conf(self, conf, dconf):
    for k in conf:
      if k == 'id':
        continue
      if type(conf[k]) == float:
        scale = conf[k] + dconf[k] + 0.001
        self.assertAlmostEqual(conf[k]/scale, dconf[k]/scale)
      else:
        self.assertEqual(conf[k], dconf[k])
    
  def test_basics(self):
    conf, s = self.create_study()
    self.check_conf(conf, s.to_conf())

  def test_serialize(self):
    ds = DummySerializer()
    conf, s = self.create_study()
    s.serialize(ds)
    self.check_conf(conf, ds.conf)

  def test_yaml_serializer(self):
    import cStringIO
    output = cStringIO.StringIO()
    ys = YamlSerializer(output, logger=logger)
    conf, s = self.create_study()
    s.serialize(ys)
    print output.getvalue()
    output.close()  

  def test_yaml_serializer2(self):
    import cStringIO
    output = cStringIO.StringIO()
    ys = YamlSerializer(output, logger=logger)
    conf, a  = self.create_action()
    conf, a2 = self.create_action(device=a.device, asetup=a.setup,
                                  study=a.context)
    a.serialize(ys)
    a2.serialize(ys)    
    print output.getvalue()
    output.close()  
    
        
def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestSerialize('test_basics'))
  suite.addTest(TestSerialize('test_serialize'))  
  suite.addTest(TestSerialize('test_yaml_serializer'))    
  suite.addTest(TestSerialize('test_yaml_serializer2'))      
  return suite

if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
