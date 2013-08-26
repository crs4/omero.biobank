# BEGIN_COPYRIGHT
# END_COPYRIGHT

import os, unittest, logging, shutil
logging.basicConfig(level=logging.ERROR)


from bl.vl.kb import KnowledgeBase as KB
import bl.vl.kb.serialize.deserialize as ds
import bl.vl.kb.serialize.writers as writers
import uuid

OME_HOST = os.getenv("OME_HOST", "localhost")
OME_USER = os.getenv("OME_USER", "root")
OME_PASS = os.getenv("OME_PASS", "romeo")

def write_action(ostream, oid, target=None, target_class=None, vid=None):
      asetup_label  = str(uuid.uuid1())
      adevice_label = str(uuid.uuid1())
      astudy_label = str(uuid.uuid1())
      
      writers.write_action_setup(ostream, asetup_label, asetup_label)
      writers.write_device(ostream, adevice_label, adevice_label, 
                           'maker_01', 'model_01', 'release_01')
      writers.write_study(ostream, astudy_label, astudy_label)
      writers.write_action(ostream, oid, 
                           writers.by_ref(asetup_label), 
                           writers.by_ref(adevice_label), 
                           "IMPORT", "Alfred E. Neumann",
                           writers.by_ref(astudy_label),
                           target=target, target_class=target_class,
                           vid=vid)


YMLS_DIR='./ymls'

if os.path.exists(YMLS_DIR):
    shutil.rmtree(YMLS_DIR)
os.mkdir(YMLS_DIR)

class TestDeserialize(unittest.TestCase):

  def __init__(self, name):
    self.kill_list = []
    super(TestDeserialize, self).__init__(name)

  def setUp(self):
    self.kb = KB(driver='omero')(OME_HOST, OME_USER, OME_PASS)

  def tearDown(self):
    while self.kill_list:
      self.kb.delete(self.kill_list.pop())

  def check_object(self, o, conf, otype):
    try:
      self.assertTrue(isinstance(o, otype))
      for k in conf.keys():
        v = conf[k]
        # FIXME this is omero specific...
        if hasattr(v, 'ome_obj'):
          self.assertEqual(getattr(o, k).id, v.id)
          self.assertEqual(type(getattr(o, k)), type(v))
        elif hasattr(v, '_id'):
          self.assertEqual(getattr(o, k)._id, v._id)
        else:
          self.assertEqual(getattr(o, k), v)
    except:
      pass

  def read_defs(self, fname):
    with open(fname) as f:
      for o in ds.deserialize_stream(self.kb, f):
        print 'o:', o
        self.kill_list.append(o.save())
        print 'id: {}'.format(o.id)
    
  def test_simple(self):
    fname = os.path.join(YMLS_DIR, 'study.yml')
    with open(fname, 'w') as o:
      writers.write_study(o, 'study_001', 'study_001')
      writers.write_study(o, 'study_002', 'study_002')      
    self.read_defs(fname)

  def test_internal_refs(self):
    fname = os.path.join(YMLS_DIR, 'action.yml')
    with open(fname, 'w') as o:
      write_action(o, 'act_01')
    self.read_defs(fname)

  def test_external_refs_by_vid(self):
    fname = os.path.join(YMLS_DIR, 'action.yml')
    with open(fname, 'w') as o:
      write_action(o, 'act_01', vid='V0D3A0DADA687147BDABD32E6784FFA8D6')
    self.read_defs(fname)
    fname = os.path.join(YMLS_DIR, 'tube.yml')
    with open(fname, 'w') as o:
      writers.write_tube(o, 'tube_01', 'tube_01', '90909090', 'DNA',
                         'CONTENTUSABLE',
                         writers.by_vid('V0D3A0DADA687147BDABD32E6784FFA8D6'))
      writers.write_tube(o, 'tube_02', 'tube_02', '90909091', 'DNA',
                         'CONTENTUSABLE',
                         writers.by_vid('V0D3A0DADA687147BDABD32E6784FFA8D6'))
    self.read_defs(fname)

  def test_external_refs_by_label(self):
    fname = os.path.join(YMLS_DIR, 'external_refs_by_label.yml')
    with open(fname, 'w') as o:
      write_action(o, 'act_01', vid='V0D3A0DADA687147BDABD32E6784FFA8D6')
      writers.write_tube(o, 'tube_01', 'tube_01', '90909090', 'DNA',
                         'CONTENTUSABLE',
                         writers.by_ref('act_01'))
    self.read_defs(fname)      
    fname = os.path.join(YMLS_DIR, 'plate_well.yml')
    with open(fname, 'w') as o:
      writers.write_titer_plate(o, 'titer_plate_01', 'titer_plate_01', 
                                '9090909', 'READY', 8, 12, 
                                writers.by_vid('V0D3A0DADA687147BDABD32E6784FFA8D6'))
      write_action(o, 'act_02', 
                   target=writers.by_label('tube_01'), target_class='Vessel')
      writers.write_plate_well(o, 'titer_plate_01:A02', 'A02', 
                               writers.by_ref('titer_plate_01'),
                               'DNA', 'CONTENTUSABLE',
                               writers.by_ref('act_02'))
    self.read_defs(fname)
        
def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestDeserialize('test_simple'))  
  suite.addTest(TestDeserialize('test_internal_refs'))    
  suite.addTest(TestDeserialize('test_external_refs_by_vid'))      
  suite.addTest(TestDeserialize('test_external_refs_by_label'))
  return suite

if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
