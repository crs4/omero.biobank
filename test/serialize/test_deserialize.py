# BEGIN_COPYRIGHT
# END_COPYRIGHT

import os, unittest, logging, shutil
logging.basicConfig(level=logging.ERROR)


from bl.vl.kb import KnowledgeBase as KB
import bl.vl.serialize.deserialize as ds

OME_HOST = os.getenv("OME_HOST", "localhost")
OME_USER = os.getenv("OME_USER", "root")
OME_PASS = os.getenv("OME_PASS", "romeo")


STUDY_YML = """
study_001:
  type: Study
  configuration:
    label: "A001"
    description: "Study 001"

study_002:
  type: Study
  configuration:
    label: "A002"
    description: "Study 002"
"""

ACTION_YML = """
action_0001:
  type: Action
  configuration:
    vid: V0D3A0DADA687147BDABD32E6784FFA8D6
    setup: {by_ref: setup_0001}
    device: {by_ref: device_0001}
    actionCategory: IMPORT
    operator: "Alfred E. Neumann"
    context: {by_ref: study_0001}
    description: some sort of description

setup_0001:
  type: ActionSetup
  configuration:
    label: asetup-label-0001
    conf: {param1: "foo", param2: "fii", param3: "foom"}

device_0001:
  type: Device
  configuration:
    label: device_0001_label
    maker: maker0001
    model: model0001
    release: release0001

study_0001:
  type: Study
  configuration:
    label: study_0001_label
    description: No description
"""

TUBE_YML = """
tube_0001:
  type: Tube
  configuration:
    label: tube_0001_label
    barcode: 93209409239402
    status: CONTENTUSABLE
    content: DNA
    currentVolume: 1.0
    initialVolume: 1.0
    action: {by_vid: V0D3A0DADA687147BDABD32E6784FFA8D6}
"""

PLATE_WELL_YML = """
action_on_vessel_0001:
  type: ActionOnVessel
  configuration:  
    setup: {by_label: asetup-label-0001}
    device: {by_label: device_0001_label}
    actionCategory: IMPORT
    operator: "Alfred E. Neumann"
    context: {by_label: study_0001_label}
    target: {by_label: tube_0001_label}
    description: some sort of description

plate_well_0001:
  type: PlateWell
  configuration:
    label: A05
    container: {by_ref: titer_plate_0001}
    status: CONTENTUSABLE
    content: DNA
    currentVolume: 1.0
    initialVolume: 1.0
    action: {by_ref: action_on_vessel_0001}
    
titer_plate_0001:
  type: TiterPlate
  configuration:
    label: titer_plate_0001
    barcode: 320904932049320
    status: INPREPARATION
    rows: 12
    columns: 8
    action: {by_vid: V0D3A0DADA687147BDABD32E6784FFA8D6}    
"""

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

  def read_defs(self, fname, yml):
    with open(fname, 'w') as f:
      f.write(yml)
    with open(fname) as f:
      for o in ds.deserialize_stream(self.kb, f):
        print 'o:', o
        self.kill_list.append(o.save())
        print 'id: {}'.format(o.id)
    
  def test_simple(self):
    fname = os.path.join(YMLS_DIR, 'study.yml')
    self.read_defs(fname, STUDY_YML)

  def test_internal_refs(self):
    fname = os.path.join(YMLS_DIR, 'action.yml')
    self.read_defs(fname, ACTION_YML)

  def test_external_refs_by_vid(self):
    fname = os.path.join(YMLS_DIR, 'action.yml')
    self.read_defs(fname, ACTION_YML)
    fname = os.path.join(YMLS_DIR, 'tube.yml')
    self.read_defs(fname, TUBE_YML)

  def test_external_refs_by_label(self):
    fname = os.path.join(YMLS_DIR, 'action.yml')
    self.read_defs(fname, ACTION_YML)
    fname = os.path.join(YMLS_DIR, 'tube.yml')
    self.read_defs(fname, TUBE_YML)
    fname = os.path.join(YMLS_DIR, 'plate_well.yml')
    self.read_defs(fname, PLATE_WELL_YML)
        
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
