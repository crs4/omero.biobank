import os, unittest, time, logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

from bl.vl.kb import KnowledgeBase as KB
from kb_object_creator import KBObjectCreator


OME_HOST = os.getenv("OME_HOST", "localhost")
OME_USER = os.getenv("OME_USER", "root")
OME_PASS = os.getenv("OME_PASS", "romeo")


class TestKB(KBObjectCreator):

  def __init__(self, name):
    super(TestKB, self).__init__(name)
    self.kill_list = []

  def setUp(self):
    logger.info('start setup')
    self.kb = KB(driver='omero')(OME_HOST, OME_USER, OME_PASS)
    logger.info('done with setup')

  def tearDown(self):
    logger.info('start tear-down')
    self.kill_list.reverse()
    for x in self.kill_list:
      self.kb.delete(x)
    self.kill_list = []
    logger.info('done with tear-down')

  def create_archetype_record(self):
    terminology = 'terminology://apps.who.int/classifications/apps/'
    term = terminology + 'gE10.htm#E10'
    archetype = 'openEHR-EHR-EVALUATION.problem-diagnosis.v1'
    field = 'at0002.1'
    rec = {field : term}
    selector = ('(archetype=="%s")&(field=="%s")&(svalue=="%s")'
                % (archetype, field, term))
    return archetype, rec, selector

  def test_ehr_record(self):
    logger.info('creating ActionOnIndividual')
    conf, action = self.create_action_on_individual()
    self.kill_list.append(action.save())
    logger.info('done creating ActionOnIndividual')
    archetype, fields, selector = self.create_archetype_record()
    # FIXME there should be a function for this...
    timestamp = int(time.time() * 1000)
    self.kb.add_ehr_record(action, timestamp, archetype, fields)
    rs = self.kb.get_ehr_records(selector)
    individual = action.target
    individual.reload()
    self.assertTrue(len(rs) > 0)
    not_found = True
    for r in rs:
      for k in ['a_id', 'i_id', 'timestamp', 'archetype', 'fields']:
        self.assertTrue(k in r)
      if r['a_id'] == action.id:
        not_found = False
        self.assertEqual(r['i_id'], individual.id)
        self.assertEqual(r['timestamp'], timestamp)
        self.assertEqual(r['archetype'], archetype)
        nfields = r['fields']
        self.assertTrue(len(nfields) >= len(fields))
        for k in fields:
          self.assertTrue(k in nfields)
          self.assertEqual(nfields[k], fields[k])
        break
    else:
      self.assertTrue(not_found)


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestKB('test_ehr_record'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
