"""
Import of Data Collection
=========================

Will read in a tsv file with the following columns::

  study    label data_sample_label
  BSTUDY   dc-01 a0390290
  BSTUDY   dc-01 a0390291
  BSTUDY   dc-02 a0390292
  BSTUDY   dc-02 0390293
  ....

This will create new DataCollection(s), whose label is defined by the
label column, and link to it, using DataCollectionItem objects,
the DataSample object identified by data_sample_label.

Record that point to an unknown (data_sample_label) will abort the
data collection loading. Previously seen collections will be noisily
ignored too. No, it is not legal to use the importer to add items to a
previously known collection.
"""

from core import Core, BadRecord

import csv, json

import itertools as it

#-----------------------------------------------------------------------------
#FIXME this should be factored out....

import logging, time
logger = logging.getLogger()
counter = 0
def debug_wrapper(f):
  def debug_wrapper_wrapper(*args, **kv):
    global counter
    now = time.time()
    counter += 1
    logger.debug('%s[%d] in' % (f.__name__, counter))
    res = f(*args, **kv)
    logger.debug('%s[%d] out (%f)' % (f.__name__, counter, time.time() - now))
    counter -= 1
    return res
  return debug_wrapper_wrapper
#-----------------------------------------------------------------------------

class Recorder(Core):
  def __init__(self, study_label=None,
               host=None, user=None, passwd=None, keep_tokens=1,
               batch_size=1000, operator='Alfred E. Neumann'):
    super(Recorder, self).__init__(host, user, passwd, keep_tokens,
                                   study_label)
    self.batch_size = batch_size
    self.operator = operator
    self.device = self.get_device(label='importer.data_collection',
                                  maker='CRS4', model='importer', release='0.1')
    self.known_data_samples = {}
    self.known_data_collections = {}

  def record(self, records):
    #--
    if not records:
      self.logger.warn('no records')
      return
    self.choose_relevant_study(records)
    self.preload_data_samples()
    self.preload_data_collections()
    #--
    by_dc_label = {}
    for r in records:
      by_dc_label.setdefault(r['label'], []).append(r)
    #--
    for k in by_dc_label:
      if k in self.known_data_collections:
        self.logger.warn('data collection %s is already in VL, rejecting' % k)
      else:
        records = self.do_consistency_checks(k, by_dc_label[k])
        if records:
          self.process_chunk(records, k)

  def choose_relevant_study(self, records):
    if self.default_study:
      return
    study_label = records[0]['study']
    for r in records:
      if r['study'] != study_label:
        m = 'all records should have the same study label'
        self.logger.critical(m)
        raise ValueError(m)
    self.default_study = self.get_study(study_label)

  def preload_data_samples(self):
    self.logger.info('start prefetching data samples')
    ds = self.kb.get_objects(self.kb.DataSample)
    for d in ds:
      self.known_data_samples[d.label] = d
    self.logger.info('there are %d DataSample(s) in the kb'
                     % (len(self.known_data_samples)))

  def preload_data_collections(self):
    self.logger.info('start prefetching data collections')
    ds = self.kb.get_objects(self.kb.DataCollection)
    for d in ds:
      self.known_data_collections[d.label] = d
    self.logger.info('there are %d DataCollection(s) in the kb'
                     % (len(self.known_data_collections)))

  #----------------------------------------------------------------

  def do_consistency_checks(self, k, records):
    self.logger.info('start consistency checks %s' % k)
    #--
    for r in records:
      if not r['data_sample_label'] in self.known_data_samples:
        self.logger.error('bad data_sample_label (%s) in %s. Rejecting it' %
                          (r['data_sample_data'], r['label']))
        return []
    self.logger.info('done consistency checks %s' % k)
    #--
    return records

  def process_chunk(self, chunk, dc_label):
    asetup = self.get_action_setup('importer.data_collection',
                                   {'study_label' : self.default_study.label,
                                    'operator' : self.operator,
                                    'data_collection_label' : dc_label})
    #--
    conf = {'setup' : asetup,
            'device': self.device,
            'actionCategory' : self.kb.ActionCategory.PROCESSING,
            'operator' : self.operator,
            'context'  : self.default_study,
            }
    action = self.kb.factory.create(self.kb.Action, conf).save()
    #--
    dc_conf = {'label' : dc_label,
               'action' : action,
               }
    dc = self.kb.factory.create(self.kb.DataCollection, dc_conf).save()
    #--
    dc_items = []
    for r in chunk:
      ds_label = r['data_sample_label']
      dci_conf = {'dataSample' : self.known_data_samples[ds_label],
                  'dataCollection' : dc
                  }
      dc_items.append(self.kb.factory.create(self.kb.DataCollectionItem,
                                             dci_conf))
    #--
    self.kb.save_array(dc_items)

def make_parser_data_collection(parser):
  parser.add_argument('-S', '--study', type=str,
                      help="""default study used as context
                      for the import action.  It will
                      over-ride the study column value.""")

def import_data_collection_implementation(args):
  recorder = Recorder(args.study,
                      host=args.host, user=args.user, passwd=args.passwd,
                      keep_tokens=args.keep_tokens)
  #--
  f = csv.DictReader(args.ifile, delimiter='\t')
  logger.info('start processing file %s' % args.ifile.name)
  records = [r for r in f]
  recorder.record(records)
  logger.info('done processing file %s' % args.ifile.name)
  #--

help_doc = """
import a new data collection definition into a virgil system.
"""

def do_register(registration_list):
  registration_list.append(('data_collection', help_doc,
                            make_parser_data_collection,
                            import_data_collection_implementation))

