"""
Import of Data Collection
=========================

Will read in a tsv file with the following columns::

  study    label data_sample_label
  BSTUDY   dc-01 a0390290
  BSTUDY   dc-01 a0390291
  BSTUDY   dc-01 a0390292
  BSTUDY   dc-01 0390293
  ....

This will create a new DataCollection, whose label is defined by the
label column, and link to it, using DataCollectionItem objects,
the DataSample object identified by data_sample_label.

Record that point to an unknown (data_sample_label) will abort the
data collection loading. Previously seen collections will be noisily
ignored too. No, it is not legal to use the importer to add items to a
previously known collection.
"""

from bl.vl.sample.kb import KBError
from core import Core, BadRecord
from version import version

import csv, json
import time, sys


class Recorder(Core):
  """
  An utility class that handles the actual recording of a DataCollection
  metadata into VL.
  """
  def __init__(self, study_label=None,
               host=None, user=None, passwd=None, keep_tokens=1,
               operator='Alfred E. Neumann'):
    super(Recorder, self).__init__(host, user, passwd, keep_tokens=keep_tokens,
                                   study_label=study_label)
    #FIXME this can probably go to core....
    self.known_studies = {}
    self.device = self.get_device('importer-0.0', 'CRS4', 'IMPORT', '0.0')
    self.asetup = self.get_action_setup('importer-version-%s-%s-%f' %
                                        (version, "DataSample", time.time()),
                                        # FIXME the json below should
                                        # record the app version, and the
                                        # parameters used.  unclear if we
                                        # need to register the file we load
                                        # data from, since it is, most
                                        # likely, a transient object.
                                        json.dumps({'study' : study_label,
                                                    'operator' : operator,
                                                    'host' : host,
                                                    'user' : user}))
    self.acat  = self.acat_map['IMPORT']
    self.operator = operator
    #
    self.input_rows = {}
    self.counter = 0
    #--------------------------------------------------------------------
    #--
    self.logger.info('start prefetching DataSample(s)')
    data_samples = self.skb.get_bio_samples(self.skb.DataSample)
    self.data_samples = {}
    for ds in data_samples:
      self.data_samples[ds.label] = ds
    self.logger.info('done prefetching DataSample(s)')
    self.logger.info('there are %d DataSample(s) in the kb' %
                     len(self.data_samples))
    #--
    self.logger.info('start prefetching DataCollection(s)')
    data_collections = self.skb.get_bio_samples(self.skb.DataCollection)
    self.data_collections = {}
    #FIXME
    for dc in data_collections:
      self.data_collections[dc.label] = dc
    self.logger.info('done prefetching DataCollection(s)')
    self.logger.info('there are %d DataCollection(s) in the kb' %
                     len(self.data_collections))


  def record_collection(self, label, data):
    self.logger.info('start loading collection %s %d items' %
                     (label, len(data)))
    if self.data_collections.has_key(label):
        self.logger.critical('collection %s is already in kb' %
                             label)
        sys.exit(1)
    #--
    study = self.default_study
    if not study:
      study_label = data[0]['study']
      ff = filter(lambda x : x['study'] != study_label, data)
      if ff:
        self.logger.critical('not uniform study for collection %s' %
                             label)
        sys.exit(1)
      study = self.get_study_by_label(study_label)
    #--
    for x in data:
      if not self.data_samples.has_key(x['data_sample_label']):
        self.logger.critical('%s referred in collection %s does not exist.' %
                             (x['data_sample_label'], label))
        sys.exit(1)
    #--
    self.logger.info('registering DataCollection label=%r' % label)
    data_collection = self.skb.DataCollection(study=study, label=label)
    data_collection = self.skb.save(data_collection)
    self.logger.info('DataCollection vid=%r' % data_collection.id)
    self.logger.info('start loading actual data')
    for x in data:
      ds_label = x['data_sample_label']
      data_sample = self.data_samples[ds_label]
      action = self.create_action_on_sample(study, data_sample, json.dumps(x))
      action = self.skb.save(action)
      #-
      dc_it = self.skb.DataCollectionItem(data_collection=data_collection,
                                          data_sample=data_sample)
      dc_it.action = action
      self.skb.save(dc_it)
      #FIXME using .id not .label
      self.logger.info('saved  %s[%s]' % (data_collection.label,
                                          ds_label))
    self.logger.info('done loading')

  def create_action_on_sample(self, study, sample, description):
    return self.create_action_helper(self.skb.ActionOnSample, description,
                                     study, self.device, self.asetup,
                                     self.acat, self.operator, sample)



help_doc = """
import new data_collections definitions into a virgil system.
"""

def make_parser_data_collection(parser):
  parser.add_argument('-S', '--study', type=str,
                      help="""default context study label.
                      It will over-ride the study column value""")
  parser.add_argument('-C', '--collection', type=str,
                      help="""default collection label.
                      It will over-ride the label column value""")

def import_data_collection_implementation(args):
  recorder = Recorder(args.study,
                      host=args.host, user=args.user, passwd=args.passwd,
                      keep_tokens=args.keep_tokens)
  f = csv.DictReader(args.ifile, delimiter='\t')
  collections = {}
  default_label = args.collection
  if default_label:
    collections[default_label] = []
    for r in f:
      collections[default_label].append(r)
  else:
    for r in f:
      collections.setdefault(r['label'], []).append(r)
  for k in collections.keys():
    recorder.record_collection(k, collections[k])

def do_register(registration_list):
  registration_list.append(('data_collection', help_doc,
                            make_parser_data_collection,
                            import_data_collection_implementation))
