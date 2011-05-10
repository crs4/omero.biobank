"""
Import of Data Collection
=========================

Will read in a tsv file with the following columns::

  study    data_sample_label
  BSTUDY   a0390290
  BSTUDY   a0390291
  BSTUDY   a0390292
  BSTUDY   a0390293
  ....

This will create a new DataCollection and link to it the DataSample
object identified by data_sample_label.

Record that point to an unknown (data_sample_label) will be noisily
ignored. Previously seen collections will be noisily ignored too. No,
it is not legal to use the importer to add items to a previously known
collection.
"""

from bl.vl.sample.kb import KBError
from core import Core, BadRecord
from version import version

import csv, json
import time, sys

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
  """
  An utility class that handles the actual recording of a DataCollection
  metadata into VL.
  """
  def __init__(self, study_label=None,
               host=None, user=None, passwd=None, keep_tokens=1,
               operator='Alfred E. Neumann'):
    super(Recorder, self).__init__(host, user, passwd)
    #FIXME this can probably go to core....
    self.default_study = None
    if study_label:
      s = self.skb.get_study_by_label(study_label)
      if not s:
        raise ValueError('No known study with label %s' % study_label)
      self.logger.info('Selecting %s[%d,%s] as default study' %
                       (s.label, s.omero_id, s.id))
      self.default_study = s
    #-------------------------
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
    self.data_collection = None
    #
    self.input_rows = {}
    self.counter = 0
    #--------------------------------------------------------------------
    #--
    self.logger.info('start prefetching DataSample(s)')
    data_samples = self.skb.get_bio_samples(self.skb.DataSample)
    self.data_samples = {}
    for ds in data_samples:
      self.data_samples[ds.name] = ds
    self.logger.info('done prefetching DataSample(s)')
    self.logger.info('there are %d DataSample(s) in the kb' %
                     len(self.data_samples))

  @debug_wrapper
  def get_study_by_label(self, study_label):
    if self.default_study:
      return self.default_study
    return self.known_studies.setdefault(study_label,
                                         super(Recorder, self)\
                                         .get_study_by_label(study_label))

  @debug_wrapper
  def get_action(self, study, sample, name, maker, model, release):
    device = self.get_device(name, maker, model, release)
    asetup = self.get_action_setup('import-%s-%s' % (maker, model),
                                   json.dumps({}))
    if sample.__class__.__name__ == 'PlateWell':
      action = self.create_action_on_sample_slot(study, sample, device, asetup,
                                                 description='')
    elif sample.__class__.__name__ == 'DNASample':
      action = self.create_action_on_sample(study, sample, device, asetup,
                                            description='')
    else:
      raise ValueError('sample [%s] is of the wrong type' % sample.label)
    return action

  @debug_wrapper
  def record(self, r):
    try:
      study = self.get_study_by_label(r['study'])
      #-
      data_sample_label = r['data_sample_label']
      if not self.data_collection:
        #FIXME data_collection does not have a label attribute!
        data_collection = self.skb.DataCollection(study=study)
        self.data_collection = self.skb.save(data_collection)
      #-
      if not self.data_samples.has_key(data_sample_label):
        raise ValueError('ignoring %s because is unknown to the kb' %
                         label)
      #-
      data_sample = self.data_samples[data_sample_label]
      action = self.create_action_on_sample(study, data_sample, json.dumps(r))
      action = self.skb.save(action)
      #-
      dc_it = self.skb.DataCollectionItem(data_collection=self.data_collection,
                                          data_sample=data_sample)
      dc_it.action = action
      self.skb.save(dc_it)
      self.logger.info('saved  %s[%s]' % (self.data_collection.id,
                                          data_sample_label))
    except KeyError, e:
      self.logger.warn('ignoring record %s because of missing value(%s)' %
                       (r, e))
      return
    except ValueError, e:
      self.logger.warn('ignoring record %s since %s' % (r, e))
      return
    except (KBError, NotImplementedError), e:
      self.logger.warn('ignoring record %s because it triggers a KB error: %s'%
                       (r, e))
      return
    except Exception, e:
      self.logger.fatal('INTERNAL ERROR WHILE PROCESSING %s (%s)' % (r, e))
      return

  @debug_wrapper
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
  for r in f:
    recorder.record(r)

def do_register(registration_list):
  registration_list.append(('data_collection', help_doc,
                            make_parser_data_collection,
                            import_data_collection_implementation))
