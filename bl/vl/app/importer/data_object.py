"""
Import of Data samples
======================

Will read in a tsv file with the following columns::

   study path data_label mimetype size sha1

   TEST01 file:/share/fs/v039303.cel CA_03030.CEL x-vl/affymetrix-cel 39090 E909090
  ....

Record that point to an unknown (data_label) will be noisily
ignored. The same will happen to records that have the same path of a
previously seen data_object

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
  An utility class that handles the actual recording of DataObject(s)
  metadata into VL, including the potential copying of datasets.
  """
  def __init__(self, study_label=None, data_dir=None, copy_data_objects=False,
               host=None, user=None, passwd=None, keep_tokens=1,
               operator='Alfred E. Neumann'):
    """
    FIXME

    :param data_dir:
    :type data_dir:
    """
    super(Recorder, self).__init__(host, user, passwd)
    self.data_dir = data_dir
    self.copy_data_objects = copy_data_objects
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
                                                    'data_dir' : self.data_dir,
                                                    'copy_data_objects' : self.copy_data_objects,
                                                    'operator' : operator,
                                                    'host' : host,
                                                    'user' : user}))
    self.acat  = self.acat_map['IMPORT']
    self.operator = operator
    #
    self.input_rows = {}
    self.counter = 0
    #-------------------------------------------------------------------------
    self.logger.info('start prefetching DataSample(s)')
    data_samples = self.skb.get_bio_samples(self.skb.DataSample)
    self.data_samples = {}
    for ds in data_samples:
      self.data_samples[ds.name] = ds
    self.logger.info('done prefetching DataSample(s)')
    self.logger.info('there are %d DataSample(s) in the kb' %
                     len(self.data_samples))
    #-------------------------------------------------------------------------
    self.logger.info('start prefetching DataObject(s)')
    data_objects = self.skb.get_bio_samples(self.skb.DataObject)
    self.data_objects = {}
    for do in data_objects:
      self.data_objects[do.path] = do
    self.logger.info('done prefetching DataObject(s)')
    self.logger.info('there are %d DataObject(s) in the kb' %
                     len(self.data_objects))

  @debug_wrapper
  def get_study_by_label(self, study_label):
    if self.default_study:
      return self.default_study
    return self.known_studies.\
           setdefault(study_label, super(Recorder, self).\
                      get_study_by_label(study_label))

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
    self.logger.debug('\tworking on %s' % r)
    try:
      study = self.get_study_by_label(r['study'])
      path, data_label, mimetype, size, sha1 = r['path'], r['data_label'], \
                                                r['mimetype'], r['size'], r['sha1']
      size = int(size)
      #-
      if self.data_objects.has_key(path):
        raise ValueError('We already have a DataObject with path %s in the kb' %
                         path)
      if not self.data_samples.has_key(data_label):
        raise ValueError('Cannot find a DataSample with label %s in the kb' %
                         data_label)
      data_sample = self.data_samples[data_label]
      data_object = self.skb.DataObject(sample=data_sample,
                                        mime_type=mimetype,
                                        path=path,
                                        size=size,
                                        sha1=sha1)
      self.skb.save(data_object)
      self.logger.info('Saving DataObject with path %s in the kb' % path)
    except KeyError, e:
      self.logger.warn('ignoring record %s because of missing value(%s)' %
                       (r, e))
      return
    except ValueError, e:
      self.logger.warn('ignoring record %s since %s' % (r, e))
      return
    except (KBError, NotImplementedError), e:
      self.logger.warn('ignoring record %s because it triggers a KB error: %s' % (r, e))
      return
    except Exception, e:
      self.logger.fatal('INTERNAL ERROR WHILE PROCESSING %s (%s)' % (r, e))
      return

  @debug_wrapper
  def create_action_on_sample(self, study, sample, device, asetup, description):
    return self.create_action_helper(self.skb.ActionOnSample, description,
                                     study, device, asetup,
                                     self.acat, self.operator, sample)

  @debug_wrapper
  def create_action_on_sample_slot(self, study, sample_slot, device, asetup, description):
    return self.create_action_helper(self.skb.ActionOnSamplesContainerSlot, description,
                                     study, device, asetup, self.acat, self.operator,
                                     sample_slot)

  @debug_wrapper
  def get_bio_sample(self, label):
    bio_sample = self.skb.get_bio_sample(label=label)
    if not bio_sample:
      raise ValueError('cannot find a sample with label <%s>' % label)
    return  bio_sample

help_doc = """
import new data_object definitions into a virgil system. It will also
import actual datasets if so instructed.
"""

def make_parser_data_object(parser):
  parser.add_argument('-S', '--study', type=str,
                      help="""default conxtest study label.
                      It will over-ride the study column value""")
  parser.add_argument('-d', '--data-dir', type=str,
                      help="""directory that contains the data object files""")
  parser.add_argument('--copy-data-objects', action='store_true', default=False,
                      help="""if set, copies datasets in VL repositories""")

def import_data_object_implementation(args):
  recorder = Recorder(args.study, data_dir=args.data_dir,
                      copy_data_objects=args.copy_data_objects,
                      host=args.host, user=args.user, passwd=args.passwd,
                      keep_tokens=args.keep_tokens)
  f = csv.DictReader(args.ifile, delimiter='\t')
  for r in f:
    recorder.record(r)

def do_register(registration_list):
  registration_list.append(('data_object', help_doc,
                            make_parser_data_object,
                            import_data_object_implementation))


