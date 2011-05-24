"""
FIXME
"""

from core import Core, BadRecord

from version import version

import csv, json, time

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

class BioSampleRecorder(Core):
  """
  An utility class that helps in the actual recording of BioSamples subclasses into VL

  """
  # FIXME: the klass_name thing is a kludge...
  def __init__(self, klass_name, study_label=None, initial_volume=None, current_volume=None,
               host=None, user=None, passwd=None, keep_tokens=1, operator='Alfred E. Neumann'):
    super(BioSampleRecorder, self).__init__(host, user, passwd, keep_tokens)
    self.default_study = None
    if study_label:
      s = self.skb.get_study_by_label(study_label)
      if not s:
        raise ValueError('No known study with label %s' % study_label)
      self.logger.info('Selecting %s[%d,%s] as default study'
                       % (s.label, s.omero_id, s.id))
      self.default_study = s
    self.initial_volume = initial_volume
    self.current_volume = current_volume
    self.known_studies = {}
    self.device = self.get_device('importer-0.0', 'CRS4', 'IMPORT', '0.0')
    self.asetup = self.get_action_setup('importer-version-%s-%s-%f'
                                        % (version, klass_name, time.time()),
                                        # FIXME the json below should
                                        # record the app version, and the
                                        # parameters used.  unclear if we
                                        # need to register the file we load
                                        # data from, since it is, most
                                        # likely, a transient object.
                                        json.dumps({'study' : study_label,
                                                    'bio_sample_class' : klass_name,
                                                    'initial_volume' : initial_volume,
                                                    'current_volume' : current_volume,
                                                    'host' : host,
                                                    'user' : user}))
    self.acat  = self.acat_map['IMPORT']
    self.operator = operator
    #
    self.input_rows = {}
    self.counter = 0
    #FIXME -- speed up attempt --
    self.device.unload()
    self.asetup.unload()
    self.acat.unload()


  @debug_wrapper
  def record_helper(self, klass, r):
    """
    Extract common information from a dict describing a BioSample.
    It expects that r contains at least the following fields:

      * study: the context study label
      * label: the specific sample label
      * barcode: the specific sample barcode
      * initial_volume: measured in ML
      * current_volume: measured in ML
      * status:        FIXME

    Records that have the same label or barcode of a previously seen
    blood sample will be noisely ignored.


    :param klass: the specific sample subclass
    :type klass: a class type

    :param r: basic biosample information.
    :type r: a dict
    """
    self.logger.debug('\tworking on %s' % r)
    try:
      i_study =  r['study']
      study = self.default_study if self.default_study \
              else self.known_studies.setdefault(i_study,
                                                 self.get_study_by_label(i_study))

      label, barcode, status = [r[k] for k in 'label barcode status'.split()]
      initial_volume =  self.initial_volume if self.initial_volume else float(r['initial_volume'])
      current_volume =  self.current_volume if self.current_volume else float(r['current_volume'])
    except KeyError, e:
      raise BadRecord(e)

    self.input_rows[barcode] = r

    if initial_volume < current_volume:
      raise BadRecord('inconsistent volume value')

    return study, label, barcode, initial_volume, current_volume, status
