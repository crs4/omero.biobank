"""
Import OpenEHR
===============

Will read in a tsv file with the following columns::

   study  individual_label timestamp      diagnosis
   ASTUDY 899              1310057541608  icd10-cm:E10
   ASTUDY 899              1310057541608  icd10-cm:G35
   ASTYDY 1806             1310057541608  exclusion-problem_diagnosis
I  ...

importer -i diagnosis.tsv diagnosis

"""

import logging

logger = logging.getLogger()

from bl.vl.kb import KBError
from core import Core, BadRecord
from version import version

import csv, json
import time, sys

import itertools as it

# This should be defined somewhere else...
LEGAL_TERMINOLOGIES = ['icd10-cm']

class Recorder(Core):
  """
  An utility class that handles the recording of diagnosis
  into VL.
  """
  def __init__(self, study_label,
               host=None, user=None, passwd=None,  keep_tokens=1,
               batch_size=1000,  operator='Alfred E. Neumann'):
    """
    FIXME
    """
    self.logger = logger
    super(Recorder, self).__init__(host, user, passwd, study_label=study_label)
    self.batch_size = batch_size
    self.operator = operator
    #--
    #--
    device_label = ('importer.ehr.diagnosis-%s' %
                    (version))
    self.device = self.get_device(label=device_label,
                                  maker='CRS4', model='importer', release='0.1')
    self.asetup = self.get_action_setup('importer.diagnosis',
                                        {'study_label' : study_label,
                                         'operator' : operator})

  def record(self, records):
    def records_by_chunk(batch_size, records):
      offset = 0
      while len(records[offset:]) > 0:
        yield records[offset:offset+batch_size]
        offset += batch_size
    #--
    if not records:
      self.logger.warn('no records')
      return
    self.choose_relevant_study(records)
    self.preload_enrollments()
    #--
    records = self.do_consistency_checks(records)
    for i, c in enumerate(records_by_chunk(self.batch_size, records)):
      self.logger.info('start processing chunk %d' % i)
      self.process_chunk(c)
      self.logger.info('done processing chunk %d' % i)
  #-----------------------------------------------------
  def process_chunk(self, chunk):
    actions = []
    for r in chunk:
      target = self.known_enrollments[r['individual_label']].individual
      conf = {'setup' : self.asetup,
              'device': self.device,
              'actionCategory' : self.kb.ActionCategory.IMPORT,
              'operator' : self.operator,
              'context'  : self.default_study,
              'target' : target
              }
      actions.append(self.kb.factory.create(self.kb.ActionOnIndividual, conf))
    self.kb.save_array(actions)
    #--
    for a,r in it.izip(actions, chunk):
      if r['diagnosis'] == 'exclusion-problem_diagnosis':
        archetype = 'openEHR-EHR-EVALUATION.exclusion-problem_diagnosis.v1'
        fields = {'at0002.1' : 'local:at0.3', # No significant medical history
                  }
      else:
        archetype = 'openEHR-EHR-EVALUATION.problem-diagnosis.v1'
        fields = {'at0002.1' : r['diagnosis']}
      self.kb.add_ehr_record(a, long(r['timestamp']), archetype, fields)

  #-----------------------------------------------------
  def legal_diagnosis(self, diag):
    if diag == 'exclusion-problem_diagnosis':
      return True
    parts = diag.split(':', 1)
    if not len(parts) == 2:
      return False
    if parts[0] not in LEGAL_TERMINOLOGIES:
      return False
    return True

  def do_consistency_checks(self, records):
    self.logger.info('start consistency checks')
    #--
    good_records = []
    for i, r in enumerate(records):
      reject = 'Rejecting import %d: ' % i
      if not r['individual_label'] in self.known_enrollments:
        msg = reject + ('unknown individual_label %s in %s.'
                        % (r['individual_label'], self.default_study.label))
        self.logger.error(msg)
        continue
      if not self.legal_diagnosis(r['diagnosis']):
        msg = reject + ('illegal diagnosis code %s' % r['diagnosis'])
        self.logger.error(msg)
        continue
      try:
        long(r['timestamp'])
      except ValueError, e:
        msg = reject + ('timestamp %r is not a long' % r['timestamp'])
        self.logger.error(msg)
        continue
        k_map[r['label']] = r
      good_records.append(r)
    self.logger.info('done consistency checks')
    #--
    return good_records

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

  def preload_enrollments(self):
    self.logger.info('start pre-loading enrolled individuals for study %s'
                     % self.default_study.label)
    self.known_enrollments = {}
    known_enrollments =  self.kb.get_enrolled(self.default_study)
    for e in known_enrollments:
      self.known_enrollments[e.studyCode] = e
    self.logger.info('there are %d enrolled individuals in study %s' %
                     (len(self.known_enrollments), self.default_study.label))


#------------------------------------------------------------------------------

help_doc = """
import new diagnosis into VL.
"""

def make_parser_diagnosis(parser):
  parser.add_argument('-S', '--study', type=str,
                      help="""context study label""")

def import_diagnosis_implementation(args):
  recorder = Recorder(args.study,
                      host=args.host, user=args.user, passwd=args.passwd,
                      keep_tokens=1)
  f = csv.DictReader(args.ifile, delimiter='\t')
  logger.info('start processing file %s' % args.ifile.name)
  records = [r for r in f]
  recorder.record(records)
  logger.info('done processing file %s' % args.ifile.name)


def do_register(registration_list):
  registration_list.append(('diagnosis', help_doc,
                            make_parser_diagnosis,
                            import_diagnosis_implementation))


