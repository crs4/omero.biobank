# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
Import diagnosis
================

Will read in a tsv file with the following columns::

   study  individual timestamp      diagnosis
   ASTUDY V899       1310057541608  icd10-cm:E10
   ASTUDY V899       1310057541608  icd10-cm:G35
   ASTYDY V1806      1310057541608  exclusion-problem_diagnosis
   ...

where the diagnosis column contains `openEHR
<http://www.openehr.org>`_ diagnosis codes.
"""

import logging, os, copy, csv, json, time
logger = logging.getLogger()
import itertools as it

import core
from version import version


# FIXME This should be defined somewhere else
LEGAL_TERMINOLOGIES = ['icd10-cm']


class Recorder(core.Core):

  def __init__(self, study_label,
               host=None, user=None, passwd=None,  keep_tokens=1,
               action_setup_conf=None,
               batch_size=1000,  operator='Alfred E. Neumann', logger=None):
    super(Recorder, self).__init__(host, user, passwd, keep_tokens=keep_tokens,
                                   study_label=study_label, logger=logger)
    self.batch_size = batch_size
    self.action_setup_conf = action_setup_conf
    self.operator = operator
    self.preloaded_individuals = {}

  def record(self, records, rtsv):
    def records_by_chunk(batch_size, records):
      offset = 0
      while len(records[offset:]) > 0:
        yield records[offset:offset+batch_size]
        offset += batch_size
    if not records:
      msg = 'No records are going to be imported'
      self.logger.critical(msg)
      raise core.ImporterValidationError(msg)
    self.preload_individuals()
    records, bad_records = self.do_consistency_checks(records)
    for br in bad_records:
      rtsv.writerow(br)
    study = self.find_study(records)
    device_label = 'importer.ehr.diagnosis-%s' %  (version)
    device = self.get_device(label=device_label,
                             maker='CRS4', model='importer', release=version)
    asetup = self.get_action_setup('importer.diagnosis-%f' % time.time(),
                                   json.dumps(self.action_setup_conf))
    for i, c in enumerate(records_by_chunk(self.batch_size, records)):
      self.logger.info('start processing chunk %d' % i)
      self.process_chunk(c, study, asetup, device)
      self.logger.info('done processing chunk %d' % i)

  def preload_individuals(self):
    self.preload_by_type('individuals', self.kb.Individual,
                         self.preloaded_individuals)

  def process_chunk(self, chunk, study, asetup, device):
    actions = []
    for r in chunk:
      target = self.preloaded_individuals[r['individual']]
      conf = {
        'setup': asetup,
        'device': device,
        'actionCategory': self.kb.ActionCategory.IMPORT,
        'operator': self.operator,
        'context': study,
        'target': target,
        }
      actions.append(self.kb.factory.create(self.kb.ActionOnIndividual, conf))
    self.kb.save_array(actions)
    for a, r in it.izip(actions, chunk):
      if r['diagnosis'] == 'exclusion-problem_diagnosis':
        archetype = 'openEHR-EHR-EVALUATION.exclusion-problem_diagnosis.v1'
        fields = {'at0002.1': 'local:at0.3'}  # No significant medical history
      else:
        archetype = 'openEHR-EHR-EVALUATION.problem-diagnosis.v1'
        fields = {'at0002.1': r['diagnosis']}
      self.kb.add_ehr_record(a, long(r['timestamp']), archetype, fields)

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
    good_records = []
    bad_records = []
    mandatory_fields = ['individual', 'diagnosis', 'timestamp']
    for i, r in enumerate(records):
      reject = 'Rejecting import %d: ' % i
      if self.missing_fields(mandatory_fields, r):
        f = 'missing mandatory field'
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      if not r['individual'] in self.preloaded_individuals:
        f = 'unknown individual with ID %s' % r['individual']
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      if not self.legal_diagnosis(r['diagnosis']):
        f = 'illegal diagnosis code %s' % r['diagnosis']
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      try:
        long(r['timestamp'])
      except ValueError, e:
        f = 'timestamp %r is not a long' % r['timestamp']
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      good_records.append(r)
    self.logger.info('done consistency checks')
    return good_records, bad_records


help_doc = """
import new diagnosis into the KB.
"""


def make_parser(parser):
  parser.add_argument('--study', metavar="STRING",
                      help="overrides the study column value")


def implementation(logger, host, user, passwd, args):
  action_setup_conf = Recorder.find_action_setup_conf(args)
  recorder = Recorder(args.study,
                      host=host, user=user, passwd=passwd,
                      operator=args.operator,
                      action_setup_conf=action_setup_conf, logger=logger)
  f = csv.DictReader(args.ifile, delimiter='\t')
  logger.info('start processing file %s' % args.ifile.name)
  records = [r for r in f]
  canonizer = core.RecordCanonizer(['study'], args)
  canonizer.canonize_list(records)
  report_fnames = copy.deepcopy(f.fieldnames)
  report_fnames.append('error')
  report = csv.DictWriter(args.report_file, report_fnames,
                          delimiter='\t', lineterminator=os.linesep,
                          extrasaction='ignore')
  report.writeheader()
  recorder.record(records, report)
  args.ifile.close()
  args.report_file.close()
  logger.info('done processing file %s' % args.ifile.name)


def do_register(registration_list):
  registration_list.append(('diagnosis', help_doc, make_parser,
                            implementation))
