# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
Import individual
=================

An individual is characterized by the following fields::

  study label gender   father mother
  xxx   id2   male     None   None
  xxx   id2   male     id4    id5

where gender can be either male or female; father and mother can be
either the string 'None' or the label of an individual in the same
study.

Individuals are the only 'bio' objects that can be loaded
independently from the KB's current content (if their parents are
'None'). The study label should, however, correspond to a previously
loaded study.

**NOTE:** The current implementation does not support cross-study
kinship.
"""

import os, time, json, csv, copy
import itertools as it

from bl.vl.individual.pedigree import import_pedigree
from bl.vl.individual import IndividualStub as Ind

import core
from version import version


class Recorder(core.Core):

  def __init__(self, out_stream=None, study_label=None,
               host=None, user=None, passwd=None,
               keep_tokens=1, batch_size=1000, operator='Alfred E. Neumann',
               action_setup_conf=None, logger=None):
    super(Recorder, self).__init__(host, user, passwd, keep_tokens=keep_tokens,
                                   study_label=study_label, logger=logger)
    self.operator = operator
    self.action_setup_conf = action_setup_conf
    self.out_stream = out_stream
    if self.out_stream:
      self.out_stream.writeheader()
    self.individuals_to_be_saved = []
    self.enrollments_to_be_saved = []
    self.chunk_size = batch_size
    self.known_studies = {}
    device = self.get_device('importer-%s.individual' % version,
                             'CRS4', 'IMPORT', version)
    asetup = self.get_action_setup('import-prog-%f' % time.time(),
                                   json.dumps(self.action_setup_conf))
    acat = self.kb.ActionCategory.IMPORT
    operator = 'Alfred E. Neumann'
    conf = {
      'setup' : asetup,
      'device' : device,
      'actionCategory' : acat,
      'operator' : operator,
      'context' : self.default_study,
      }
    self.action = self.kb.factory.create(self.kb.Action, conf)
    #FIXME what happens if we do not have individuals to save?
    self.action.save()
    self.counter = 0
    self.known_enrollments = {}
    if self.default_study:
      self.logger.info('start pre-loading known enrolled individuals')
      known_enrollments = self.kb.get_enrolled(self.default_study)
      for e in known_enrollments:
        self.known_enrollments[e.studyCode] = e
      self.logger.info('done pre-loading known enrolled individuals')
      self.logger.info('there are %d enrolled individuals in study %s' %
                       (len(self.known_enrollments), self.default_study.label))

  def dump_out(self):
    self.logger.debug('\tthere are %s records to save' %
                      len(self.individuals_to_be_saved))
    self.kb.save_array(self.individuals_to_be_saved)
    for i, e in it.izip(self.individuals_to_be_saved,
                        self.enrollments_to_be_saved):
      e.individual = i
    self.kb.save_array(self.enrollments_to_be_saved)
    for i, e in it.izip(self.individuals_to_be_saved,
                        self.enrollments_to_be_saved):
      self.out_stream.writerow({
        'study': e.study.label,
        'label': e.studyCode,
        'type': 'Individual',
        'vid': i.id,
        })
    self.individuals_to_be_saved = []
    self.enrollments_to_be_saved = []
    self.logger.debug('\tdone')

  def clean_up(self):
    self.dump_out()

  def retrieve_enrollment(self, identifier):
    study_label, label = identifier
    self.logger.info('importing (%s, %s)' % (study_label, label))
    assert study_label == self.default_study.label
    study = self.default_study or self.known_studies.setdefault(
      study_label, self.get_study(study_label)
      )
    e = self.known_enrollments.get(
      label, self.kb.get_enrollment(study, ind_label=label)
      )
    return study, e

  def retrieve(self, identifier):
    study, e = self.retrieve_enrollment(identifier)
    return e.individual if e else None

  def record(self, identifier, gender, father, mother):
    gender_map = {
      'MALE': self.kb.Gender.MALE,
      'FEMALE': self.kb.Gender.FEMALE
      }
    # FIXME quick hack to support reloading
    if father:
      father.reload()
    if mother:
      mother.reload()
    i_conf = {
      'gender': gender_map[gender.upper()],
      'action': self.action,
      }
    if father:
      i_conf['father'] = father
    if mother:
      i_conf['mother'] = mother
    i = self.kb.factory.create(self.kb.Individual, i_conf)
    e_conf = {
      'study': self.default_study,
      'individual': i,
      'studyCode': identifier[1],
      }
    e = self.kb.factory.create(self.kb.Enrollment, e_conf)
    self.individuals_to_be_saved.append(i)
    self.enrollments_to_be_saved.append(e)
    if len(self.individuals_to_be_saved) >= self.chunk_size:
      self.dump_out()
    return i

  def do_consistency_checks(self, records):
    def check_illegal_values(label, values):
      matched_values = [v for v in values if v in label]
      return matched_values
    self.logger.info('starting consistency checks')
    good_records = []
    bad_records = []
    study_label = records[0]['study']
    seen = {}
    mandatory_fields = ['study', 'label', 'gender', 'father', 'mother']
    illegal_values = [':']
    for i, r in enumerate(records):
      reject = 'Rejecting record %d:' % i
      if self.missing_fields(mandatory_fields, r):
        msg = 'missing mandatory field'
        self.logger.error(reject + msg)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = msg
        bad_records.append(bad_rec)
        continue
      if r['study'] != study_label:
        msg = 'non uniform study label'
        self.logger.error(reject + msg)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = msg
        bad_records.append(bad_rec)
        continue
      matched_illegal_values = check_illegal_values(r['label'], illegal_values)
      if len(matched_illegal_values) > 0:
        msg = 'found illegal value(s) %r in label %s' % (matched_illegal_values,
                                                         r['label'])
        self.logger.error(reject + msg)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = msg
        bad_records.append(bad_rec)
        continue
      if r['label'] in self.known_enrollments:
        msg = 'label %s already used in study %s' % (r['label'], study_label)
        self.logger.error(reject + msg)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = msg
        bad_records.append(bad_rec)
        continue
      if r['gender'].upper() not in ['MALE', 'FEMALE']:
        msg = 'unknown gender value'
        self.logger.error(reject + msg)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = msg
        bad_records.append(bad_rec)
        continue
      seen[r['label']] = r
    for i, r in enumerate(records):
      if r not in bad_records:
        reject = 'Rejecting record %d:' % i
        for parent in ['father', 'mother']:
          if not (r[parent].upper() == 'NONE' or
                  r[parent] in seen or
                  r[parent] in self.known_enrollments):
            msg = 'undefined %s label.' % parent
            self.logger.critical(reject + msg)
            raise ValueError(msg)
        good_records.append(r)
    return good_records, bad_records


def make_ind_by_label(records):
  by_label = {}
  for r in records:
    k = (r['study'], r['label'])
    father = None if r['father'] == 'None' else (r['study'], r['father'])
    mother = None if r['mother'] == 'None' else (r['study'], r['mother'])
    by_label[k] = Ind(k, r['gender'], father, mother)
  for k, i in by_label.iteritems():
    i.father = by_label.get(i.father)
    i.mother = by_label.get(i.mother)
  return by_label


help_doc = """
import new individual definitions into the KB and enroll them in a study.
"""


def make_parser(parser):
  parser.add_argument('--study', metavar="STRING",
                      help="overrides the study column value")
  parser.add_argument('-N', '--batch-size', type=int, metavar="INT",
                      default=1000,
                      help="n. of objects to be processed at a time")


def implementation(logger, host, user, passwd, args):
  action_setup_conf = Recorder.find_action_setup_conf(args)
  f = csv.DictReader(args.ifile, delimiter='\t')
  records = [r for r in f]
  if len(records) == 0:
    msg = 'No records are going to be imported'
    logger.critical(msg)
    raise core.ImporterValidationError(msg)
  canonizer = core.RecordCanonizer(['study'], args)
  canonizer.canonize_list(records)
  study_label = records[0]['study']
  o = csv.DictWriter(args.ofile, fieldnames=['study', 'label', 'type', 'vid'],
                     delimiter='\t', lineterminator=os.linesep)
  recorder = Recorder(o, study_label, host, user, passwd,
                      args.keep_tokens, args.batch_size,
                      operator=args.operator,
                      action_setup_conf=action_setup_conf, logger=logger)
  report_fnames = copy.deepcopy(f.fieldnames)
  report_fnames.append('error')
  report = csv.DictWriter(args.report_file, report_fnames,
                          delimiter='\t', lineterminator=os.linesep,
                          extrasaction='ignore')
  report.writeheader()
  records, bad_records = recorder.do_consistency_checks(records)
  for br in bad_records:
    report.writerow(br)
  if args.blocking_validator and len(bad_records) >= 1:
    args.ofile.close()
    args.ifile.close()
    args.report_file.close()
    msg = '%d invalid records' % len(bad_records)
    recorder.logger.critical(msg)
    raise core.ImporterValidationError(msg)
  by_label = make_ind_by_label(records)
  import_pedigree(recorder, by_label.itervalues())
  recorder.clean_up()
  args.ofile.close()
  args.ifile.close()
  args.report_file.close()


def do_register(registration_list):
  registration_list.append(('individual', help_doc, make_parser,
                            implementation))
