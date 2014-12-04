# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
Extract in tabular form EHR data from KB
========================================
"""

import os, csv

from bl.core.utils import NullLogger
from bl.vl.app.importer.core import Core


class EHR(Core):
  
  SUPPORTED_FIELDS_SETS = ['diagnosis']
  ATYPE_EXCLUSION = 'openEHR-EHR-EVALUATION.exclusion-problem_diagnosis.v1'
  ATYPE_DIAGNOSIS = 'openEHR-EHR-EVALUATION.problem-diagnosis.v1'

  def __init__(self, host=None, user=None, passwd=None, keep_tokens=1,
               batch_size = 100, operator='Alfred E. Neumann', logger=None):
    super(EHR, self).__init__(host, user, passwd, keep_tokens=keep_tokens)
    self.logger = logger or NullLogger()
    self.batch_size = batch_size

  def load_enrollments(self):
    self.logger.info('start pre-loading enrolled individuals for study %s' %
                     self.default_study.label)
    self.known_enrollments = {}
    known_enrollments = self.kb.get_enrolled(self.default_study)
    for e in known_enrollments:
      self.known_enrollments[e.individual.id] = e
    self.logger.info('there are %d enrolled individuals in study %s' %
                     (len(self.known_enrollments), self.default_study.label))

  def dump(self, study_label, fields_set, ofile):
    self.default_study = None
    if study_label:
      self.default_study = self.get_study(study_label)
    if not self.default_study:
      raise ValueError('unknown study %s' % study_label)
    if fields_set == 'diagnosis':
      self.load_enrollments()
      return self.dump_diagnosis(ofile)

  def dump_diagnosis(self, ofile):
    def records_by_chunk(batch_size, records):
      offset = 0
      while len(records[offset:]) > 0:
        yield records[offset:offset+batch_size]
        offset += batch_size
    def process_chunk(chunk):
      selector = '(' + '|'.join(['(i_vid=="%s")' % x for x in chunk]) + ')'
      selector += ('&' + '(' +
                   '|'.join(['(archetype=="%s")' % x
                             for x in [EHR.ATYPE_DIAGNOSIS,
                                       EHR.ATYPE_EXCLUSION]])
                   + ')')
      return self.kb.get_ehr_records(selector)
    rs = []
    for i, c in enumerate(records_by_chunk(self.batch_size,
                                           self.known_enrollments.keys())):
      self.logger.info('start processing chunk %d' % i)
      rs.extend(process_chunk(c))
      self.logger.info('done processing chunk %d' % i)
    self.logger.info('got %d records' % len(rs))
    fnames = 'study individual_label timestamp diagnosis'.split()
    tsv = csv.DictWriter(ofile, fnames, delimiter='\t',
                         lineterminator=os.linesep)
    tsv.writeheader()
    for r in rs:
      e = self.known_enrollments[r['i_id']]
      x = {
        'study': self.default_study.label,
        'individual_label': e.studyCode,
        'timestamp': r['timestamp'],
        }
      if r['archetype'] == EHR.ATYPE_EXCLUSION:
        x['diagnosis'] = 'exclusion-problem_diagnosis'
      else:
        x['diagnosis'] =  r['fields']['at0002.1']
      tsv.writerow(x)


help_doc = """
Extract ehr-related info from the KB
"""


def make_parser(parser):
  parser.add_argument('--study-label', metavar="STRING", required=True,
                      help="study label")
  parser.add_argument('--fields-set', metavar="STRING", default='diagnosis',
                      choices=EHR.SUPPORTED_FIELDS_SETS,
                      help="choose all the fields listed in this set")


def implementation(logger, host, user, passwd, args):
  ehr = EHR(host=host, user=user, passwd=passwd,
            keep_tokens=args.keep_tokens, logger=logger)
  ehr.dump(args.study_label, args.fields_set, args.ofile)


def do_register(registration_list):
  registration_list.append(('ehr', help_doc, make_parser, implementation))
