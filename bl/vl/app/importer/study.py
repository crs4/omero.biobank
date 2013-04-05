# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
Import study
============

A study represents a general context. It is characterized by the
following fields::

  label  description
  ASTUDY A textual description of ASTUDY, no tabs please.

The description column is optional. The study sub-operation will read
in a tsv files with the above information and output the VIDs of the
newly created study objects.
"""

import os, csv, copy

import core


DEFAULT_DESCRIPTION = 'No description provided'


class Recorder(core.Core):
  
  def __init__(self, out_stream=None, report_stream=None,
               host=None, user=None, passwd=None, keep_tokens=1,
               batch_size=1000, operator='Alfred E. Neumann', logger=None):
    super(Recorder, self).__init__(host, user, passwd, keep_tokens=keep_tokens,
                                   study_label=None, logger=logger)
    self.out_stream = out_stream
    if self.out_stream:
      self.out_stream.writeheader()
    self.report_stream = report_stream
    if self.report_stream:
      self.report_stream.writeheader()
    self.batch_size = batch_size
    self.operator = operator

  def record(self, records, blocking_validation):
    def records_by_chunk(batch_size, records):
      offset = 0
      while len(records[offset:]) > 0:
        yield records[offset:offset+batch_size]
        offset += batch_size
    if not records:
      self.logger.warn('no records')
      return
    self.preload_studies()
    records, bad_records = self.do_consistency_checks(records)
    for br in bad_records:
      self.report_stream.writerow(br)
    if blocking_validation and len(bad_records) >= 1:
      raise core.ImporterValidationError('%d invalid records' % len(bad_records))
    for i, c in enumerate(records_by_chunk(self.batch_size, records)):
      self.logger.info('start processing chunk %d' % i)
      self.process_chunk(c)
      self.logger.info('done processing chunk %d' % i)

  def preload_studies(self):
    self.logger.info('start prefetching studies')
    self.known_studies = {}
    studies = self.kb.get_objects(self.kb.Study)
    for s in studies:
      self.known_studies[s.label] = s
    self.logger.info('there are %d Study(s) in the kb'
                     % (len(self.known_studies)))

  def do_consistency_checks(self, records):
    self.logger.info('start consistency checks')
    k_map = {}
    good_records = []
    bad_records = []
    mandatory_fields = ['label']
    for i, r in enumerate(records):
      reject = ' Rejecting import of record %d: ' % i
      if self.missing_fields(mandatory_fields, r):
        f = 'missing mandatory field'
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      if r['label'] in self.known_studies:
        f = 'there is a pre-existing study with label %s' % r['label']
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      if r['label'] in k_map:
        f = 'there is a pre-existing study with label %s in this batch' % r['label']
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      k_map['label'] = r
      good_records.append(r)
    self.logger.info('done with consistency checks')
    return good_records, bad_records

  def process_chunk(self, chunk):
    studies = []
    for r in chunk:
      conf = {'label': r['label'], 'description': r['description']}
      studies.append(self.kb.factory.create(self.kb.Study, conf))
    self.kb.save_array(studies)
    for d in studies:
      self.logger.info('saved %s[%s] as %s.' % (d.label, d.description, d.id))
      self.out_stream.writerow({
        'study': 'None',
        'label': d.label,
        'type': 'Study',
        'vid': d.id,
        })


help_doc = """
import new Study definitions into the KB.
"""


def make_parser(parser):
  parser.add_argument('--label', metavar="STRING",
                      help="overrides the label column value")


class RecordCanonizer(core.RecordCanonizer):

  def canonize(self, r):
    super(RecordCanonizer, self).canonize(r)
    r.setdefault('description', DEFAULT_DESCRIPTION)


def implementation(logger, host, user, passwd, args):
  f = csv.DictReader(args.ifile, delimiter='\t')
  logger.info('start processing file %s' % args.ifile.name)
  records = [r for r in f]
  if not records:
    logger.info('empty file')
    return
  canonizer = RecordCanonizer(['label'], args)
  canonizer.canonize_list(records)
  o = csv.DictWriter(args.ofile, fieldnames=['study', 'label', 'type', 'vid'],
                     delimiter='\t', lineterminator=os.linesep)
  report_fnames = f.fieldnames
  report_fnames.append('error')
  report = csv.DictWriter(args.report_file, report_fnames,
                          delimiter='\t', lineterminator=os.linesep,
                          extrasaction='ignore')
  recorder = Recorder(o, report, host=host, user=user, passwd=passwd,
                      keep_tokens=args.keep_tokens, logger=logger)
  try:
    recorder.record(records, args.blocking_validator)
  except core.ImporterValidationError, ve:
    args.ifile.close()
    args.ofile.close()
    args.report_file.close()
    logger.critical(ve.message)
    raise ve
  args.ifile.close()
  args.ofile.close()
  args.report_file.close()
  logger.info('done processing file %s' % args.ifile.name)


def do_register(registration_list):
  registration_list.append(('study', help_doc, make_parser,
                            implementation))
