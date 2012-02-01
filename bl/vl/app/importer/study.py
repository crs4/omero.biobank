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

import os, csv

import core


DEFAULT_DESCRIPTION = 'No description provided'


class Recorder(core.Core):
  
  def __init__(self, out_stream=None,
               host=None, user=None, passwd=None, keep_tokens=1,
               batch_size=1000, operator='Alfred E. Neumann', logger=None):
    super(Recorder, self).__init__(host, user, passwd, keep_tokens=keep_tokens,
                                   study_label=None, logger=logger)
    self.out_stream = out_stream
    if self.out_stream:
      self.out_stream.writeheader()
    self.batch_size = batch_size
    self.operator = operator

  def record(self, records):
    def records_by_chunk(batch_size, records):
      offset = 0
      while len(records[offset:]) > 0:
        yield records[offset:offset+batch_size]
        offset += batch_size
    if not records:
      self.logger.warn('no records')
      return
    self.preload_studies()
    records = self.do_consistency_checks(records)
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
    mandatory_fields = ['label']
    for i, r in enumerate(records):
      reject = ' Rejecting import of record %d: ' % i
      if self.missing_fields(mandatory_fields, r):
        f = reject + 'missing mandatory field'
        self.logger.error(f)
        continue
      if r['label'] in self.known_studies:
        f = reject + 'there is a pre-existing study with label %s'
        self.logger.warn(f % r['label'])
        continue
      if r['label'] in k_map:
        f = (reject +
             'there is a pre-existing study with label %s (in this batch)')
        self.logger.error(f % r['label'])
        continue
      k_map['label'] = r
      good_records.append(r)
    self.logger.info('done with consistency checks')
    return good_records

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


def implementation(logger, args):
  f = csv.DictReader(args.ifile, delimiter='\t')
  logger.info('start processing file %s' % args.ifile.name)
  records = [r for r in f]
  args.ifile.close()
  if not records:
    logger.info('empty file')
    return
  canonizer = RecordCanonizer(['label'], args)
  canonizer.canonize_list(records)
  o = csv.DictWriter(args.ofile, fieldnames=['study', 'label', 'type', 'vid'],
                     delimiter='\t', lineterminator=os.linesep)
  recorder = Recorder(o, host=args.host, user=args.user, passwd=args.passwd,
                      keep_tokens=args.keep_tokens, logger=logger)
  recorder.record(records)
  args.ofile.close()
  logger.info('done processing file %s' % args.ifile.name)


def do_register(registration_list):
  registration_list.append(('study', help_doc, make_parser,
                            implementation))
