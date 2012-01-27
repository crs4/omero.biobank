"""
Group definition
================

Will read in a tsv file with the following columns::

  group    group_code individual
  BSTUDY   dc-01 V0390290
  BSTUDY   dc-01 V0390291
  BSTUDY   dc-02 V0390292
  BSTUDY   dc-02 V390293
  ...

This will create new Study objects, whose group_code is listed in the
group column, and enroll the individuals corresponding to the VIDs
listed in the individual column.

Records that point to an unknown Individual will cause the program to
exit with an error. Previously seen collections will be noisily
ignored. No, it is not legal to use the importer to add items to a
previously known collection.

TODO We should been using a Group object derived from VLCollection.
"""

import os, csv
import itertools as it

import core


class Recorder(core.Core):
  def __init__(self, host=None, user=None, passwd=None, keep_tokens=1,
               batch_size=1000, operator='Alfred E. Neumann',
               logger=None, action_setup_conf=None):
    super(Recorder, self).__init__(host, user, passwd, keep_tokens=keep_tokens,
                                   study_label=None, logger=logger)
    self.batch_size = batch_size
    self.operator = operator
    self.action_setup_conf = action_setup_conf
    self.preloaded_groups = {}
    self.preloaded_individuals = {}

  def record(self, records, otsv):
    def records_by_chunk(batch_size, records):
      offset = 0
      while len(records[offset:]) > 0:
        yield records[offset:offset+batch_size]
        offset += batch_size
    if len(records) == 0:
      self.logger.warn('no records')
      return
    self.preload_groups()
    self.preload_individuals()
    def keyfunc(r): return r['group']
    sub_records = []
    records = sorted(records, key=keyfunc)
    for k, g in it.groupby(records, keyfunc):
      sub_records.append(self.do_consistency_checks(k, list(g)))
    records = sum(sub_records, [])
    if len(records) == 0:
      self.logger.warn('no records')
      return
    records = sorted(records, key=keyfunc)
    for k, g in it.groupby(records, keyfunc):
      group_conf = {'label': k}
      group = self.kb.factory.create(self.kb.Study, group_conf).save()
      for i, c in enumerate(records_by_chunk(self.batch_size, list(g))):
        self.logger.info('start processing chunk %s-%d' % (k, i))
        self.process_chunk(otsv, group, c)
        self.logger.info('done processing chunk %s-%d' % (k,i))

  def preload_individuals(self):
    self.preload_by_type('individuals', self.kb.Individual,
                         self.preloaded_individuals)

  def preload_groups(self):
    self.logger.info('start preloading groups')
    ds = self.kb.get_objects(self.kb.Study)
    for d in ds:
      self.preloaded_groups[d.label] = d
    self.logger.info('there are %d Groups(s) in the kb' %
                     len(self.preloaded_groups))

  def do_consistency_checks(self, k, records):
    self.logger.info('start consistency checks on %s' % k)
    if k in self.preloaded_groups:
      self.logger.error('There is already a Group with label %s' % k)
      return []
    failures = 0
    seen = []
    for r in records:
      if not r['individual'] in self.preloaded_individuals:
        f = 'bad individual %s in %s.'
        self.logger.error( f % (r['individual'], k))
        failures += 1
        continue
      if r['individual'] in seen:
        f = 'multiple copy of the same individual %s in %s.'
        self.logger.error( f % (r['individual'], k))
        failures += 1
        continue
      if r['group_code'] in seen:
        f = 'multiple copy of the same label %s in %s.'
        self.logger.error( f % (r['group_code'], k))
        failures += 1
        continue
      seen.append(r['individual'])
      seen.append(r['group_code'])
    self.logger.info('done consistency checks on %s' % k)
    return [] if failures else records

  def process_chunk(self, otsv, group, chunk):
    items = []
    for r in chunk:
      conf = {
        'individual': self.preloaded_individuals[r['individual']],
        'studyCode': r['group_code'],
        'study': group,
        }
      items.append(self.kb.factory.create(self.kb.Enrollment, conf))
    self.kb.save_array(items)
    otsv.writerow({
      'study': 'None',
      'label': group.label,
      'type': group.get_ome_table(),
      'vid': group.id,
      })


def make_parser(parser):
  parser.add_argument('--group', metavar="STRING",
                      help="overrides the group column value")


def implementation(logger, args):
  action_setup_conf = Recorder.find_action_setup_conf(args)
  recorder = Recorder(host=args.host, user=args.user, passwd=args.passwd,
                      operator=args.operator,
                      action_setup_conf=action_setup_conf, logger=logger)
  f = csv.DictReader(args.ifile, delimiter='\t')
  logger.info('start processing file %s' % args.ifile.name)
  records = [r for r in f]
  args.ifile.close()
  canonizer = core.RecordCanonizer(["group"], args)
  canonizer.canonize_list(records)
  o = csv.DictWriter(args.ofile,
                     fieldnames=['study', 'label', 'type', 'vid'],
                     delimiter='\t', lineterminator=os.linesep)
  o.writeheader()
  recorder.record(records, o)
  args.ofile.close()
  logger.info('done processing file %s' % args.ifile.name)


help_doc = """
create a new group definition in the KB.
"""


def do_register(registration_list):
  registration_list.append(('group', help_doc, make_parser, implementation))
