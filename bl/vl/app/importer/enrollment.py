# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
Import of new enrollments related to existing individuals
=========================================================

An enrollment is characterized by the following fields:

 source                              study  label
 V044DE795E7F9F42FEB9855288CF577A77  xxx    id1
 V06C59B915C0FD47DABE6AE02C731780AF  xxx    id2
 V01654DCFC5BB640C0BB7EE088194E629D  xxx    id3

where source must be the VID of an existing Individual object, study a
label of an existing Study object and label the enrollment code for
the patient in the study.

The enrollment sub-operation will retrieve the source individual from
the DB, create a new enrollment related to it and output the VIDs of
newly created enrollments. It is not possible to create two
enrollments with the same code related to the same study, nor is it
possible to enroll a patient twice in the same study, even with
different codes.
"""

import os, csv

import core


class Recorder(core.Core):

  def __init__(self, out_stream=None, study_label=None,
               host=None, user=None, passwd=None,
               keep_tokens=1, batch_size=1000, logger=None):
    super(Recorder, self).__init__(host, user, passwd, keep_tokens=keep_tokens,
                                   study_label=study_label, logger=logger)
    self.batch_size = batch_size
    self.preloaded_sources = {}
    self.preloaded_enrollments = {}
    self.preloaded_enrolled_inds = {}

  def record(self, records, otsv):
    def records_by_chunk(batch_size, records):
      offset = 0
      while len(records[offset:]) > 0:
        yield records[offset:offset+batch_size]
        offset += batch_size
    if len(records) == 0:
      self.logger.warn('no records')
      return
    study = self.find_study(records)
    self.preload_individuals()
    self.preload_enrollments(study)
    records = self.do_consistency_checks(records)
    if not records:
      self.logger.warn('no records')
      return
    for i, c in enumerate(records_by_chunk(self.batch_size, records)):
      self.logger.info('start processing chunk %d' % i)
      self.process_chunk(otsv, c, study)
      self.logger.info('done processing chunk %d' % i)

  def preload_individuals(self):
    self.preload_by_type('individuals', self.kb.Individual,
                         self.preloaded_sources)

  def preload_enrollments(self, study):
    self.logger.info('start preloading enrollments for study %s' % study.label)
    if study:
      query = '''SELECT en FROM Enrollment en
                 JOIN FETCH en.individual ind
                 JOIN en.study st
                 WHERE st.label = :st_label'''
      enrolls = self.kb.find_all_by_query(query, {'st_label': study.label})
      for e in enrolls:
        self.preloaded_enrollments[e.studyCode] = e
        self.preloaded_enrolled_inds[e.individual.vid] = e
    self.logger.info('done preloading enrollments')
    self.logger.info('preloaded %d enrollments' %
                     len(self.preloaded_enrollments))

  def do_consistency_checks(self, records):
    self.logger.info('starting consistency checks')
    k_map = {}
    good_records = []
    mandatory_fields = ['label', 'study', 'source']
    for i, r in enumerate(records):
      reject = 'Rejecting import of row %d: ' % i
      if self.missing_fields(mandatory_fields, r):
        f = reject + 'missing mandatory field.'
        self.logger.error(f)
        continue
      if r['label'] in self.preloaded_enrollments:
        f = reject + 'there is a pre-existing Enrollment with label %s.'
        self.logger.warn(f % r['label'])
        continue
      if r['source'] in self.preloaded_enrolled_inds:
        f = reject + 'Individual with VID %s already enrolled'
        self.logger.warn(f % r['source'])
        continue
      if r['label'] in k_map:
        f = reject + 'duplicate label %s (in this batch)'
        self.logger.error(f % r['label'])
        continue
      if r['source'] not in self.preloaded_sources:
        f = reject + 'no known source for Individual with label %s'
        self.logger.error(f % r['label'])
        continue
      k_map[r['label']] = r
      good_records.append(r)
    self.logger.info('done with consistency checks')
    return good_records

  def process_chunk(self, otsv, chunk, study):
    enrollments = []
    for r in chunk:
      ind = self.preloaded_sources[r['source']]
      conf = {
          'study': study,
          'individual': ind,
          'studyCode': r['label'],
          }
      en = self.kb.factory.create(self.kb.Enrollment, conf)
      enrollments.append(en)
    assert len(chunk) == len(enrollments)
    self.kb.save_array(enrollments)
    for en in enrollments:
      otsv.writerow({
          'study': study.label,
          'label': en.studyCode,
          'type': en.get_ome_table(),
          'vid': en.id,
          })


def make_parser(parser):
  parser.add_argument('--study', metavar="STRING",
                      help="overrides the study column value")
  parser.add_argument('--batch-size', type=int, metavar="INT", default=1000,
                      help="n. of objects to be processed at a time")


def implementation(logger, host, user, passwd, args):
  recorder = Recorder(args.study,
                      host=host, user=user, passwd=passwd,
                      logger=logger)
  logger.info('start processing file %s' % args.ifile.name)
  f = csv.DictReader(args.ifile, delimiter='\t')
  records = [r for r in f]
  args.ifile.close()
  canonizer = core.RecordCanonizer(["study"], args)
  canonizer.canonize_list(records)
  if len(records) > 0:
    o = csv.DictWriter(args.ofile,
                       fieldnames=['study', 'label', 'type', 'vid'],
                       delimiter='\t', lineterminator=os.linesep)
    o.writeheader()
    recorder.record(records, o)
  else:
    logger.info('empty file')
  args.ofile.close()
  logger.info('done processing file %s' % args.ifile.name)


help_doc = """
import new enrollments for previously registered individuals.
"""


def do_register(registration_list):
  registration_list.append(('enrollment', help_doc, make_parser,
                            implementation))
