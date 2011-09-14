"""
Importing a Study definition
============================

A study is essentially a label to represent a general context.
It is characterized by the following fields::

  label  description
  ASTUDY A textual description of ASTUDY, no tabs please.

The description column is optional. It will be filled with the string
'''No description provided''' if missing.

The study sub-operation will read in a tsv files with that information
and output the vid ids  of the created study objects.

.. code-block:: bash

   bash> cat study.tsv
   label  description
   BSTUDY A basically empty description of BSTUDY
   CSTUDY A basically empty description of CSTUDY
   bash> ${IMPORTER} -i study.tsv -o study_mapping.tsv study
   bash> cat study_mapping.tsv
   study  label type  vid
   None BSTUDY  Study V058F28010E99945A5930375FE22363FC4
   None CSTUDY  Study V0BBAACD697EFA4A6B982FAED36673A17D


"""

from bl.vl.kb import KBError
from core import Core, BadRecord
from version import version

import itertools as it
import csv, json
import time, sys

#-----------------------------------------------------------------------------

class Recorder(Core):
  """
  An utility class that handles the actual recording of Study(s)
  into VL, including Study(s) generation as needed.
  """
  def __init__(self,
               out_stream=None,
               host=None, user=None, passwd=None, keep_tokens=1,
               batch_size=1000, operator='Alfred E. Neumann', logger=None):
    """
    FIXME
    """
    super(Recorder, self).__init__(host, user, passwd, keep_tokens,
                                   None, logger=logger)
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
    #--
    if not records:
      self.logger.warn('no records')
      return
    self.preload_studies()
    #--
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
    #--
    good_records = []
    mandatory_fields = ['label']
    for i, r in enumerate(records):
      reject = ' Rejecting import of record %d: ' % i

      if self.missing_fields(mandatory_fields, r):
        f = reject + 'missing mandatory field'
        self.logger.error(f)
        continue

      if r['label'] in self.known_studies:
        f = reject + 'there is a pre-existing study with label %s.'
        self.logger.warn(f % r['label'])
        continue
      if r['label'] in k_map:
        f = (reject +
             'there is a pre-existing study with label %s. (in this batch)')
        self.logger.error(f % r['label'])
        continue
      k_map['label'] = r
      good_records.append(r)
    self.logger.info('done consistency checks')
    #--
    return good_records

  def process_chunk(self, chunk):
    studies = []
    for r in chunk:
      # FIXME do we really need to do this?
      conf = {'label' : r['label'], 'description' : r['description']}
      studies.append(self.kb.factory.create(self.kb.Study, conf))
    self.kb.save_array(studies)
    #--
    for d in studies:
      self.logger.info('saved %s[%s] as %s.'
                       % (d.label, d.description, d.id))
      self.out_stream.writerow({'study' : 'None',
                                'label' : d.label,
                                'type'  : 'Study',
                                'vid'   : d.id})

help_doc = """
import new Study definitions into a virgil system.
"""

def make_parser_study(parser):
  pass

def canonize_records(args, records):
  for r in records:
    if 'description' not in r:
      r['description'] = 'No description provided'

def import_study_implementation(logger, args):
  o = csv.DictWriter(args.ofile, fieldnames=['study', 'label', 'type', 'vid'],
                     delimiter='\t')
  recorder = Recorder(o, host=args.host, user=args.user, passwd=args.passwd,
                      keep_tokens=args.keep_tokens, logger=logger)
  f = csv.DictReader(args.ifile, delimiter='\t')
  logger.info('start processing file %s' % args.ifile.name)
  records = [r for r in f]
  #--
  canonize_records(args, records)
  #--
  recorder.record(records)
  #--
  logger.info('done processing file %s' % args.ifile.name)


def do_register(registration_list):
  registration_list.append(('study', help_doc,
                            make_parser_study,
                            import_study_implementation))


