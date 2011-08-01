"""
Map user available labels to vid
================================

FIXME

"""

from bl.vl.app.importer.core import Core
from version import version

import csv, json
import argparse
import time, sys
import itertools as it

import logging

class MapApp(Core):
  """
  An utility class that handles the dumping of map
  specification data from the KB.
  """
  def __init__(self, host=None, user=None, passwd=None, keep_tokens=1,
               study_label=None,
               operator='Alfred E. Neumann', logger=None):
    """
    FIXME
    """
    self.logger = logging.getLogger()
    super(MapApp, self).__init__(host, user, passwd,
                                 keep_tokens=keep_tokens,
                                 study_label=study_label,
                                 logger=logger)

  def resolve_mapping_individual(self, labels):
    mapping = {}
    self.logger.info('start selecting enrolled individuals')
    known_enrollments = self.kb.get_enrolled(self.default_study)
    for e in known_enrollments:
      if e.studyCode in labels:
        i = e.individual
        i.reload()
        mapping[e.studyCode] = i.id
    diff = set(labels) - set(mapping.keys())
    if len(diff) > 0:
      for x in diff:
        self.logger.critical('cannot map %s as an individual in study %s' %
                             (x, self.default_study.label))
      raise ValueError('cannot map required individuals')
    self.logger.info('done selecting enrolled individuals')
    return mapping

  def resolve_mapping_object(self, source_type, labels):
    mapping = {}
    self.logger.info('start selecting %s' % source_type.get_ome_table())
    # FIXME this is not going to scale...
    objs = self.kb.get_objects(source_type)
    for o in objs:
      if o.label in labels:
        mapping[o.label] = o.id
    self.logger.info('done selecting %s' % source_type.get_ome_table())
    return mapping

  def resolve_mapping(self, source_type, labels):
    if source_type == self.kb.Individual:
      return self.resolve_mapping_individual(labels)
    else:
      return self.resolve_mapping_object(source_type, labels)

  def dump(self, ifile, source_type_label, column_label, ofile):
    if not hasattr(self.kb, source_type_label):
      msg = 'Unknown source type: %s' % source_type_label
      self.logger.critical(msg)
      raise ValueError(msg)
    source_type = getattr(self.kb, source_type_label)

    f = csv.DictReader(ifile, delimiter='\t')
    records = [r for r in f]
    if len(records) == 0:
      return

    labels = [r[column_label] for r in records]
    mapping = self.resolve_mapping(source_type, labels)

    self.logger.info('start writing %s' % ofile.name)
    fieldnames = [k for k in records[0].keys() if k != column_label]
    fieldnames.append('source')
    o = csv.DictWriter(ofile, fieldnames=fieldnames, delimiter='\t')
    o.writeheader()
    for r in records:
      r['source'] = mapping[r.pop(column_label)]
      o.writerow(r)
    self.logger.info('done writing %s' % ofile.name)
#-------------------------------------------------------------------------
help_doc = """
Map user defined objects label to vid.

usage example:

   kb_query -H biobank05  -o bs_mapped.tsv map \
                          -i blood_sample.tsv \
                          --column 'individual_label' --study BSTUDY \
                          --source-type Individual
"""

def make_parser_map(parser):
  parser.add_argument('-i', '--ifile', type=argparse.FileType('r'),
                      help='the input tsv file',
                      required=True)
  parser.add_argument('--source-type', type=str,
                      choices=['Tube', 'Individual'],
                      help="""assigned source type""",
                      required=True)
  parser.add_argument('--column', type=str,
                      help="object labels column name",
                      required=True)
  parser.add_argument('--study', type=str,
                      help="study label")

def import_map_implementation(logger, args):
  #--
  app = MapApp(host=args.host, user=args.user, passwd=args.passwd,
               keep_tokens=args.keep_tokens,
               study_label=args.study, logger=logger)

  app.dump(args.ifile, args.source_type, args.column, args.ofile)

def do_register(registration_list):
  registration_list.append(('map', help_doc,
                            make_parser_map,
                            import_map_implementation))


