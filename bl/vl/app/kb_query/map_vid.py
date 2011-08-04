"""
Map user available labels to vid
================================

FIXME

"""

from bl.vl.app.importer.core import Core
from version import version

# FIXME this is an hack that specific to the omero driver...
from bl.vl.kb.drivers.omero.utils import make_unique_key


import csv, json
import argparse
import time, sys
import itertools as it


class MapVIDApp(Core):
  """
  An utility class that handles the dumping of map
  specification data from the KB.

  FIXME

  special case::

    foo  well_label
    xx   fooplate:A01

  """

  SUPPORTED_SOURCE_TYPES = ['Tube', 'Individual', 'TiterPlate', 'PlateWell',
                            'Chip', 'DataSample']

  def __init__(self, host=None, user=None, passwd=None, keep_tokens=1,
               study_label=None,
               operator='Alfred E. Neumann', logger=None):
    """
    FIXME
    """
    super(MapVIDApp, self).__init__(host, user, passwd,
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

  def resolve_mapping_plate_well(self, source_type, labels):
    slot_labels = [make_unique_key(*l.split(':')) for l in labels]
    back_to_label = dict(it.izip(slot_labels, labels))

    mapping = {}
    self.logger.info('start selecting %s' % source_type.get_ome_table())
    # FIXME this is not going to scale...
    objs = self.kb.get_objects(source_type)
    for o in objs:
      if o.containerSlotLabelUK in slot_labels:
        mapping[back_to_label[o.containerSlotLabelUK]] = o.id
    self.logger.info('done selecting %s' % source_type.get_ome_table())
    return mapping

  def resolve_mapping_object(self, source_type, labels):
    mapping = {}
    self.logger.info('start selecting %s' % source_type.get_ome_table())
    # FIXME this is not going to scale...
    self.logger.debug('\tlabels: %s' % labels)
    objs = self.kb.get_objects(source_type)
    for o in objs:
      self.logger.debug('\t-> %s' % o.label)
      if o.label in labels:
        mapping[o.label] = o.id
    self.logger.info('done selecting %s' % source_type.get_ome_table())
    self.logger.debug('mapping: %s' % mapping)
    return mapping

  def resolve_mapping(self, source_type, labels):
    if source_type == self.kb.Individual:
      return self.resolve_mapping_individual(labels)
    elif source_type == self.kb.PlateWell:
      return self.resolve_mapping_plate_well(source_type, labels)
    else:
      return self.resolve_mapping_object(source_type, labels)

  def dump(self, ifile, source_type_label,
           column_label, transformed_column_label, ofile):
    if not hasattr(self.kb, source_type_label):
      msg = 'Unknown source type: %s' % source_type_label
      self.logger.critical(msg)
      raise ValueError(msg)
    source_type = getattr(self.kb, source_type_label)

    self.logger.info('start reading %s' % ifile.name)
    f = csv.DictReader(ifile, delimiter='\t')
    records = [r for r in f]
    self.logger.info('done reading %s' % ifile.name)

    if len(records) == 0:
      self.logger.info('file %s is empty.' % ifile.name)
      return

    labels = [r[column_label] for r in records]
    mapping = self.resolve_mapping(source_type, labels)

    self.logger.info('start writing %s' % ofile.name)
    fieldnames = [k for k in records[0].keys() if k != column_label]
    fieldnames.append(transformed_column_label)
    o = csv.DictWriter(ofile, fieldnames=fieldnames, delimiter='\t')
    o.writeheader()
    for r in records:
      r[transformed_column_label] = mapping[r.pop(column_label)]
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
  def pair_of_names(s):
    parts = s.split(',')
    if len(parts) == 1:
      return tuple([parts[0], 'source'])
    elif len(parts) == 2:
      return tuple(parts)
    else:
      raise ValueError()

  parser.add_argument('-i', '--ifile', type=argparse.FileType('r'),
                      help='the input tsv file',
                      required=True)
  parser.add_argument('--source-type', type=str,
                      choices=MapVIDApp.SUPPORTED_SOURCE_TYPES,
                      help="""assigned source type, it is taken as
                      the type of the first name in the column flag.""",
                      required=True)
  parser.add_argument('--column', type=pair_of_names,
                      help="""comma separated list (no spaces) of the
                      object labels column name and the name of the
                      replacement column. The latter defaults to 'source'""",
                      required=True)
  parser.add_argument('--study', type=str,
                      help="study label")

def import_map_implementation(logger, args):
  #--
  app = MapVIDApp(host=args.host, user=args.user, passwd=args.passwd,
               keep_tokens=args.keep_tokens,
               study_label=args.study, logger=logger)

  app.dump(args.ifile, args.source_type,
           args.column[0], args.column[1], args.ofile)

def do_register(registration_list):
  registration_list.append(('map_vid', help_doc,
                            make_parser_map,
                            import_map_implementation))


