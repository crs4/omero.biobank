# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
Map user-visible labels to VIDs
===============================

Reads an input tsv file, replaces labels with VIDs for the specified
columns and outputs a new tsv files with the VIDs.
"""

import csv, argparse, sys
import itertools as it

from bl.vl.app.importer.core import Core
from bl.vl.kb.drivers.omero.utils import make_unique_key


class MapVIDApp(Core):

  SUPPORTED_SOURCE_TYPES = ['Tube', 'Individual', 'TiterPlate', 'PlateWell',
                            'Chip', 'DataSample', 'Marker', 'Scanner',
                            'SoftwareProgram', 'SNPMarkersSet', 'DataCollectionItem']

  def __init__(self, host=None, user=None, passwd=None, keep_tokens=1,
               study_label=None, operator='Alfred E. Neumann', logger=None):
    super(MapVIDApp, self).__init__(host, user, passwd, keep_tokens=keep_tokens,
                                    study_label=study_label, logger=logger)
    if study_label is None:
      self.default_study = None

  def resolve_mapping_individual(self, labels):
    mapping = {}
    self.logger.info('start selecting enrolled individuals')
    known_studies = set([lab.split(':')[0] for lab in labels])
    known_enrollments = []
    for kst in known_studies:
      known_enrollments.extend(self.kb.get_enrolled(self.kb.get_study(kst)))
      self.logger.debug('Loaded enrollments for study %s' % kst)
    for e in known_enrollments:
      enroll_label = '%s:%s' % (e.study.label, e.studyCode)
      if enroll_label in labels:
        i = e.individual
        mapping[enroll_label] = i.id
    diff = set(labels).difference(mapping)
    if len(diff) > 0:
      for x in diff:
        self.logger.error('cannot map %s as an individual' % x)
      self.logger.error('the lines with unmapped individuals will be ignored.')
    self.logger.info('done selecting enrolled individuals')
    return mapping

  def resolve_mapping_plate_well(self, source_type, labels):
    slot_labels = [make_unique_key(*l.split(':')) for l in labels]
    back_to_label = dict(it.izip(slot_labels, labels))
    mapping = {}
    self.logger.info('start selecting %s' % source_type.get_ome_table())
    # FIXME this is not going to scale
    objs = self.kb.get_objects(source_type)
    for o in objs:
      if o.containerSlotLabelUK in slot_labels:
        mapping[back_to_label[o.containerSlotLabelUK]] = o.id
    self.logger.info('done selecting %s' % source_type.get_ome_table())
    return mapping

  def resolve_mapping_data_collection_item(self, source_type, labels):
    self.logger.info('start selecting %s' % source_type.get_ome_table())
    self.logger.debug('retrieving data collections')
    dcols = self.kb.get_objects(self.kb.DataCollection)
    self.logger.debug('%d data collections loaded' % len(dcols))
    dcols_map = {}
    for dc in dcols:
      self.logger.debug('loading items for data collection %s' % dc.label)
      dc_items = self.kb.get_data_collection_items(dc)
      self.logger.debug('%d data collection items loaded' % len(dc_items))
      for dci in dc_items:
        dcols_map.setdefault(dc.label, {})[dci.dataSample.label] = dci
    self.logger.info('done selection %s' % source_type.get_ome_table())
    mapping = {}
    for l in labels:
      try:
        mapping[l] = dcols_map[l.split(':')[0]][l.split(':')[1]].id
      except KeyError, ke:
        self.error('Cannot map label %s' % l)
        self.logger.debug('invalid key %s' % ke)
    return mapping

  def resolve_mapping_object(self, source_type, labels):
    mapping = {}
    self.logger.info('start selecting %s' % source_type.get_ome_table())
    # FIXME this is not going to scale
    self.logger.debug('\tlabels: %s' % labels)
    objs = self.kb.get_objects(source_type)
    for o in objs:
      self.logger.debug('\t-> %s' % o.label)
      if o.label in labels:
        mapping[o.label] = o.id
    self.logger.info('done selecting %s' % source_type.get_ome_table())
    self.logger.debug('mapping: %s' % mapping)
    return mapping

  def resolve_mapping_marker(self, source_type, labels):
    mapping = {}
    self.logger.info('start selecting %s' % source_type.get_ome_table())
    objs = self.kb.get_snp_markers(labels=labels, col_names=['vid', 'label'])
    for o in objs:
      mapping[o.label] = o.id
    self.logger.info('done selecting %s' % source_type.get_ome_table())
    return mapping

  def resolve_mapping(self, source_type, labels):
    if source_type == self.kb.Individual:
      return self.resolve_mapping_individual(labels)
    elif source_type == self.kb.PlateWell:
      return self.resolve_mapping_plate_well(source_type, labels)
    elif source_type == self.kb.Marker:
      return self.resolve_mapping_marker(source_type, labels)
    elif source_type == self.kb.DataCollectionItem:
      return self.resolve_mapping_data_collection_item(source_type, labels)
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
    if source_type == self.kb.Individual:
      if self.default_study:
        labels = ['%s:%s' % (self.default_study.label, r[column_label])
                  for r in records]
      else:
        try:
          labels = ['%s:%s' % (r['study'], r[column_label])
                    for r in records]
        except KeyError, ke:
          msg = 'No %s column and no default study provided' % ke
          self.logger.critical(msg)
          sys.exit(msg)
    else:
      labels = [r[column_label] for r in records]
    mapping = self.resolve_mapping(source_type, labels)
    self.logger.debug('mapped %d records' % len(mapping))
    self.logger.info('start writing %s' % ofile.name)
    fieldnames = [k for k in records[0].keys() if k != column_label]
    fieldnames.append(transformed_column_label)
    o = csv.DictWriter(ofile, fieldnames=fieldnames, delimiter='\t',
                       extrasaction = 'ignore')
    o.writeheader()
    for r in records:
      if source_type == self.kb.Individual:
        if self.default_study:
          field = '%s:%s' % (self.default_study.label, r[column_label])
        else:
          field = '%s:%s' % (r['study'], r[column_label])
      else:
        field = r[column_label]
      if field in mapping:
        r[transformed_column_label] = mapping[field]
        # if column_label != transformed_column_label:
        #   r.pop(column_label)
        o.writerow(r)
    ofile.close()
    self.logger.info('done writing %s' % ofile.name)


help_doc = """
Map user-visible labels to KB VIDs
"""


def make_parser(parser):
  def pair_of_names(s):
    parts = s.split(',')
    if len(parts) == 1:
      return tuple([parts[0], 'source'])
    elif len(parts) == 2:
      return tuple(parts)
    else:
      raise ValueError()
  parser.add_argument(
    '-i', '--ifile', type=argparse.FileType('r'),
    help='input tsv file', required=True
    )
  parser.add_argument(
    '--source-type', metavar="STRING", required=True,
    choices=MapVIDApp.SUPPORTED_SOURCE_TYPES, help="source type, e.g., Tube"
    )
  parser.add_argument(
    '--column', type=pair_of_names, metavar="N1,N2", required=True,
    help="""comma-separated pair of column names: the first one identifies
    the set of input labels to map, while the second one, which
    defaults to 'source', will be used for mapped output values""")
  parser.add_argument('--study', metavar="STRING", help="study label")


def implementation(logger, host, user, passwd, args):
  app = MapVIDApp(host=host, user=user, passwd=passwd,
                  keep_tokens=args.keep_tokens,
                  study_label=args.study, logger=logger)
  app.dump(args.ifile, args.source_type,
           args.column[0], args.column[1], args.ofile)


def do_register(registration_list):
  registration_list.append(('map_vid', help_doc, make_parser, implementation))
