# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
Map user-visible labels to VIDs
===============================

Reads an input tsv file, replaces labels with VIDs for the specified
columns and outputs a new tsv files with the VIDs.
"""

import sys, os, csv, argparse
import itertools as it

from bl.vl.app.importer.core import Core
from bl.vl.kb.drivers.omero.utils import make_unique_key


class MappingError(Exception):
  pass


class MapVIDApp(Core):

  SUPPORTED_SOURCE_TYPES = ['Tube', 'Individual', 'TiterPlate', 'PlateWell',
                            'Chip', 'DataSample', 'Scanner',
                            'SoftwareProgram', 'SNPMarkersSet',
                            'DataCollectionItem', 'Device',
                            'FlowCell', 'Lane', 'SequencerOutput',
                            'IlluminaArrayOfArrays', 'IlluminaBeadChipArray']

  def __init__(self, host=None, user=None, passwd=None, keep_tokens=1,
               study_label=None, mset_label=None,
               operator='Alfred E. Neumann', logger=None):
    super(MapVIDApp, self).__init__(host, user, passwd, keep_tokens=keep_tokens,
                                    study_label=study_label, logger=logger)
    if study_label is None:
      self.default_study = None
    self.mset_label = mset_label

  def resolve_mapping_individual(self, labels):
    def check_labels(labels):
      good_labels = []
      for l in labels:
        if ':' not in l:
          self.logger.error('Invalid syntax for label %s, cannot map' % l)
          continue
        good_labels.append(l)
      return good_labels
    mapping = {}
    self.logger.info('start selecting enrolled individuals')
    labels = check_labels(labels)
    if len(labels) == 0:
      return mapping
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
      except KeyError as ke:
        self.logger.error('Cannot map label %s' % l)
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

  def resolve_mapping_marker(self):
    ms = self.kb.get_snp_markers_set(self.mset_label)
    if not ms:
      self.logger.error("unknown marker set label '%s'" % self.mset_label)
      return {}
    mapping = {}
    self.logger.info('mapping markers from set %s' % self.mset_label)
    if not ms.has_markers():
      ms.load_markers()
    mapping = dict(zip(ms.markers['label'], ms.markers['vid']))
    self.logger.info('done mapping markers from set %s' % self.mset_label)
    return mapping

  def resolve_mapping(self, source_type, labels):
    if source_type == self.kb.Individual:
      return self.resolve_mapping_individual(labels)
    elif source_type == self.kb.PlateWell or source_type == self.kb.IlluminaBeadChipArray:
      return self.resolve_mapping_plate_well(source_type, labels)
    elif source_type == self.kb.DataCollectionItem:
      return self.resolve_mapping_data_collection_item(source_type, labels)
    else:
      return self.resolve_mapping_object(source_type, labels)

  def dump(self, ifile, source_type_label,
           column_label, transformed_column_label, ofile,
           strict_mapping):
    def get_out_writer(ofile, fieldnames, old_column_label, new_column_label):
        old_column_index = fieldnames.index(old_column_label)
        fieldnames.remove(old_column_label)
        fieldnames.insert(old_column_index, new_column_label)
        o = csv.DictWriter(ofile, fieldnames, delimiter='\t',
                           extrasaction='ignore', lineterminator=os.linesep)
        o.writeheader()
        return o
    if not hasattr(self.kb, source_type_label):
      msg = 'Unknown source type: %s' % source_type_label
      self.logger.critical(msg)
      raise ValueError(msg)
    source_type = getattr(self.kb, source_type_label)
    self.logger.info('start reading %s' % ifile.name)
    f = csv.DictReader(ifile, delimiter='\t')
    records = [r for r in f]
    self.logger.info('done reading %s' % ifile.name)
    o = get_out_writer(ofile, f.fieldnames, column_label, transformed_column_label)
    if len(records) == 0:
      ifile.close()
      ofile.close()
      msg = 'No records are going to be mapped'
      self.logger.critical(msg)
      sys.exit(msg)
    if source_type == self.kb.Individual and self.default_study:
      labels = ['%s:%s' % (self.default_study.label, r[column_label])
                for r in records]
    else:
      labels = [r[column_label] for r in records]
    mapping = self.resolve_mapping(source_type, labels)
    self.logger.debug('mapped %d records' % len(mapping))
    self.logger.info('start writing %s' % ofile.name)
    mapped_records = []
    for r in records:
      if source_type == self.kb.Individual and self.default_study:
        field = '%s:%s' % (self.default_study.label, r[column_label])
      else:
        field = r[column_label]
      if field in mapping:
        r[transformed_column_label] = mapping[field]
        mapped_records.append(r)
      elif field == 'None':
        r[transformed_column_label] = field
        mapped_records.append(r)
    if strict_mapping and len(records) != len(mapped_records):
        msg = '%d unmapped records' % (len(records) - len(mapped_records))
        self.logger.critical(msg)
        raise MappingError(msg)
    for r in mapped_records:
      o.writerow(r)
    ifile.close()
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
    '-S', '--show-source-types', action="store_true",
    help="show supported source types and exit",
    )
  parser.add_argument(
    '-i', '--ifile', type=argparse.FileType('r'), help='input tsv file'
    )
  parser.add_argument(
    '--source-type', metavar="STRING",
    choices=MapVIDApp.SUPPORTED_SOURCE_TYPES, help="source type, e.g., Tube"
    )
  parser.add_argument(
    '--column', type=pair_of_names, metavar="N1,N2",
    help="""comma-separated pair of column names: the first one identifies
    the set of input labels to map, while the second one, which
    defaults to 'source', will be used for mapped output values""")
  parser.add_argument('--study', metavar="STRING", help="study label")
  parser.add_argument('--marker-set', metavar="STRING",
                      help="marker set label (only for markers)")
  parser.add_argument('--strict-mapping', action='store_true',
                      help='raise an exception if one or more records are not mapped')


def validate_args(args):
  if args.show_source_types:
    print "Supported source types:"
    for t in sorted(MapVIDApp.SUPPORTED_SOURCE_TYPES):
      print "\t%s" % t
    sys.exit(0)
  if args.source_type == "Marker" and not args.marker_set:
    sys.exit("error: --marker-set must be specified when mapping markers")
  # we can't simply set required=True for the following, that would break -S
  for name in "ifile", "source_type", "column":
    if not getattr(args, name):
      sys.exit("error: argument %s is required" % name)


def implementation(logger, host, user, passwd, args):
  validate_args(args)
  app = MapVIDApp(host=host, user=user, passwd=passwd,
                  keep_tokens=args.keep_tokens, study_label=args.study,
                  mset_label=args.marker_set, logger=logger)
  app.dump(args.ifile, args.source_type,
           args.column[0], args.column[1], args.ofile,
           args.strict_mapping)


def do_register(registration_list):
  registration_list.append(('map_vid', help_doc, make_parser, implementation))
