"""
Import Markers Set Definitions
==============================

A **SNPMarkersSet** is, essentially, an ordered list of markers where
the order is, in principle, arbitrary, but it usually comes from the
alignment of the SNP markers against a reference genome.  Within
OMERO.biobank, different genotyping technologies are mapped to
different SNPMarkersSet instances.

In more detail, a marker set is defined by:

 * identification information:

   * **maker:** the name of the organization that has defined the
     SNPMarkersSet, e.g., 'CRS4'

   * **model** the specific 'model' of the SNPMarkersSet, e.g.,
     'AffymetrixGenome6.0'

   * **release** a string that identifies this specific instance, e.g.,
     'aligned_on_human_g1k_v37'

 * markers reference list: for each marker that should go in the list,
   the following information is provided:

   * **marker_vid** the vid identifier of the marker

   * **marker_indx** the position of the marker within the marker list

   * **allele_flip** False if the alleles are in the same order as
     recorded in the marker definition, True if they are swapped.

For instance::

  marker_vid   marker_indx allele_flip
  V902909090  0            False
  V902909091  1            False
  V902909092  2            True
  ...
"""
import sys, os, time, csv, json

from core import Core
from version import version


class Recorder(Core):
  
  def __init__(self, study_label, host=None, user=None, passwd=None,
               keep_tokens=1, operator='Alfred E. Neumann',
               action_setup_conf=None, logger=None):
    super(Recorder, self).__init__(host, user, passwd, keep_tokens=keep_tokens,
                                   study_label=study_label, logger=logger)
    self.action_setup_conf = action_setup_conf
    self.operator = operator
    self.preloaded_markers = {}
    self.preloaded_markers_sets = {}

  def record(self, records, otsv):
    if len(records) == 0:
      self.logger.warn('no records')
    self.preload_markers_sets()
    # FIXME this is not very efficient, we could directly check here
    # if we actually need to preload the markers
    self.preload_markers(records)
    records = self.do_consistency_checks(records)
    if not records:
      self.logger.warn('no records')
      return
    study  = self.find_study(records)
    action = self.find_action(study)
    label, maker, model, release = self.find_markers_set_label(records)
    self.logger.info('start creating markers set')
    set_vid = self.kb.add_snp_markers_set(maker, model, release, action.id)
    self.logger.info('done creating markers set')
    self.logger.info('start loading markers in marker set')
    N = self.kb.fill_snp_markers_set(set_vid, records, action.id)
    assert N == len(records)
    self.logger.info('done loading markers in marker set')
    self.logger.info('start creating gdo repository')
    self.kb.create_gdo_repository(set_vid, N)
    self.logger.info('done creating gdo repository')
    self.logger.info('start creating SNPMarkersSet')
    conf = {'label': label,
            'maker' : maker, 'model' : model, 'release' : release,
            'markersSetVID' : set_vid,
            'action' : action}
    mset = self.kb.factory.create(self.kb.SNPMarkersSet, conf).save()
    self.logger.info('done creating SNPMarkersSet')
    otsv.writerow({'study' : study.label,
                   'label' : mset.label,
                   'type'  : mset.get_ome_table(),
                   'vid'   : mset.id })

  def find_markers_set_label(self, records):
    r = records[0]
    return r['label'], r['maker'], r['model'], r['release']

  def find_action(self, study):
    device_label = ('importer.marker_definition.SNP-markers-set-%s' % version)
    device = self.get_device(label=device_label,
                             maker='CRS4', model='importer', release='0.1')
    asetup = self.get_action_setup('importer.markers_set-%f' % time.time(),
                                   json.dumps(self.action_setup_conf))
    acat = self.kb.ActionCategory.IMPORT
    action = self.kb.factory.create(self.kb.Action,
                                    {'setup' : asetup,
                                     'device' : device,
                                     'actionCategory' : acat,
                                     'operator' : self.operator,
                                     'context' : study,
                                     })
    return action.save()

  def preload_markers_sets(self):
    self.logger.info('start preloading SNPMarkersSet')
    msets = self.kb.get_objects(self.kb.SNPMarkersSet)
    self.preloaded_markers_sets = dict((m.label, m) for m in msets)
    self.logger.info('done preloading SNPMarkersSet')

  def preload_markers(self, records):
    self.logger.info('start preloading related markers')
    markers = self.kb.get_snp_markers(vids=[r['marker_vid'] for r in records])
    self.preloaded_markers = dict((m.id, m) for m in markers)
    self.logger.info('done preloading related markers')

  def do_consistency_checks(self, records):
    good_records = []
    label = records[0]['label']
    maker = records[0]['maker']
    model = records[0]['model']
    release = records[0]['release']
    study = records[0]['study']
    if label in self.preloaded_markers_sets:
      msg = 'a marker set labeled %s is already present in the kb' % label
      self.logger.critical(msg)
      sys.exit(1)
    for i, r in enumerate(records):
      reject = 'Rejecting import of row %d: ' % i
      if r['marker_vid'] not in self.preloaded_markers:
        f = reject + 'marker_vid not in VL'
        self.logger.error(f)
        continue
      if r['maker'] != maker:
        f = reject + 'inconsistent maker'
        self.logger.error(f)
        continue
      if r['model'] != model:
        f = reject + 'inconsistent model'
        self.logger.error(f)
        continue
      if r['release'] != release:
        f = reject + 'inconsistent release'
        self.logger.error(f)
        continue
      if r['study'] != study:
        f = reject + 'inconsistent study'
        self.logger.error(f)
        continue
      good_records.append(r)
    if len(good_records) != len(records):
      msg = 'cannot process an incomplete markers_set definition'
      self.logger.critical(msg)
      sys.exit(1)
    return good_records


def canonize_records(args, records):
  fields = ['study', 'label', 'maker', 'model', 'release']
  for f in fields:
    v = getattr(args, f, None)
    if v is not None:
      for r in records:
        r[f] = v
  for r in records:
    r['marker_indx'] = int(r['marker_indx'])
    r['allele_flip'] = {'TRUE': True, 'FALSE': False}[r['allele_flip'].upper()]


help_doc = """
import new markers set definition into VL.
"""


def make_parser_markers_set(parser):
  parser.add_argument('--study', type=str, required=True,
                      help="""context study label""")
  parser.add_argument('--label', type=str,
                      help="""markers_set unique label""")
  parser.add_argument('--maker', type=str, required=True,
                      help="""markers_set maker""")
  parser.add_argument('--model', type=str, required=True,
                      help="""markers_set model""")
  parser.add_argument('--release', type=str, required=True,
                      help="""markers set release""")


def import_markers_set_implementation(logger, args):
  action_setup_conf = Recorder.find_action_setup_conf(args)
  recorder = Recorder(args.study,
                      host=args.host, user=args.user, passwd=args.passwd,
                      operator=args.operator,
                      action_setup_conf=action_setup_conf,
                      logger=logger)
  f = csv.DictReader(args.ifile, delimiter='\t')
  logger.info('start processing file %s' % args.ifile.name)
  records = [r for r in f]
  canonize_records(args, records)
  if len(records) > 0:
    o = csv.DictWriter(args.ofile,
                       fieldnames=['study', 'label', 'type', 'vid'],
                       delimiter='\t', lineterminator=os.linesep)
    o.writeheader()
    recorder.record(records, o)
  else:
    logger.info('empty file')
  logger.info('done processing file %s' % args.ifile.name)


def do_register(registration_list):
  registration_list.append(('markers_set', help_doc,
                            make_parser_markers_set,
                            import_markers_set_implementation))
