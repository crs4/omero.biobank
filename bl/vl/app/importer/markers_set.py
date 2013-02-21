# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
Import markers_set
==================

A SNPMarkersSet represents an ordered list of markers where the order
usually comes from aligning the SNP markers against a reference
genome. Within Biobank, different genotyping technologies are mapped
to different SNPMarkersSet instances.

In more detail, a marker set is defined by:

 * identification information:

   * maker: the name of the organization that has defined the
     SNPMarkersSet, e.g., 'CRS4'

   * model: the specific 'model' of the SNPMarkersSet, e.g.,
     'AffymetrixGenome6.0'

   * release: a string that identifies this specific instance, e.g.,
     'aligned_on_human_g1k_v37'

 * reference list: for each marker that should go in the list,
   the following information is provided:

   * marker_vid: the vid identifier of the marker

   * marker_indx: the position of the marker within the marker list

   * allele_flip: False if the alleles are in the same order as
     recorded in the marker definition, True if they are swapped.

For instance::

  marker_vid  marker_indx  allele_flip
  V902909090  0            False
  V902909091  1            False
  V902909092  2            True
  ...
"""
import os, time, csv, json, copy

import core
from version import version


class Recorder(core.Core):
  
  def __init__(self, study_label, host=None, user=None, passwd=None,
               keep_tokens=1, operator='Alfred E. Neumann',
               action_setup_conf=None, logger=None):
    super(Recorder, self).__init__(host, user, passwd, keep_tokens=keep_tokens,
                                   study_label=study_label, logger=logger)
    self.action_setup_conf = action_setup_conf
    self.operator = operator

  def record(self, records, otsv, rtsv):
    if len(records) == 0:
      self.logger.warn('no records')
      return
    self.logger.info('start preloading marker vids')
    self.preloaded_marker_vids = set(
      m[0] for m in self.kb.get_snp_marker_definitions(col_names=["vid"])
      )
    self.logger.info('done preloading marker vids')
    good_records, bad_records = self.do_consistency_checks(records)
    for br in bad_records:
      rtsv.writerow(br)
    if len(good_records) != len(records):
      msg = 'cannot process an incomplete markers_set definition'
      self.logger.critical(msg)
      raise ValueError(msg)
    else:
      records = good_records
    study = self.find_study(records)
    action = self.find_action(study)
    label, maker, model, release = self.find_markers_set_label(records)
    N = len(records)
    def stream():
      for r in records:
        yield r['marker_vid'], r['marker_indx'], r['allele_flip']
    mset = self.kb.create_snp_markers_set(label, maker, model, release,
                                          N, stream(), action)
    otsv.writerow({
      'study': study.label,
      'label': mset.label,
      'type': mset.get_ome_table(),
      'vid': mset.id,
      })

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
    conf = {
      'setup': asetup,
      'device': device,
      'actionCategory': acat,
      'operator': self.operator,
      'context': study,
      }
    action = self.kb.factory.create(self.kb.Action, conf)
    return action.save()

  def do_consistency_checks(self, records):
    good_records = []
    bad_records = []
    maker = records[0]['maker']
    model = records[0]['model']
    release = records[0]['release']
    study = records[0]['study']
    preloaded_marker_vids = self.preloaded_marker_vids  # speed hack
    for i, r in enumerate(records):
      reject = 'Rejecting import of row %d: ' % i
      if r['marker_vid'] not in preloaded_marker_vids:
        f = 'there is no knwon marker with ID %s' % r['marker_vid']
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      if r['maker'] != maker:
        f = 'inconsistent maker'
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      if r['model'] != model:
        f = 'inconsistent model'
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      if r['release'] != release:
        f = 'inconsistent release'
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      if r['study'] != study:
        f = 'inconsistent study'
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      good_records.append(r)
    return good_records, bad_records


class RecordCanonizer(core.RecordCanonizer):

  def canonize(self, r):
    super(RecordCanonizer, self).canonize(r)
    r['marker_indx'] = int(r['marker_indx'])
    r['allele_flip'] = r['allele_flip'].upper() == 'TRUE'


help_doc = """
import new markers set definitions into the KB.
"""


def make_parser(parser):
  parser.add_argument('--study', metavar="STRING", required=True,
                      help="study label")
  parser.add_argument('--label', metavar="STRING",
                      help="markers_set unique label")
  parser.add_argument('--maker', metavar="STRING", required=True,
                      help="markers_set maker")
  parser.add_argument('--model', metavar="STRING", required=True,
                      help="markers_set model")
  parser.add_argument('--release', metavar="STRING", required=True,
                      help="markers_set release")


def implementation(logger, host, user, passwd, args):
  action_setup_conf = Recorder.find_action_setup_conf(args)
  recorder = Recorder(args.study,
                      host=host, user=user, passwd=passwd,
                      operator=args.operator,
                      action_setup_conf=action_setup_conf, logger=logger)
  for m in recorder.kb.get_objects(recorder.kb.SNPMarkersSet):
    if m.label == args.label:
      msg = 'a marker set labeled %s is already present in the kb' % args.label
      logger.error(msg)
      return
  f = csv.DictReader(args.ifile, delimiter='\t')
  logger.info('start processing file %s' % args.ifile.name)
  records = [r for r in f]
  fields_to_canonize = ['study', 'label', 'maker', 'model', 'release']
  canonizer = RecordCanonizer(fields_to_canonize, args)
  canonizer.canonize_list(records)
  if len(records) > 0:
    o = csv.DictWriter(args.ofile,
                       fieldnames=['study', 'label', 'type', 'vid'],
                       delimiter='\t', lineterminator=os.linesep)
    o.writeheader()
    report_fnames = copy.deepcopy(f.fieldnames)
    report_fnames.append('error')
    report = csv.DictWriter(args.report_file, report_fnames,
                            delimiter='\t', lineterminator=os.linesep,
                            extrasaction='ignore')
    report.writeheader()
    recorder.record(records, o, report)
  else:
    logger.info('empty file')
  args.ifile.close()
  args.ofile.close()
  args.report_file.close()
  logger.info('done processing file %s' % args.ifile.name)


def do_register(registration_list):
  registration_list.append(('markers_set', help_doc, make_parser,
                            implementation))
