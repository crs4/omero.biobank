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

 * for the whole set:

   * maker: the name of the organization that has defined the
     SNPMarkersSet, e.g., 'CRS4'

   * model: the specific 'model' of the SNPMarkersSet, e.g.,
     'AffymetrixGenome6.0'

   * release: a string that identifies this specific instance, e.g.,
     'aligned_on_human_g1k_v37'

 * for each marker in the set:

   * label: a descriptive string, usually assigned by the manufacturer
     and unique within the specific genotyping platform

   * mask: a string that describes the DNA structure of the marker, in
     the <FLANK>[A/B]<FLANK> format, e.g., ACGTCCAC[A/G]ACTAGCTA.  All
     input masks are expected to be in the TOP Illumina convention, if
     the Illumina strand detection algorithm yields a result (see
     :func:`~bl.vl.utils.snp.convert_to_top`).

   * index: the position of the marker within the marker list

   * allele_flip: False if the alleles are in the same order as
     recorded in the manufacturer's data sheet

Set info is provided through command line parameters, while per-marker
info must be listed in the tab-separated input file.  For instance::

  label  mask               index  allele_flip
  SNP-1  ACGTCC[A/G]ACTAGC  0      False
  SNP-2  CGATCG[T/C]ACACTG  1      False
  SNP-3  TGACTA[T/G]TAGCGA  2      True
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
        yield r['label'], r['mask'], r['index'], r['allele_flip']
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
    return r['ms_label'], r['maker'], r['model'], r['release']

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
    for i, r in enumerate(records):
      reject = 'Rejecting import of row %d: ' % i
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
    r['index'] = int(r['index'])
    r['allele_flip'] = r['allele_flip'].strip().upper()
    r['allele_flip'] = r['allele_flip'] == 'TRUE' or r['allele_flip'] == '1'


help_doc = """
import new markers set definitions into the KB.
"""


def make_parser(parser):
  parser.add_argument('--study', metavar="STRING", required=True,
                      help="study label")
  parser.add_argument('--label', metavar="STRING", dest="ms_label",
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
    if m.label == args.ms_label:
      logger.error(
        'a marker set labeled %s is already present in the kb' % args.ms_label
        )
      return
  f = csv.DictReader(args.ifile, delimiter='\t')
  logger.info('start processing file %s' % args.ifile.name)
  records = [r for r in f]
  fields_to_canonize = ['study', 'ms_label', 'maker', 'model', 'release']
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
