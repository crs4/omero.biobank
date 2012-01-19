"""
Import Marker Alignments
========================

Will read in a tsv file with the following columns::

  marker_vid ref_genome chromosome pos      strand allele copies
  V0909090   hg18       10         82938938 True   A      1
  V0909091   hg18       1          82938999 True   A      2
  V0909092   hg18       1          82938938 True   B      2
  ...

The pos fields is with respect to 5' and thus, if the marker has been
aligned on the other strand, it is the responsibility of the aligner
app to report the actual distance from 5', while, at the same time,
registering that the snp has actually been aligned on the other strand.

The chromosome field is an integer with values in the range(1, 25)
with 23 (X), 24 (Y), 25(XY) and 26(MT).
"""
import csv, json, time

from core import Core
from version import version


MANDATORY_FIELDS = ['marker_vid', 'ref_genome', 'chromosome', 'pos', 'strand',
                    'allele', 'copies']
STRAND_ENCODINGS = frozenset(['TRUE', '+'])


class Recorder(Core):
  
  def __init__(self, study_label, host=None, user=None, passwd=None,
               keep_tokens=1, action_setup_conf=None, logger=None,
               operator='Alfred E. Neumann', ms_label=None):
    super(Recorder, self).__init__(host, user, passwd, keep_tokens=keep_tokens,
                                   study_label=study_label, logger=logger)
    self.action_setup_conf = action_setup_conf
    device_label = ('importer.marker_alignment.SNP-marker-alignment-%s' %
                    version)
    device = self.get_device(label=device_label,
                             maker='CRS4', model='importer', release='0.1')
    asetup = self.get_action_setup(
      'importer.marker_alignment-%f' % time.time(),
      json.dumps(self.action_setup_conf)
      )
    acat = self.kb.ActionCategory.IMPORT
    self.action = self.kb.factory.create(self.kb.Action,
                                         {'setup' : asetup,
                                          'device' : device,
                                          'actionCategory' : acat,
                                          'operator' : operator,
                                          'context' : self.default_study,
                                          })
    #-- FIXME what happens if we do not have alignments to save?
    self.action.save()
    self.mset_vid = self.__get_mset_vid(ms_label)

  def __get_mset_vid(self, ms_label):
    if ms_label is None:
      return None
    mset = self.kb.get_snp_markers_set(ms_label)
    if mset is None:
      self.logger.warn('no marker set labeled %r, setting to None' % ms_label)
      return None
    return mset.id

  def do_consistency_checks(self, records):
    self.logger.info('start consistency checks')
    vids = [r['marker_vid'] for r in records]
    markers = dict((m.id, m) for m in self.kb.get_snp_markers(
      vids=vids, col_names=['vid']
      ))
    accepted = []
    for i, r in enumerate(records):
      reject = 'Rejecting import of record %d: ' % i
      if not r['marker_vid'] in markers:
        f = reject + 'unkown marker_vid value.'
        self.logger.error(f)
        continue
      if self.missing_fields(MANDATORY_FIELDS, r):
        f = reject + 'missing mandatory field.'
        self.logger.error(f)
        continue
      if not 0 < r['chromosome'] < 27:
        f = reject + 'chomosome value out ot the [1:26] range.'
        self.logger.error(f)
        continue
      if not 0 < r['pos']:
        f = reject + 'non positive pos.'
        self.logger.error(f)
        continue
      accepted.append(r)
    return accepted

  def record(self, records):
    records = self.do_consistency_checks(records)
    self.kb.add_snp_alignments(records, op_vid=self.action.id,
                               ms_vid=self.mset_vid)


def canonize_records(args, records):
  fields = ['study', 'ref_genome']
  for f in fields:
    v = getattr(args, f, None)
    if v is not None:
      for r in records:
        r[f] = v
  for r in records:
    r['chromosome'] = int(r['chromosome'])
    r['pos'] = int(r['pos'])
    r['global_pos'] = 10**10 * r['chromosome'] + r['pos']
    r['strand'] = r['strand'].upper() in STRAND_ENCODINGS
    r['copies'] = int(r['copies'])
  return records


help_doc = """
import new marker alignments into VL.
"""


def make_parser_marker_alignment(parser):
  parser.add_argument('-S', '--study', type=str,
                      help="""context study label""")
  parser.add_argument('--ref-genome', type=str,
                      help="""reference genome used""")
  parser.add_argument('--markers-set', type=str,
                      help="""related markers set, if any""")


def import_marker_alignment_implementation(logger, args):
  if not (args.study):
    msg = 'missing context study label'
    logger.critical(msg)
    raise ValueError(msg)
  action_setup_conf = Recorder.find_action_setup_conf(args)
  recorder = Recorder(args.study,
                      host=args.host, user=args.user, passwd=args.passwd,
                      action_setup_conf=action_setup_conf,
                      operator=args.operator, logger=logger, keep_tokens=1,
                      ms_label=args.markers_set)
  f = csv.DictReader(args.ifile, delimiter='\t')
  recorder.logger.info('start processing file %s' % args.ifile.name)
  records = []
  for r in f:
    if r['chromosome'] == 'None':
      logger.warn('%s: chr is None, skipping' % r['marker_vid'])
    else:
      records.append(r)
  canonize_records(args, records)
  recorder.record(records)
  recorder.logger.info('done processing file %s' % args.ifile.name)


def do_register(registration_list):
  registration_list.append(('marker_alignment', help_doc,
                            make_parser_marker_alignment,
                            import_marker_alignment_implementation))
