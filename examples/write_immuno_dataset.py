""" ...

Read an Illumina 'Final Report' text file and write snp calls for each
well as an SSC (SampleSnpCall) file in the x-ssc-messages format.

"""

# 'call' --> 'Allele1 - AB' + 'Allele2 - AB'
# 'confidence' --> 'GC Score' (1-gc_score? 1/gc_score?)
# 'sig_A' --> 'X Raw'
# 'sig_B' --> 'Y Raw'
# 'weight_AA' --> ?
# 'weight_AB' --> ?
# 'weight_BB' --> ?
#
# weights: for now, whatever fits the call (e.g., (1, 0, 0) for AA).
#
# FIXME: NOT FINISHED!

import sys, argparse, logging

from bl.core.io.illumina import GenomeStudioFinalReportReader as DataReader
from bl.core.io.illumina import IllSNPReader
from bl.core.io import MessageStreamWriter
import bl.core.gt.messages.SnpCall as SnpCall
from bl.vl.kb import KBError


LOG_FORMAT = '%(asctime)s|%(levelname)-8s|%(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'
LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

MSET_LABEL = "IMMUNO_BC_11419691_B"  # FIXME could be a param

PAYLOAD_MSG_TYPE = 'core.gt.messages.SampleSnpCall'


def get_marker_ids(kb, mset_label):
  ms = kb.get_snp_markers_set(label=mset_label)
  if ms is None:
    msg = "unable to retrieve marker set %s" % mset_label
    logger.critical(msg)
    raise KBError(msg)
  logger.info("loading markers for marker set %s" % mset_label)
  ms.load_markers()
  logger.info("%s consists of %d markers" % (mset_label, len(ms.markers)))
  return dict((m.label, m.id) for m in ms.markers)


def get_marker_name_to_label(ill_annot_file):
  with open(ill_annot_file) as f:
    reader = IllSNPReader(f)
    return dict((r["Name"], r["IlmnID"]) for r in reader)


def get_marker_name_to_id(marker_name_to_label, marker_ids):
  marker_name_to_id = {}
  assert(len(marker_name_to_label) == len(marker_ids))
  for name, label in marker_name_to_label.iteritems():
    try:
      marker_name_to_id[name] = marker_ids[label]
    except KeyError:
      msg = "no marker in kb is labeled as %s" % label
      logger.critical(msg)
      raise KBError(msg)
  return marker_name_to_id


def write_ssc_data_set_file(out_fn, marker_name_to_id, device_id,
                            min_datetime, max_datetime, data_block):
  sample_id = block.sample_id  # FIXME: merge with plate_id
  header = {'device_id' : device_id,
            'sample_id' : sample_id,
            'min_datetime' : '%s' % min_datetime,
            'max_datetime' : '%s' % max_datetime}
  stream = MessageStreamWriter(out_fn, PAYLOAD_MSG_TYPE, header)
  for k in block.snp_names():
    snp = block.snp[k]
    snp_id = marker_name_to_id[snp['SNP Name']]
    stream.write({
      'sample_id' : sample_id,
      'snp_id' : snp_id,
      'call' : 'FIXME',
      'confidence' : 'FIXME',
      'sig_A' : 'FIXME',
      'sig_B' : 'FIXME',
      'weight_AA' : 'FIXME',
      'weight_AB' : 'FIXME',
      'weight_BB' : 'FIXME',
  stream.close()


def make_parser():
  parser = argparse.ArgumentParser(description="write Immunochip data files")
  parser.add_argument('ifiles', metavar='FILE', type=str, nargs='+',
                      help='input FinalReport text files')
  parser.add_argument('-a', '--annot-file', metavar='FILE', required=True,
                      help='Original Illumina SNP annotation file')
  parser.add_argument('--logfile', type=str, help='log file (default=stderr)')
  parser.add_argument('--loglevel', type=str, choices=LOG_LEVELS,
                      help='logging level', default='INFO')
  parser.add_argument('--run-id', type=str, required=True,
                      help='a unique identifier for this run')
  parser.add_argument('--device-label', type=str, help='device label')
  parser.add_argument('--prefix', type=str, help='output files prefix',
                      default='vl-immuno-')
  parser.add_argument('-H', '--host', type=str, help='omero hostname',
                      default='localhost')
  parser.add_argument('-U', '--user', type=str, help='omero user',
                      default='root')
  parser.add_argument('-P', '--passwd', type=str, help='omero password',
                      required=True)
  return parser


def main(argv):
  parser = make_parser()
  args = parser.parse_args(argv)
  log_level = getattr(logging, args.loglevel)
  kwargs = {'format': LOG_FORMAT, 'datefmt': LOG_DATEFMT, 'level': log_level}
  if args.logfile:
    kwargs['filename'] = args.logfile
  logging.basicConfig(**kwargs)
  logger = logging.getLogger()
  kb = KB(driver='omero')(args.host, args.user, args.passwd)
  marker_ids = get_marker_ids(kb, MSET_LABEL)
  marker_name_to_label = get_marker_name_to_label(args.annot_file)
  marker_name_to_id = get_marker_name_to_id(marker_name_to_label, marker_ids)
  for fn in args.ifile:
    with open(fn) as f:
      data_reader = DataReader(f)
      for block in data_reader.get_sample_iterator():
        write_ssc_data_set_file('FIXME')


main(sys.argv[1:])
