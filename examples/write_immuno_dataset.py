""" ...

Read Illumina 'Final Report' text files and write snp calls for each
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
from bl.vl.app.importer.core import Core  # move this script to importer app?

LOG_FORMAT = '%(asctime)s|%(levelname)-8s|%(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'
LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

# FIXME should these be available as parameters?
MSET_LABEL = "IMMUNO_BC_11419691_B"
DEVICE_MAKER = "Illumina"
DEVICE_MODEL = "GenomeStudio"
#DEVICE_TYPE = "SoftwareProgram"

PAYLOAD_MSG_TYPE = 'core.gt.messages.SampleSnpCall'


class Writer(Core):

  def critical(msg):
    self.logger.critical(msg)
    raise KBError(msg)
  
  def get_marker_ids(self, mset_label):
    ms = self.kb.get_snp_markers_set(label=mset_label)
    if ms is None:
      self.critical("unable to retrieve marker set %s" % mset_label)
    self.logger.info("loading markers for marker set %s" % mset_label)
    ms.load_markers()
    self.logger.info("%s has %d markers" % (mset_label, len(ms.markers)))
    self.marker_ids = dict((m.label, m.id) for m in ms.markers)

  def get_marker_name_to_label(self, ill_annot_file):
    with open(ill_annot_file) as f:
      reader = IllSNPReader(f)
      self.marker_name_to_label = dict((r["Name"], r["IlmnID"]) for r in reader)

  def get_marker_name_to_id(self):
    self.marker_name_to_id = {}
    assert(len(self.marker_name_to_label) == len(self.marker_ids))
    for name, label in self.marker_name_to_label.iteritems():
      try:
        self.marker_name_to_id[name] = self.marker_ids[label]
      except KeyError:
        self.critical("no marker in kb is labeled as %s" % label)

  def get_plate_barcode(self, fn):
    plate_fields = os.path.splitext(fn)[0].split("_")[1:-1]
    plate_label = "_".join(plate_fields+["imm"])
    plate = kb.get_container(plate_label)
    if plate is None:
      self.critical("no container in kb is labeled as %s" % plate_label)
    return plate.barcode

  def write_ssc_data_set_file(self, prefix, plate_barcode, data_block,
                              min_datetime, max_datetime):
    #-NOTE: this is a CRS4-specific hack to fix ambiguous immunochip labels-
    padded_id = data_block.sample_id.rjust(12, '0')
    sample_id = "%s|%s" % (padded_id, plate_barcode)
    #-----------------------------------------------------------------------
    header = {'device_id' : device_id,
              'sample_id' : sample_id,
              'min_datetime' : '%s' % min_datetime,
              'max_datetime' : '%s' % max_datetime}
    stream = MessageStreamWriter(out_fn, PAYLOAD_MSG_TYPE, header)
    for k in data_block.snp_names():
      snp = data_block.snp[k]
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
  writer = Writer(args.host, args.user, args.passwd)
  writer.get_marker_ids(MSET_LABEL)
  writer.get_marker_name_to_label(args.annot_file)
  writer.get_marker_name_to_id()
  for fn in args.ifile:
    sample_id = writer.build_sample_id(fn)
    with open(fn) as f:
      data_reader = DataReader(f)
      release = data_reader.header["GSGT Version"]
      device_label = "%s.%s.%s" % (DEVICE_MAKER, DEVICE_MODEL, release)
      out_fname = '%s%s-%s.ssc' % (args.prefix, device.id, sample_id)
      for block in data_reader.get_sample_iterator():
        write_ssc_data_set_file(out_fn, marker_name_to_id, device_id,
                                min_datetime, max_datetime, data_block)


main(sys.argv[1:])
