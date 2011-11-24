""" ...

Read Illumina 'Final Report' text files and write snp calls for each
well as an SSC (SampleSnpCall) file in the x-ssc-messages
format. Also, write tsv input files for DataSample and DataObject
importers.

**NOTE:** the program outputs the PlateWell *label* in the 'source'
column: the mapping to the corresponding VID must be done by an
external tool

"""
import sys, os, argparse, hashlib, csv, logging
from datetime import datetime
from contextlib import nested

from bl.core.io.illumina import GenomeStudioFinalReportReader as DataReader
from bl.core.io.illumina import IllSNPReader
from bl.core.io import MessageStreamWriter
import bl.core.gt.messages.SnpCall as SnpCall
from bl.vl.kb import KBError
from bl.vl.app.importer.core import Core  # move this script to importer app?


LOG_FORMAT = '%(asctime)s|%(levelname)-8s|%(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'
LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

# FIXME should (some of) these be available as parameters?
MSET_LABEL = "IMMUNO_BC_11419691_B"
DEVICE_MAKER = "Illumina"
DEVICE_MODEL = "GenomeStudio"
DEVICE_TYPE = "SoftwareProgram"
DATA_SAMPLE_TYPE = "GenotypeDataSample"
DS_FN = "import_data_sample.tsv"
DO_FN = "import_data_object.tsv"

PAYLOAD_MSG_TYPE = 'core.gt.messages.SampleSnpCall'

PAD_SIZE = 12


# copied from import_taqman_results.py
def compute_sha1(fname):
  BUFSIZE = 10000000
  sha1 = hashlib.sha1()
  with open(fname) as fi:
    s = fi.read(BUFSIZE)
    while s:
      sha1.update(s)
      s = fi.read(BUFSIZE)
  return sha1.hexdigest()


def adjust_immuno_sample_id(old_sample_id, plate_barcode, pad_size=PAD_SIZE):
  """
  This is a CRS4-specific hack to fix ambiguous immunochip labels.
  """
  padded_id = old_sample_id.rjust(pad_size, '0')
  return "%s|%s" % (padded_id, plate_barcode)


class Writer(Core):

  MIN_DATETIME = datetime.max
  MAX_DATETIME = datetime.min
  WEIGHTS = {
    'AA': (1., 0., 0.),
    'AB': (0., 1., 0.),
    'BB': (0., 0., 1.),
    '--': (.5, 0., .5),
    }
  DS_HEADER = ["label", "source", "device", "device_type", "data_sample_type",
               "markers_set"]
  DO_HEADER = ["path", "data_sample_label", "mimetype", "size", "sha1"]
  
  def critical(self, msg):
    self.logger.critical(msg)
    raise KBError(msg)
  
  def get_marker_ids(self, mset_label):
    ms = self.kb.get_snp_markers_set(label=mset_label)
    if ms is None:
      self.critical("unable to retrieve marker set %s" % mset_label)
    self.logger.info("loading markers for marker set %s" % mset_label)
    ms.load_markers()
    self.logger.info("%s has %d markers" % (mset_label, len(ms.markers)))
    self.markers_set_id = ms.id
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
    plate_fields = os.path.splitext(os.path.basename(fn))[0].split("_")[1:-1]
    plate_label = "_".join(plate_fields+["imm"])
    plate = self.kb.get_container(plate_label)
    if plate is None:
      self.critical("no container in kb is labeled as %s" % plate_label)
    return plate.barcode

  def write_output_files(self, in_fnames, prefix):
    with nested(open(DS_FN, "w"), open(DO_FN, "w")) as (ds_f, do_f):
      ds_w = csv.writer(ds_f, delimiter="\t", lineterminator=os.linesep)
      do_w = csv.writer(do_f, delimiter="\t", lineterminator=os.linesep)
      ds_w.writerow(self.DS_HEADER)
      do_w.writerow(self.DO_HEADER)
      for fn in in_fnames:
        self.logger.info("processing %s" % fn)
        plate_barcode = self.get_plate_barcode(fn)
        with open(fn) as f:
          data_reader = DataReader(f)
          release = data_reader.header.get("GSGT Version", "UNKNOWN_RELEASE")
          label = "%s.%s.%s" % (DEVICE_MAKER, DEVICE_MODEL, release)
          device = self.get_device(label, DEVICE_MAKER, DEVICE_MODEL, release)
          n_blocks = data_reader.header.get('Num Samples', '?')
          for i, block in enumerate(data_reader.get_sample_iterator()):
            self.logger.info("processing block %d/%s " % (i+1, n_blocks))
            sample_id, fn = self.write(block, device.id, plate_barcode, prefix)
            label = os.path.basename(fn)
            self.logger.info("computing checksum for %s" % fn)
            path = os.path.abspath(fn)
            sha1 = compute_sha1(fn)
            size = str(os.stat(fn).st_size)
            ds_w.writerow([label, sample_id, device.id, DEVICE_TYPE,
                           DATA_SAMPLE_TYPE, self.markers_set_id])
            do_w.writerow([path, sample_id, 'x-ssc-messages', size, sha1])

  def write(self, data_block, device_id, plate_barcode, prefix):
    sample_id = adjust_immuno_sample_id(data_block.sample_id, plate_barcode)
    header = {'device_id': device_id,
              'sample_id': sample_id,
              'min_datetime': str(self.MIN_DATETIME),
              'max_datetime': str(self.MAX_DATETIME)}
    out_fn = '%s%s-%s.ssc' % (prefix, device_id, sample_id)
    stream = MessageStreamWriter(out_fn, PAYLOAD_MSG_TYPE, header)
    for k in data_block.snp_names():
      snp = data_block.snp[k]
      snp_id = self.marker_name_to_id[snp['SNP Name']]
      call = '%s%s' % (snp['Allele1 - AB'], snp['Allele2 - AB'])
      w_aa, w_ab, w_bb = self.WEIGHTS[call]
      # GC Score = 0.0 ==> call = '--'
      stream.write({
        'sample_id': sample_id,
        'snp_id': snp_id,
        'call': getattr(SnpCall, call, SnpCall.NOCALL),
        'confidence': float(snp['GC Score']),  # 1-gc_score? 1/gc_score?
        'sig_A': float(snp['X Raw']),
        'sig_B': float(snp['Y Raw']),
        'weight_AA': w_aa,
        'weight_AB': w_ab,
        'weight_BB': w_bb,
        })
    stream.close()
    return sample_id, out_fn


def make_parser():
  parser = argparse.ArgumentParser(description="write Immunochip data files")
  parser.add_argument('ifiles', metavar='DATA_FILE', type=str, nargs='+',
                      help='input FinalReport text files')
  parser.add_argument('-a', '--annot-file', metavar='ANN_FILE', required=True,
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
  writer = Writer(args.host, args.user, args.passwd, logger=logger)
  writer.get_marker_ids(MSET_LABEL)
  writer.get_marker_name_to_label(args.annot_file)
  writer.get_marker_name_to_id()
  writer.write_output_files(args.ifiles, args.prefix)


if __name__ == "__main__":
  main(sys.argv[1:])
