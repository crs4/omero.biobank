""" ...

Read Illumina 'Final Report' text files and write snp calls for each
sample as an SSC (SampleSnpCall) file in the mimetypes.SSC_FILE
format. Also, write tsv input files for DataSample and DataObject
importers.

The program outputs the sample *label* in the 'source'
column: the mapping to the corresponding PlateWell VID must be done by
an external tool

## MSET_LABEL = "IMMUNO_BC_11419691_B"
## DEVICE_MAKER = "Illumina"
## DEVICE_MODEL = "GenomeStudio"
## DEVICE_TYPE = "SoftwareProgram"

## release = data_reader.header.get("GSGT Version", "UNKNOWN_RELEASE")
## label = "%s.%s.%s" % (DEVICE_MAKER, DEVICE_MODEL, release)
## device = self.get_device(label, DEVICE_MAKER, DEVICE_MODEL, release)
"""
import sys, os, argparse, csv, logging
from contextlib import nested

from bl.core.io.illumina import GenomeStudioFinalReportReader as DataReader
from bl.core.io.illumina import IllSNPReader
from bl.core.io import MessageStreamWriter
import bl.core.gt.messages.SnpCall as SnpCall
from bl.vl.kb import KBError, mimetypes, KnowledgeBase as KB
from bl.vl.app.importer.core import Core  # move this script to importer app?
from bl.vl.utils import compute_sha1


LOG_FORMAT = '%(asctime)s|%(levelname)-8s|%(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'
LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

DS_FN = "import_data_sample.tsv"
DO_FN = "import_data_object.tsv"

DATA_SAMPLE_TYPE = "GenotypeDataSample"
PAYLOAD_MSG_TYPE = 'core.gt.messages.SampleSnpCall'

WEIGHTS = {
  'AA': (1., 0., 0.),
  'AB': (0., 1., 0.),
  'BB': (0., 0., 1.),
  '--': (1/3., 1/3., 1/3.),
  }
DS_

PAD_SIZE = 12


class Writer(csv.DictWriter):
  def __init__(self, f, fieldnames):
    csv.DictWriter.__init__(self, f, fieldnames, delimiter="\t",
                            lineterminator="\n")


class DataSampleWriter(Writer):
  def __init__(self, f):
    Writer.__init__(self, f, ["label", "source", "device", "device_type",
                              "data_sample_type", "status"])


class DataObjectWriter(Writer):
  def __init__(self, f):
    Writer.__init__(self, f, ["path", "data_sample_label", "mimetype", "size",
                              "sha1"])


def get_snp_name_to_label(ill_annot_file):
  with open(ill_annot_file) as f:
    reader = IllSNPReader(f)
    return dict((r["Name"], r["IlmnID"]) for r in reader)


def adjust_immuno_sample_id(old_sample_id, plate_barcode, pad_size=PAD_SIZE):
  """
  This is a CRS4-specific hack to fix ambiguous immunochip labels.
  """
  padded_id = old_sample_id.rjust(pad_size, '0')
  return "%s|%s" % (padded_id, plate_barcode)


def critical(logger, msg):
  logger.critical(msg)
  raise KBError(msg)


def get_plate_barcode(kb, fn):
  plate_fields = os.path.splitext(os.path.basename(fn))[0].split("_")[1:-1]
  plate_label = "_".join(plate_fields+["imm"])
  plate = kb.get_container(plate_label)
  if plate is None:
    critical("no container in kb is labeled as %s" % plate_label)
  return plate.barcode


def write_block(self, data_block, plate_barcode, out_dir, snp_name_to_label):
  sample_id = adjust_immuno_sample_id(data_block.sample_id, plate_barcode)
  out_fn = os.path.join(out_dir, '%s.ssc' % sample_id)
  header = {'sample_id': sample_id}
  stream = MessageStreamWriter(out_fn, PAYLOAD_MSG_TYPE, header)
  for k in data_block.snp_names():
    snp = data_block.snp[k]
    snp_id = name_to_label[snp['SNP Name']]
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
      'w_AA': w_aa,
      'w_AB': w_ab,
      'w_BB': w_bb,
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
  parser.add_argument('--out-dir', type=str, help='ssc output directory',
                      default="ssc")
  parser.add_argument('-H', '--host', type=str, help='omero hostname',
                      default='localhost')
  parser.add_argument('-U', '--user', type=str, help='omero user',
                      default='root')
  parser.add_argument('-P', '--passwd', type=str, help='omero password',
                      required=True)
  parser.add_argument('--device-vid', type=str, help='device vid',
                      required=True)
  parser.add_argument('--ds-fn', metavar='DS_FILE', default=DS_FN,
                      help='output path for import data sample tsv file')
  parser.add_argument('--do-fn', metavar='DO_FILE', default=DO_FN,
                      help='output path for import data object tsv file')
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

  kb = KB(driver="omero")(args.host, args.user, args.passwd)
  try:
    device = kb.get_by_vid(kb.Device, args.device_vid)
  except ValueError, e:
    sys.exit(e)

  snp_name_to_label = get_snp_name_to_label(args.annot_file)

  with nested(open(args.ds_fn, "w"), open(args.do_fn, "w")) as (ds_f, do_f):
  ds_w = DataSampleWriter(ds_f)
  do_w = DataObjectWriter(do_f)
  for w in ds_w, do_w:
    w.writeheader()
  for fn in args.ifiles:
    logger.info("processing %s" % fn)
    plate_barcode = get_plate_barcode(kb, fn)
    with open(fn) as f:
      data_reader = DataReader(f)
      n_blocks = data_reader.header.get('Num Samples', '?')
      for i, block in enumerate(data_reader.get_sample_iterator()):
        logger.info("processing block %d/%s " % (i+1, n_blocks))
        sample_id, out_fn = write_block(block, plate_barcode, out_dir,
                                        snp_name_to_label)
        label = os.path.basename(out_fn)
        out_path = os.path.abspath(out_fn)
        logger.info("computing checksum for %s" % label)
        sha1 = compute_sha1(out_path)
        size = str(os.stat(out_path).st_size)
        ds_w.writerow({
          "label": label,
          "source": sample_id,
          "device": device.id,
          "device_type": device.__class__.__name__,
          "data_sample_type": DATA_SAMPLE_TYPE,
          "status": "USABLE"
          })
        do_w.writerow([path, label, mimetypes.SSC_FILE, size, sha1])
        for outf in ds_f, do_f:
          outf.flush(); os.fsync(outf.fileno())


if __name__ == "__main__":
  main(sys.argv[1:])
