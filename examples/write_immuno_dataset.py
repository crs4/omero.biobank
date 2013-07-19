# BEGIN_COPYRIGHT
# END_COPYRIGHT

""" ...

Read Illumina 'Final Report' text files and write snp calls for each
sample as an SSC (SampleSnpCall) file in the mimetypes.SSC_FILE
format. Also, write tsv input files for DataSample and DataObject
importers.

Input Final Report files can be provided as separate arguments or,
alternatively, listed in a file specified via the --input-list
option. In the latter case, if additional (tab-separated) columns are
present, their values will be considered as the only sample IDs to be
processed.

The program outputs the sample *label* in the 'source' column: the
mapping to the corresponding PlateWell VID must be done by an external
tool. The same holds for the 'device' and the 'markers_set'
columns. Note that objects corresponding to these labels need *not*
exist at the time this program is run.

Here is an example of tsv input for the import device tool (multiple
spaces represent tabs)::

  device_type      label                        maker     model         release
  SoftwareProgram  Illumina-GenomeStudio-1.8.4  Illumina  GenomeStudio  1.8.4

"""
import sys, os, argparse, csv
from collections import OrderedDict
from contextlib import nested

from bl.core.io.illumina import GenomeStudioFinalReportReader as DataReader
from bl.core.io.illumina import IllSNPReader
from bl.core.io import MessageStreamWriter
import bl.core.gt.messages.SnpCall as SnpCall

from bl.vl.kb import mimetypes
from bl.vl.utils import compute_sha1, LOG_LEVELS, get_logger


DS_FN = "import_data_sample.tsv"
DO_FN = "import_data_object.tsv"

DEVICE_TYPE = "SoftwareProgram"
DATA_SAMPLE_TYPE = "GenotypeDataSample"
PAYLOAD_MSG_TYPE = 'core.gt.messages.SampleSnpCall'

WEIGHTS = {
  'AA': (1., 0., 0.),
  'AB': (0., 1., 0.),
  'BB': (0., 0., 1.),
  '--': (1/3., 1/3., 1/3.),
  }

PAD_SIZE = 12


class Everything(set):
  
  def __init__(self):
    super(Everything, self).__init__()
  
  def __contains__(self, item):
    return True


class Writer(csv.DictWriter):
  def __init__(self, f, fieldnames):
    csv.DictWriter.__init__(self, f, fieldnames, delimiter="\t",
                            lineterminator="\n")


class DataSampleWriter(Writer):
  def __init__(self, f):
    Writer.__init__(self, f, ["label", "source", "device", "device_type",
                              "data_sample_type", "markers_set", "status"])


class DataObjectWriter(Writer):
  def __init__(self, f):
    Writer.__init__(self, f, ["path", "data_sample_label", "mimetype", "size",
                              "sha1"])


def get_input_map(fn):
  input_map = OrderedDict()
  with open(fn) as f:
    reader = csv.reader(f, delimiter="\t")
    for row in reader:
      fr_path = row[0]
      requested_sample_ids = set(row[1:]) or Everything()
      input_map[fr_path] = requested_sample_ids
  return input_map


def get_snp_name_to_label(ill_annot_file):
  with open(ill_annot_file) as f:
    reader = IllSNPReader(f)
    return dict((r["Name"], r["IlmnID"]) for r in reader)


#--- this is a CRS4-specific hack to fix ambiguous immunochip labels -------
def adjust_immuno_sample_id(old_sample_id, plate_barcode, pad_size=PAD_SIZE):
  padded_id = old_sample_id.rjust(pad_size, '0')
  return "%s|%s" % (padded_id, plate_barcode)
#---------------------------------------------------------------------------


def write_block(data_block, plate_barcode, out_dir, snp_name_to_label, header,
                requested_sample_ids):
  sample_id = adjust_immuno_sample_id(data_block.sample_id, plate_barcode)
  if sample_id not in requested_sample_ids:
    return sample_id, None
  out_fn = os.path.join(out_dir, '%s.ssc' % sample_id)
  header['sample_id'] = sample_id
  stream = MessageStreamWriter(out_fn, PAYLOAD_MSG_TYPE, header)
  for k in data_block.snp_names():
    snp = data_block.snp[k]
    snp_id = snp_name_to_label[snp['SNP Name']]
    call = '%s%s' % (snp['Allele1 - AB'], snp['Allele2 - AB'])
    w_aa, w_ab, w_bb = WEIGHTS[call]
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
  parser.add_argument('ifiles', metavar='FILE', type=str, nargs='*',
                      help='input FinalReport text files')
  parser.add_argument('--input-list', metavar='FILE',
                      help='input FinalReport file list in tsv format')
  parser.add_argument('-a', '--annot-file', metavar='ANN_FILE', required=True,
                      help='Original Illumina SNP annotation file')
  parser.add_argument('--logfile', type=str, help='log file (default=stderr)')
  parser.add_argument('--loglevel', type=str, choices=LOG_LEVELS,
                      help='logging level', default='INFO')
  parser.add_argument('--out-dir', type=str, help='ssc output directory',
                      default="ssc")
  parser.add_argument('--device-label', type=str, help='device label',
                      required=True)
  parser.add_argument('--marker-set-label', type=str, help='marker set label',
                      required=True)
  parser.add_argument('--ds-fn', metavar='DS_FILE', default=DS_FN,
                      help='output path for import data sample tsv file')
  parser.add_argument('--do-fn', metavar='DO_FILE', default=DO_FN,
                      help='output path for import data object tsv file')
  return parser


def main(argv):
  parser = make_parser()
  args = parser.parse_args(argv)

  # can't use argparse's mutually exclusive group, one arg is not optional
  if args.input_list and args.ifiles:
    sys.exit("ERROR: no positional arg accepted if --input-list is specified")
  if not args.input_list and not args.ifiles:
    sys.exit("ERROR: no input source has been specified")

  logger = get_logger("main", level=args.loglevel, filename=args.logfile)

  if args.input_list:
    input_map = get_input_map(args.input_list)
  else:
    input_map = OrderedDict((fn, Everything()) for fn in args.ifiles)
  logger.info("total input files: %d" % len(input_map))
  
  snp_name_to_label = get_snp_name_to_label(args.annot_file)
  logger.info("total SNPs: %d" % len(snp_name_to_label))

  with nested(open(args.ds_fn, 'w'), open(args.do_fn, 'w')) as (ds_f, do_f):
    ds_w = DataSampleWriter(ds_f)
    do_w = DataObjectWriter(do_f)
    for w in ds_w, do_w:
      w.writeheader()
    for k, (fn, requested_sample_ids) in enumerate(input_map.iteritems()):
      logger.info("processing %s (%d/%d)" % (fn, k+1, len(input_map)))
      plate_barcode = os.path.splitext(os.path.basename(fn))[0].split("_")[1]
      with open(fn) as f:
        data_reader = DataReader(f)
        header = data_reader.header
        header['markers_set'] = args.marker_set_label
        n_blocks = header.get('Num Samples', '?')
        for i, block in enumerate(data_reader.get_sample_iterator()):
          logger.info("processing block %d/%s " % (i+1, n_blocks))
          sample_id, out_fn = write_block(
            block, plate_barcode, args.out_dir,
            snp_name_to_label, header, requested_sample_ids
            )
          if out_fn is None:
            logger.info("skipped sample %r (not requested)" % sample_id)
            continue
          label = os.path.basename(out_fn)
          out_path = os.path.abspath(out_fn)
          ds_w.writerow({
            "label": label,
            "source": sample_id,
            "device": args.device_label,
            "device_type": DEVICE_TYPE,
            "data_sample_type": DATA_SAMPLE_TYPE,
            "markers_set": args.marker_set_label,
            "status": "USABLE",
            })
          do_w.writerow({
            "path": out_path,
            "data_sample_label": label,
            "mimetype": mimetypes.SSC_FILE,
            "size": str(os.stat(out_path).st_size),
            "sha1": compute_sha1(out_path),
            })
          for outf in ds_f, do_f:
            outf.flush(); os.fsync(outf.fileno())


if __name__ == "__main__":
  main(sys.argv[1:])
