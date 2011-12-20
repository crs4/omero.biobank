""" ...
Given a list of SDS 2.3 AD (text) files, all related to the same set
of samples but for different snp markers, we will show how to
do the following:

 #. find the list of the snp tags (by actually looking into the files)

 #. look up on the ABI site their definition

 #. check if they are already present in Omero/VL

 #. if any of them are not in Omero/VL:

    #. write out a marker definition import file for the missing markers

    #. exit

 #. else:

    #. write out the relevant marker set import file

    #. for each sample write out all the relevant snp calls as a SSC
       (SampleSnpCall) file in the mimetypes.SSC_FILE format.

Usage:

.. code-block:: bash

   python import_taqman_results.py --device-label pula01 \
                                --prefix foobar -P romeo \
                                --ifile data/file_list.lst --run-id foobar

"""
import sys, os, argparse, logging, csv, urllib2
from datetime import datetime

from BeautifulSoup import BeautifulSoup

from bl.core.io.abi import SDSReader
from bl.core.io import MessageStreamWriter
from bl.core.seq.utils import reverse_complement as rc
import bl.core.gt.messages.SnpCall as SnpCall

from bl.vl.kb import KnowledgeBase as KB
from bl.vl.utils import compute_sha1
from bl.vl.utils.snp import split_mask, approx_equal_masks, convert_to_top


ABI_SOURCE = 'ABI'
ABI_CONTEXT = 'TaqMan-SNP_Genotyping_Assays'
ABI_RELEASE = '12/12/2009' # arbitrary date

logger = None

""" ..

The following is a simple utility class to look-up markers definition
in the Applied Biosystems markers database.

"""

class ABISnpService(object):
  SERVER='https://products.appliedbiosystems.com/'
  DOC_BASE='ab/en/US/adirect/'
  QUERY_FORM= (SERVER + DOC_BASE +
               'ab?cmd=ABAssayDetailDisplay&assayID=%s&Fs=y')

  def __init__(self):
    pass

  def get_marker_definition(self, abi_id):
    url = self.QUERY_FORM % abi_id
    f = urllib2.urlopen(url)
    soup = BeautifulSoup(f.read())
    tds = soup.findAll('td')
    mark_def = {}
    for td in tds:
      if td.string == 'Assay ID':
        mark_def['label'] = str(td.findNextSibling('td').string)
      if td.string == 'dbSNP ID':
        mark_def['rs_label'] = str(td.findNext('a').string)
      if td.string == 'Context Sequence ([VIC/FAM])':
        mark_def['mask'] = str(td.findNext('td').string)
    return mark_def


def get_markers_definition(found_markers, sds, abi_service):
  for m,v in sds.header['markers_info'].iteritems():
    if m in found_markers:
      logger.critical('the same marker (%s) is appearing twice' % m)
      #sys.exit(1)
      continue
    v['abi_definition'] = abi_service.get_marker_definition(abi_id=m)
    found_markers[m] = v


def canonize_call(mask, abi_call):
  """
  Canonize call against top mask. Directly uses the base
  called by TaqMan to compute the relevant allele code.
  """
  if abi_call.upper() == 'BOTH':
    return SnpCall.AB
  if abi_call.upper() == 'UNDETERMINED':
    return SnpCall.NOCALL
  _, call_base = abi_call.split('-')

  _, alleles, _ = split_mask(mask)
  if call_base in [alleles[0], rc(alleles[0])]:
    return SnpCall.AA
  elif call_base in [alleles[1], rc(alleles[1])]:
    return SnpCall.BB
  else:
    raise ValueError('Cannot map %s (alleles: %s)' % (abi_call, alleles))


def add_kb_marker_objects(kb, found_markers):
  missing_kb_markers = []
  by_label = dict(((m.label, m) for m in
                   kb.get_snp_markers_by_source(source=ABI_SOURCE,
                                                context=ABI_CONTEXT,
                                                release=ABI_RELEASE)))
  for m, v in found_markers.iteritems():
    if v['abi_definition']:
      label = v['abi_definition']['label']
      if label not in by_label:
        v['kb_marker'] = None
        missing_kb_markers.append(m)
      else:
        v['kb_marker'] = by_label[label]
  return missing_kb_markers


def write_import_markers_file(fname, found_markers, missing_kb_markers):
  "FIXME: The generated markers list is not sorted on the alignments"
  fo = csv.DictWriter(open(fname, mode='w'),
                      fieldnames=['source', 'context', 'release',
                                  'label', 'mask'],
                      delimiter='\t')
  fo.writeheader()
  for m in missing_kb_markers:
    marker = found_markers[m]
    r = {
      'source' : ABI_SOURCE,
      'context' : ABI_CONTEXT,
      'release' : ABI_RELEASE,
      'label' : marker['abi_definition']['label'],
      'mask' : marker['abi_definition']['mask'],
      }
    fo.writerow(r)


def write_markers_set_def_file(fname, found_markers):
  fo = csv.DictWriter(open(fname, mode='w'),
                      fieldnames=['marker_vid', 'marker_indx', 'allele_flip'],
                      delimiter='\t')
  fo.writeheader()
  for i, m in enumerate(found_markers):
    marker = found_markers[m]
    flip = False # Since TaqMan assays directly report the actual base called
    r = {
      'marker_vid' : marker['kb_marker'].id,
      'marker_indx' : i,
      'allele_flip' : flip,
      }
    fo.writerow(r)


def write_ssc_data_samples_import_file(fname, ssc_data_set):
  fo = csv.DictWriter(open(fname, mode='w'),
                      fieldnames=['label', 'source', 'device', 'device_type',
                                  'scanner'],
                      delimiter='\t')
  fo.writeheader()
  for label, sample_id, device_id, fname in ssc_data_set:
    fo.writerow({'label' : label,
                 'source' : sample_id,
                 'device' : device_id,
                 'device_type' : 'Scanner',
                 'scanner': device_id,
                 })


def write_ssc_data_objects_import_file(fname, ssc_data_set):
  fo = csv.DictWriter(open(fname, mode='w'),
                      fieldnames=['path', 'data_sample_label', 'mimetype',
                                  'size', 'sha1'],
                      delimiter='\t')
  fo.writeheader()
  for label, sample_id, device_id, fname in ssc_data_set:
    size = os.stat(fname).st_size
    sha1 = compute_sha1(fname)
    fo.writerow({
      'path' : 'file://' + os.path.realpath(fname),
      'data_sample_label' : label,
      'mimetype' : mimetypes.SSC_FILE,
      'size' : size,
      'sha1' : sha1,
      })


def write_ssc_data_set_file(fname, found_markers,
                            device_id, sample_id,
                            min_datetime, max_datetime, data):
  payload_msg_type = 'core.gt.messages.SampleSnpCall'
  header = {'device_id' : device_id,
            'sample_id' : sample_id,
            'min_datetime' : '%s' % min_datetime,
            'max_datetime' : '%s' % max_datetime}
  stream = MessageStreamWriter(fname, payload_msg_type, header)
  for d in data:
    found_marker = found_markers[d['Marker Name']]
    m = found_marker['kb_marker']
    stream.write({
      'sample_id' : sample_id,
      'snp_id' : m.id,
      'call' : canonize_call(m.mask, d['Call']),
      'confidence' : d['Quality Value'],
      'sig_A' : d['Allele X Rn']*d['Passive Ref'],
      'sig_B' : d['Allele X Rn']*d['Passive Ref'],
      'w_AA' : d['Allele X Rn'],
      'w_AB' : 0.0,
      'w_BB' : d['Allele X Rn']})
  stream.close()


def make_parser():
  parser = argparse.ArgumentParser(description="Define Taq Markers in Omero/VL")
  parser.add_argument('--logfile', type=str,
                      help='logfile. Will write to stderr if not specified')
  parser.add_argument('--loglevel', type=str,
                      choices=['DEBUG', 'INFO', 'WARNING', 'CRITICAL'],
                      help='logging level', default='INFO')
  parser.add_argument('--ifile', type=argparse.FileType('r'),
                      help='file with the list of files',
                      default=sys.stdin)
  parser.add_argument('--run-id', type=str,
                      help='an unique identifier for this run',
                      required=True)
  parser.add_argument('--device-label', type=str,
                      help='device label')
  parser.add_argument('--prefix', type=str,
                      help='output files prefix',
                      default='vl-taq-markers-')
  parser.add_argument('-H', '--host', type=str,
                      help='omero host system',
                      default='localhost')
  parser.add_argument('-U', '--user', type=str,
                      help='omero user',
                      default='test')
  parser.add_argument('-P', '--passwd', type=str,
                      help='omero user passwd',
                      required=True)
  return parser


def main(argv):
  global logger
  parser = make_parser()
  args = parser.parse_args(argv)
  logformat = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  loglevel  = getattr(logging, args.loglevel)
  if args.logfile:
    logging.basicConfig(filename=args.logfile, format=logformat, level=loglevel)
  else:
    logging.basicConfig(format=logformat, level=loglevel)
  logger = logging.getLogger()

  abi_service = ABISnpService()
  found_markers = {}
  data = {}
  min_datetime = datetime.max #datetime(2999, 12, 31, 23, 59, 59)
  max_datetime = datetime.min #datetime(1000, 1,  1,  0, 0, 1)
  for f in (_.strip() for _ in args.ifile if _.strip()):
    logger.info('processing %s' % f)

    sds = SDSReader(open(f), swap_sample_well_columns=True)
    min_datetime = min(min_datetime, sds.datetime)
    max_datetime = max(max_datetime, sds.datetime)

    get_markers_definition(found_markers, sds, abi_service)

    for r in sds:
      data.setdefault(r['Sample Name'], []).append(r)

  kb = KB(driver='omero')(args.host, args.user, args.passwd)
  logger.info('qui - main')
  missing_kb_markers = add_kb_marker_objects(kb, found_markers)

  if missing_kb_markers:
    logger.info('there are missing markers. Cannot proceed further.')
    fname = '%smarker-defs.tsv' % args.prefix
    write_import_markers_file(fname, found_markers, missing_kb_markers)
    logger.info('the list of the missing marker is in %s.' % fname)
    sys.exit(0)

  fname = '%smarkers-set-def.tsv' % args.prefix
  write_markers_set_def_file(fname, found_markers)

  ssc_data_set = {}
  device = kb.get_device(args.device_label)
  for sample_id, d in data.iteritems():
    fname = '%s%s-%s.ssc' % (args.prefix, device.id, sample_id)
    write_ssc_data_set_file(fname, found_markers,
                            device.id, sample_id,
                            min_datetime, max_datetime, d)
    ssc_data_set[sample_id] = ('taqman-%s-%s' % (args.run_id, sample_id),
                               sample_id, device.id, fname)
  fname = '%simport.ssc' % args.prefix
  write_ssc_data_samples_import_file(fname, ssc_data_set.values())
  write_ssc_data_objects_import_file(fname, ssc_data_set.values())


if __name__ == "__main__":
  main(sys.argv[1:])
