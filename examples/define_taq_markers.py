"""
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

    #. for each sample write out all the relevant calls as a SSC
       (SampleSnpCall) file in the x-protobuf-ssc format.

"""
import sys, argparse, logging
from datetime import datetime

from bl.core.io.abi import SDSReader
from bl.core.gt.io import SnpCallStream

from bl.vl.kb import KnowledgeBase as KB

import urllib2
from BeautifulSoup import BeautifulSoup

logger = None

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

#----------------------------------------------------------------------

def get_markers_definition(found_markers, sds):
  for m,v in sds.header['markers_info'].iteritems():
    if m in found_markers:
      logger.critical('the same marker (%s) is appearing twice' % m)
      #sys.exit(1)
      continue
    v['abi_definition'] = abi_service.get_marker_definition(abi_id=m)
    found_markers[m] = v

def add_kb_marker_objects(found_markers):
  missing_kb_markers = []
  for m, v in found_markers.iteritems():
    if v['abi_definition']:
      kb_mrk = kb.get_snp_markers(rs_labels=[v['abi_definition']['rs_label']])
      if kb_mrk is None:
        v['kb_marker'] = None
        missing_kb_markers.append(m)
      else:
        v['kb_marker'] = kb_mrk[0]
  return missing_kb_markers

def write_import_markers_file(fname, found_markers, missing_kb_markers):
  "FIXME: The generated markers list is not sorted on the alignments"
  fo = csv.DictWriter(open(fname, mode='w'),
                      fieldnames=['source', 'context', 'release',
                                  'label', 'rs_label', 'mask'],
                      delimiter='\t')
  fo.writeheader()
  for m in missing_kb_markers:
    marker = found_markers[m]
    r = {
      'source' : 'ABI',
      'context' : 'TaqMan',
      'release' : 'SNP Genotyping Assays',
      'label' : marker['abi_definition']['label'],
      'rs_label' : marker['abi_definition']['rs_label'],
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
    #FIXME:here we should be comparing marker['kb_marker'].mask with
    #     marker['abi_definition']['mask'] to check if there has been
    #     a flip in alleles enumeration.
    flip = False
    r = {
      'marker_vid' : marker['kb_marker'].id,
      'marker_indx' : i,
      'allele_flip' : flip,
      }
    fo.writerow(r)

def write_ssc_data_set_file(fname, found_markers, device_id, sample_id,
                            datetime_start, datetime_stop, data):
  pass

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
                      fieldnames=['path', 'data_sample', 'mimetype',
                                  'size', 'sha1'],
                      delimiter='\t')
  fo.writeheader()
  for label, sample_id, device_id, fname in ssc_data_set:
    fo.writerow({
      'path' : os.path.join('<FIXME>', fname),
      'data_sample_label' : label,
      'mimetype' : 'x-ssc-messages-flow',
      'size' : FIXME,
      'sha1' : 'FIXME',
      })

def write_ssc_data_set_file(fname, found_markers,
                            device_id, sample_id,
                            min_datetime, max_datetime, data):
  payload_msg_type = 'core.gt.messages.SampleSnpCall'
  header = {'device_id' : device_id,
            'sample_id' : sample_id,
            'min_datetime' : min_datetime,
            'max_datetime' : max_datetime}
  stream = MessageStreamWriter(fname, payload_msg_type, header)
  for d in data:
    m = markers[d['Marker Name']]
    stream.write({
      'sample_id' : sample_id,
      'snp_id' : marker['kb_marker'].id,
      'call' : canonize_call(m, d['Call']),
      'confidence' : d['Quality Value'],
      'sig_A' : d['Allele X Rn']*d['Passive Ref'],
      'sig_B' : d['Allele X Rn']*d['Passive Ref'],
      'weight_AA' : d['Allele X Rn'],
      'weight_AB' : 0.0,
      'weight_BB' : d['Allele X Rn']})
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
                      default='root')
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
  min_datetime = datetime(2999, 12, 31, 23, 59, 59)
  max_datetime = datetime(1000, 1,  1,  0, 0, 1)
  for f in (_ for _ in args.ifile if _.strip()):
    logger.info('processing %s' % f)

    sds = SDSReader(open(f), swap_sample_well_columns=True)
    min_datetime = min(min_datetime, sds.datetime)
    max_datetime = max(max_datetime, sds.datetime)

    get_marker_definition(found_markers, sds)

    for r in sds:
      data.setdefault(r['Sample Name'], []).append(r)

  kb = KB(driver='omero')(args.host, args.user, args.passwd)
  missing_kb_markers = add_kb_marker_objects(found_markers)
  if missing_kb_markers:
    logger.info('there are missing markers. Cannot proceed further.')
    fname = '%smarker-defs.tsv' % args.prefix
    write_import_markers_file(fname, found_markers, missing_kb_markers)
    sys.exit(0)

  fname = '%smarkers-set-def.tsv' % args.prefix
  write_markers_set_def_file(fname, found_markers)

  ssc_data_set = {}
  device = kb.get_device(args.device_label)
  for sample_id, d in data.iteritems():
    fname = '%s%s-%s.ssc' % (args.prefix, device.id)
    write_ssc_data_set_file(fname, found_markers,
                            device.id, sample_id,
                            min_datetime, max_datetime, d)
    ssc_data_set[s] = ('taqman-%s-%s' % (args.run_id, sample_id, min_datetime),
                       sample_id, device.id, fname)
  fname = '%simport.ssc' % args.prefix
  write_ssc_data_samples_import_file(fname, ssc_data_set)
  write_ssc_data_objects_import_file(fname, ssc_data_set)

main(sys.argv[1:])
