#!/usr/bin/env python
"""

Create Fake SampleSnpCall files
===============================

FIXME
"""

import logging

LOG_FILENAME='create_fake_gdo.log'
logging.basicConfig(
  format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
  level=logging.INFO,
  filename=LOG_FILENAME)

logger = logging.getLogger()

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

formatter = logging.Formatter(
  fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

ch.setFormatter(formatter)

logger.addHandler(ch)

------------------------------------------------------------------------------
import os, sys, csv
import argparse

def make_parser():
  desc="Create a collection of SampleSnpCall files."
  parser = argparse.ArgumentParser(
    description=desc,
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)

  parser.add_argument('-H', '--host', type=str,
                      help='omero host system',
                      default='localhost')
  parser.add_argument('-U', '--user', type=str,
                      help='omero user',
                      default='test')
  parser.add_argument('-P', '--passwd', type=str,
                      help='omero user passwd',
                      required=True)
  parser.add_argument('--data-samples', type=argparse.FileType('r'),
                      help='the list of the GenotypeDataSample files',
                      required=True)
  parser.add_argument('--outfile', type=argparse.FileType('w'),
                      help='a tsv with data objects files details',
                      required=True)
  return parser

from bl.vl.kb     import KnowledgeBase as KB
from bl.core.io import MessageStreamWriter
import bl.core.gt.messages.SnpCall as SnpCall
from datetime import datetime

def make_data_object(sample_id, fname, ms):
  payload_msg_type = 'core.gt.messages.SampleSnpCall'
  header = {'device_id' : 'FAKE',
            'sample_id' : sample_id,
            'min_datetime' : datetime.now().isoformat(),
            'max_datetime' : datetime.now().isoformat()}
  stream = MessageStreamWriter(fname, payload_msg_type, header)
  for m in ms.markers:
    call, sig_A, sig_B, weight_AA, weight_AB, weight_BB = random.choice(
      [(SnpCall.AA, 1.0, 0.0, 1.0, 0.0, 0.0),
       (SnpCall.AB, 1.0, 1.0, 0.0, 1.0, 0.0),
       (SnpCall.BB, 0.0, 1.0, 0.0, 0.0, 1.0)])
    stream.write({
      'sample_id' : sample_id,
      'snp_id' : m.id,
      'call' : call,
      'confidence' : 0.0,
      'sig_A' : sig_A,
      'sig_B' : sig_B,
      'weight_AA' : weight_AA,
      'weight_AB' : weight_AB,
      'weight_BB' : weight_BB})
  stream.close()

import hashlib

def compute_sha1(fname):
  BUFSIZE = 10000000
  sha1 = hashlib.sha1()
  with open(fname) as fi:
    s = fi.read(BUFSIZE)
    while s:
      sha1.update(s)
      s = fi.read(BUFSIZE)
  return sha1.hexdigest()

def main():
  parser = make_parser()
  args = parser.parse_args()
  kb = KB(driver='omero')(args.host, args.user, args.passwd)

  by_label = dict(((x.label, x) for x in kb.get_objects(kb.GenotypeDataSample)))
  msets = {}
  itsv = csv.DictReader(args.data_samples, delimiter='\t')
  otsv = csv.DictWriter(open(args., mode='w'),
                      fieldnames=['path', 'data_sample_label', 'mimetype',
                                  'size', 'sha1'],
                      delimiter='\t')
  otsv.writeheader()

  for r in itsv:
    ds_label = r['label']
    logger.info('Gathering info on %s' % ds_label)
    if ds_label not in by_label:
      logger.critical('There is no GenotypeDataSample with label %s' % label)
      sys.exit(1)
    ds = by_label[ds_label]
    # FIXME
    if ds.spnMarkersSet.omero_id not in msets:
      ms = ds.snpMarkersSet
      ms.load_markers()
      msets[ds.snpMarkersSet.omero_id] = ms
    ms = msets[ds.snpMarkersSet.omero_id]
    fname = ds_label + '_do.ssc'
    make_data_object(ds.id, fname, ms)
    size = os.stat(fname).st_size
    sha1 = compute_sha1(fname)
    otsv.writerow({
      'path' : 'file://' + os.path.realpath(fname),
      'data_sample' : ds.id,
      'mimetype' : 'x-ssc-messages',
      'size' : size,
      'sha1' : sha1,
      })

if __name__ == "__main__":
  main()


# Local Variables: **
# mode: python **
# End: **
