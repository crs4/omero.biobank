"""
A basic tool to convert from Illumina CSV probe definition files to VL
marker definition import files.
It currently ignores CNV probes.
"""
#---------------------------------------------------------------
PROGRAM='format_illumina_probes'
import logging, time
LOG_FILENAME = '%s.log' % PROGRAM
LOG_LEVEL=logging.DEBUG

logging.basicConfig(filename=LOG_FILENAME,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    level=logging.ERROR)

#-----------------------------------------------
logger = logging.getLogger()
ch = logging.StreamHandler()
ch.setLevel(LOG_LEVEL)
formatter = logging.Formatter(fmt="%(asctime)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)

#-------------------------------------------------------------------------
import argparse
import sys

def make_parser():
  parser = argparse.ArgumentParser(description="""
  A basic tool to convert from Illumina CSV probe definition files to
  VL marker definition import files.  It currently ignores CNV probes.
  """)
  parser.add_argument('-i', '--ifile', type=argparse.FileType('r'),
                        help='the input Illumina csv file')
  parser.add_argument('-o', '--ofile', type=argparse.FileType('w'),
                        help='the output tsv filename',
                        default=sys.stdout)
  return parser

#-------------------------------------------------------------------------

import csv, re

from bl.vl.utils.snp import conjugate, convert_to_top, identify_strand

def consistency_check(r):
  alleles = r['SNP'][1:-1].split('/')
  Allele_A, Allele_B = alleles
  alleles = set(alleles)
  seq    = r['SourceSeq'].upper()
  topseq = r['TopGenomicSeq'].upper()
  lflank, center, rflank = re.split('[\[\]]', topseq)
  allele_a_probeseq = r['AlleleA_ProbeSeq'].upper()
  allele_b_probeseq = r['AlleleB_ProbeSeq'].upper()
  if istrand == 'TOP':
    if ('A' in  alleles  and ('G' in alleles
                              or 'C' in alleles)):
      alleles.remove('A')
      allele_A, allele_B = 'A', alleles.pop()
    elif 'A' in alleles:
      assert 'T' in alleles
      allele_A, allele_B = 'A', 'T'
    else:
      assert 'C' in alleles
      assert 'G' in alleles
      allele_A, allele_B = 'C', 'G'
  elif istrand == 'BOT':
    if ('T' in alleles  and ('G' in alleles
                             or 'C' in alleles)):
      alleles.remove('T')
      allele_A, allele_B = 'T', alleles.pop()
      crflank = conjugate(rflank)
    elif 'T' in alleles:
      assert 'A' in alleles
      allele_A, allele_B = 'T', 'A'
      crflank = conjugate(rflank)
      #assert (allele_a_probeseq == (crflank[-len(allele_a_probeseq) + 1:]+'T'))
      #assert (allele_b_probeseq == (crflank[-len(allele_b_probeseq) + 1:]+'A'))
    else:
      assert 'C' in alleles
      assert 'G' in alleles
      allele_A, allele_B = 'G', 'C'
      crflank = conjugate(rflank)
      #assert (allele_a_probeseq == (crflank[-len(allele_a_probeseq) + 1:]+'G'))
      #assert (allele_b_probeseq == (crflank[-len(allele_b_probeseq) + 1:]+'C'))
  else:
    assert(istrand)

def process_snp(r):
  """
  Convert illumina csv snp data description to VL marker
  defition input format.
  Return a dict with the following fields::

   label rs_label mask strand allele_a allele_b

  """
  #consistency_check(r)
  alleles = r['SNP'][1:-1].split('/')
  allele_a, allele_b = alleles
  x = {'label': r['IlmnID'],
       'rs_label' : r['Name'] if r['Name'].startswith('rs') else 'None',
       'mask' : r['TopGenomicSeq'].upper(),
       'strand': r['IlmnStrand'].upper(),
       'allele_a' : allele_a.upper(),
       'allele_b' : allele_b.upper()}
  return x

def skip_header(stream, args):
  for i, r in enumerate(stream):
    if r['IlmnID'] ==  'IlmnID':
      break

def process_stream(istream, ostream, args):
  for i, r in enumerate(istream):
    if r['CNV_Probe'] != '0':
      continue
    y = process_snp(r)
    try:
      tops = convert_to_top(y['mask'])
      ctops = conjugate(tops)
    except KeyError, e:
      logger.error('Cannot process record %s' % y['label'])
      continue
    ostream.writerow(y)

def main():
  parser = make_parser()
  args = parser.parse_args()
  if not args.ifile:
    parser.error('Missing IFILE')
  #---
  i_field_names = "IlmnID,Name,IlmnStrand,SNP,AddressA_ID,AlleleA_ProbeSeq,AddressB_ID,AlleleB_ProbeSeq,GenomeBuild,Chr,MapInfo,Ploidy,Species,Source,SourceVersion,SourceStrand,SourceSeq,TopGenomicSeq,BeadSetID,CNV_Probe,Intensity_Only,Exp_Clusters".split(',')
  #--
  o_field_names = "label rs_label mask strand allele_a allele_b".split()

  icsv = csv.DictReader(args.ifile, fieldnames=i_field_names, delimiter=',')
  otsv = csv.DictWriter(args.ofile, fieldnames=o_field_names, delimiter='\t')
  otsv.writeheader()
  #--
  skip_header(icsv, args)
  process_stream(icsv, otsv, args)

main()
