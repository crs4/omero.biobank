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
  A basic tool to convert from Affymetrix Annot tsv to
  VL marker definition import files.  It currently ignores CNV probes.
  """)
  parser.add_argument('-i', '--ifile', type=argparse.FileType('r'),
                        help='the input Affymetrix tsv file')
  parser.add_argument('-o', '--ofile', type=argparse.FileType('w'),
                        help='the output tsv filename',
                        default=sys.stdout)
  return parser

#-------------------------------------------------------------------------

import csv, re

from bl.vl.utils.snp import conjugate, convert_to_top, identify_strand

def process_snp(r):
  """
  Convert Affymetrix csv snp data description to VL marker
  defition input format.
  Return a dict with the following fields::

   label rs_label mask strand allele_a allele_b

  """
  #consistency_check(r)
  allele_a, allele_b = r['Allele A'], r['Allele B']
  mask = r['Flank']

  x = {'label': r['Probe Set ID'],
       'rs_label' : r['dbSNP RS ID'],
       'mask' : r['Flank'],
       'strand': 'None', # this is because affy
       'allele_a' : allele_a,
       'allele_b' : allele_b}
  return x

def process_stream(istream, ostream, args):
  for i, r in enumerate(istream):
    y = process_snp(r)
    ostream.writerow(y)

def main():
  parser = make_parser()
  args = parser.parse_args()
  if not args.ifile:
    parser.error('Missing IFILE')
  #---
  i_field_names = "Probe Set ID,Affy SNP ID,dbSNP RS ID,Chromosome,Physical Position,Strand,ChrX pseudo-autosomal region 1,Cytoband,Flank,Allele A,Allele B,Associated Gene,Genetic Map,Microsatellite,Fragment Enzyme Type Length Start Stop,Allele Frequencies,Heterozygous Allele Frequencies,Number of individuals/Number of chromosomes,In Hapmap,Strand Versus dbSNP,Copy Number Variation,Probe Count,ChrX pseudo-autosomal region 2,In Final List,Minor Allele,Minor Allele Frequency,% GC".split(',')

  #--
  o_field_names = "label rs_label mask strand allele_a allele_b".split()

  # clean up initial comments
  for l in args.ifile:
    if not l.startswith('#'):
      break
  assert l.startswith('Probe Set ID')
  #--
  icsv = csv.DictReader(args.ifile, fieldnames=i_field_names, delimiter='\t')
  otsv = csv.DictWriter(args.ofile, fieldnames=o_field_names, delimiter='\t')
  otsv.writeheader()
  #--
  process_stream(icsv, otsv, args)

main()
