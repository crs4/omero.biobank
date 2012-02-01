# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""

Extracts diagnosis records from a 'Ilena pheno format' tsv file.

from one row, it will generate as many as needed records with the
following structure::

   study  individual_label diagnosis_term
   ASTUDY 899              terminology://apps.who.int/classifications/apps/icd/icd10online/gE10.htm#E10

"""
#---------------------------------------------------------------
import logging, time
LOG_FILENAME = 'extract_diagnosis.log'
logging.basicConfig(filename=LOG_FILENAME,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    level=logging.DEBUG)

logger = logging.getLogger()

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter(fmt="%(levelname)s - %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)

#-------------------------------------------------------------------------

import argparse
import csv
import sys

def dv_coded_text(terminology, term):
  return ':'.join([terminology, term])

term = {'TD' : dv_coded_text('icd10-cm', 'E10'), # Type 1 Diabetes
        'MS' : dv_coded_text('icd10-cm', 'G35'), # Multiple Sclerosis
        }

def diagnosis_conversion_rule(study, x):
  dds = []
  is_characterized = False
  is_affected = False
  #--
  timestamp = long(1000 * time.time())
  for k in ['TD', 'MS']:
    if x[k] == '2':
      is_affected = True
      dds.append({'study' : study,
                  'individual_label' : '%s_%s' % (x['Fam'], x['ID']),
                  'timestamp' : timestamp,
                  'diagnosis' : term[k]})
    elif x[k] == '1':
      is_characterized = True
  #--
  if is_characterized and not is_affected:
    dds.append({'study' : study,
                'individual_label' : '%s_%s' % (x['Fam'], x['ID']),
                'timestamp' : timestamp,
                'diagnosis' : 'exclusion-problem_diagnosis'})
  return dds

def make_parser():
  parser = argparse.ArgumentParser(description="""
  An importer for Ilenia's pheno style tsv files.
  """)
  parser.add_argument('-i', '--ifile', type=argparse.FileType('r'),
                        help='the input tsv file',
                        default=sys.stdin)
  parser.add_argument('-r', '--ofile-root', type=str,
                        help='the output tsv filename root',
                        default='extracted-')
  parser.add_argument('-s', '--study', type=str,
                        help='study label')
  parser.add_argument('--record-delimiter', type=str,
                        help='csv record delimiter', default=' ')
  return parser

def is_worth_analyzing(x):
  has_data = False
  for k in x.keys():
    if k in ['TD', 'MS', 'MD', 'AD']:
      has_data = True
  return has_data

def normalizer(args, x):
  if x.has_key(None):
    del x[None]
  for k in x.keys():
    x[k] = 'x' if x[k] == '-' else x[k]
  return x

def dump_(fname, objs, fieldnames=None):
  if not objs:
    return
  if not fieldnames:
    fieldnames = ['study', 'label'] + [k for k in objs[0].keys() if not k  in ['study', 'label']]
  with open(fname, 'w') as o:
    tsv = csv.DictWriter(o, fieldnames, delimiter='\t')
    tsv.writeheader()
    for x in objs:
      tsv.writerow(x)

def main():
  parser = make_parser()
  args = parser.parse_args()
  #-
  f = csv.DictReader(args.ifile, delimiter=args.record_delimiter)
  diagnosis_records = []
  for x in f:
    x = normalizer(args, x)
    if not is_worth_analyzing(x):
      continue
    dss = diagnosis_conversion_rule(args.study, x)
    diagnosis_records.extend(dss)
  #-
  dump_(args.ofile_root + 'diagnosis.tsv', diagnosis_records,
        fieldnames='study individual_label timestamp diagnosis'.split())
  #--


main()
