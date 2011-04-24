"""

Extracts records for individuals from a 'Ilena format' tsv file.

It will map parents to 'None' if their label is referring to an
unknown individual.

"""
#---------------------------------------------------------------
import logging, time
LOG_FILENAME = 'extract_individuals.log'
logging.basicConfig(filename=LOG_FILENAME,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    level=logging.DEBUG)

logger = logging.getLogger()

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

formatter = logging.Formatter()

ch.setFormatter(formatter)

logger.addHandler(ch)

#-------------------------------------------------------------------------

import argparse
import csv
import sys

study = 'TEST01'


def individual_conversion_rule(study, x):
  y= {'study' : study}
  y['label'] = x['ID']
  y['gender'] = 'male' if x['Submitted_Gender'] == '1' else 'female'
  y['father'] = x['Father'] if not x['Father'] == '0' else 'None'
  y['mother'] = x['Mother'] if not x['Mother'] == '0' else 'None'
  return y


def make_parser():
  parser = argparse.ArgumentParser(description="""
  An importer for Ilenia's style tsv files.
  """)
  parser.add_argument('-i', '--ifile', type=argparse.FileType('r'),
                        help='the input tsv file',
                        default=sys.stdin)
  parser.add_argument('-o', '--ofile', type=argparse.FileType('w'),
                        help='the output tsv file',
                        default=sys.stdout)
  parser.add_argument('-s', '--study', type=str,
                        help='study label')
  parser.add_argument('-f', '--force-founders', action='store_true', default=False,
                      help="""if set, it will force parents that
                              are not listed in the imput file to None""")

  return parser

def main():
  parser = make_parser()
  args = parser.parse_args()
  #-
  f = csv.DictReader(args.ifile, delimiter='\t')

  individuals = {}
  for x in f:
    y = individual_conversion_rule(args.study, x)
    k = y['label']
    if individuals.has_key(k):
      logger.warn('Ignoring duplicate %s <-> %s' % (y, individuals[k]))
      continue
    individuals[k] = y
  #-
  fieldnames = 'study label gender father mother'.split()
  o = csv.DictWriter(args.ofile, fieldnames, delimiter='\t')

  o.writeheader()
  for y in individuals.values():
    if args.force_founders:
      if not (y['father'] == 'None' or individuals.has_key(y['father'])):
        logger.warn('individual %s --> forcing father %s to none' % (y['label'],
                                                                     y['father']))
        y['father'] = 'None'

      if not (y['mother'] == 'None' or individuals.has_key(y['mother'])):
        logger.warn('individual %s --> forcing mother %s to none' % (y['label'],
                                                                     y['mother']))
        y['mother'] = 'None'
    o.writerow(y)

main()
