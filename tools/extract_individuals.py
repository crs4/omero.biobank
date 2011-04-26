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
ch.setLevel(logging.INFO)

formatter = logging.Formatter(fmt="%(levelname)s - %(message)s")

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
  if x['Submitted_Gender'] == '1':
    y['gender'] = 'male'
  elif x['Submitted_Gender'] == '2':
    y['gender'] = 'female'
  y['father'] = x['Father'] if not (x['Father'] == '0'
                                    or x['Father'] == 'x'
                                    or x['Father'] == ''
                                    ) else 'None'
  y['mother'] = x['Mother'] if not (x['Mother'] == '0'
                                    or x['Mother'] == 'x'
                                    or x['Mother'] == ''
                                    ) else 'None'
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
                        help='study label', default=study)
  parser.add_argument('-f', '--force-founders', action='store_true', default=False,
                      help="""if set, it will force parents that
                              are not listed in the imput file to None""")

  return parser

def load_individuals(args, stream):
  individuals = {}
  for x in stream:
    y = individual_conversion_rule(args.study, x)
    k = y['label']
    if individuals.has_key(k):
      if not (individuals[k]['father'] == y['father']
              and individuals[k]['mother'] == y['mother']):
        logger.error('Inconsistent parental info between records for %s.' % k)
        logger.error('Removing records (%s, %s) for %s.' % (individuals[k], y, k))
        del individuals[k]
        continue
      if not (individuals[k]['gender'] == y['gender']):
        logger.error('Inconsistent gender between records for %s.' % k)
        logger.error('Removing records (%s, %s) for %s.' % (individuals[k], y, k))
        del individuals[k]
        continue
      logger.warn('Ignoring duplicate %s <-> %s' % (y, individuals[k]))
      continue
    individuals[k] = y
  return individuals

def dump_out(args, individuals, ostream):
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
    ostream.writerow(y)

def check_consistency(args, individuals):
  def gender_check(k, label, expected):
    if label == 'None':
      return
    if not individuals.has_key(label):
      logger.warn('putative parent >%s< of %s is not in this set.' % (label, k))
    elif not (individuals[label]['gender'].upper() == expected):
      logger.critical('putative gender for %s is wrong: expected >%s< got >%s<. Bailing out!'
                      % (label, expected, individuals[label]['gender'].upper()))
      #sys.exit(1)
  #-
  for k in individuals.keys():
    father = individuals[k]['father']
    mother = individuals[k]['mother']
    gender_check(k, father, 'MALE')
    gender_check(k, mother, 'FEMALE')

def main():
  parser = make_parser()
  args = parser.parse_args()
  #-
  f = csv.DictReader(args.ifile, delimiter='\t')
  #-
  individuals = load_individuals(args, f)
  #-
  check_consistency(args, individuals)
  #-
  fieldnames = 'study label gender father mother'.split()
  o = csv.DictWriter(args.ofile, fieldnames, delimiter='\t')
  o.writeheader()
  dump_out(args, individuals, o)

main()
