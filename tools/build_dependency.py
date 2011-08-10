import argparse
import sys
import csv
import os
import logging

logger = logging.getLogger()
logformat = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
loglevel  = logging.DEBUG
logging.basicConfig(format=logformat, level=loglevel)

def make_parser():
  parser = argparse.ArgumentParser(description="""
  Extract dependency tree from an Ilenia's style tsv files.
  """)
  parser.add_argument('-i', '--ifile', type=argparse.FileType('r'),
                        help='the input tsv file',
                        default=sys.stdin)
  parser.add_argument('-r', '--ofile-root', type=str,
                        help='the output tsv filename root',
                        default='extracted-')
  parser.add_argument('-s', '--study', type=str,
                        help='study label')
  parser.add_argument('-p', '--patch-missing-plate-info', action='store_true',
                      default=False,
                      help="""if set, it will add fictitious
                              TiterPlates and wells when needed to
                              mantain the dependency chain""")
  return parser

#-----------------------------------------------------------------


def normalizer(args, x):
  def rename_field(x, mapping):
    for f,t in mapping:
      if f in x:
        x[t] = x[f]
        del x[f]
      else:
        x[t] = None

  if x.has_key(None):
    del x[None]
  for k in x.keys():
    x[k] = 'x' if x[k] == '-' else x[k]
    x[k] = 'x' if x[k] == 'TBF' else x[k]
    if x[k] in 'xX':   x[k] = None

    x['Gender'] = {'1': 'MALE', '2': 'FEMALE'}[x['Submitted_Gender']]


  rename_field(x, [
    ('Affymetrix_Plate_Lanusei', 'Affy_Plate_Lanusei'),
    ('Affymetrix_Plate_Pula',    'Affy_Plate_Pula'),
    ('Affymetrix_Plate_USA',     'Affy_Plate_USA'),
    ('Illumina_1M',              'Illumina_Base'),
    ('Illumina_1M_duplicate',    'Illumina_Duplicate'),
    ('Illumina_Plate',           'Illumina_Plate_Base'),
    ('Illumina_dup_Plate',       'Illumina_Plate_Duplicate'),
    ])

  if x['Affy_USA']:
    x['Affy_USA'] = x['Affy_USA'] + '.CEL' if not x['Affy_USA'].upper().endswith('.CEL') else ''


  return x


def map_to_sample(records):
  by_sample = {}

  for r in records:
    if 'Sample_Name' in r and r['Sample_Name']:
      by_sample.setdefault(r['Sample_Name'], []).append(r)
    else:
      by_sample.setdefault('NO-SAMPLE', []).append(r)
  return by_sample

class Individual(object):
  def __init__(self, i, gender, father, mother):
    self.id = i
    self.gender = gender
    self.father = father
    self.mother = mother

  def is_orphan(self):
    return not (self.father or self.mother)

  @property
  def label(self):
    return '%s_%s' % self.id


def map_to_pedigree(records):
  known_individuals = {}
  known_individuals_by_id = {}
  bad_id = []

  for r in records:
    k = (r['Fam_ID'], r['ID'])
    indy = Individual(k, r['Gender'],
                      None if r['Father'] == '0' else r['Father'],
                      None if r['Mother'] == '0' else r['Mother'])
    known_individuals[k] = indy
    if k[1] in known_individuals_by_id:
      logger.critical('multiple individuals with the same ID -> %s'
                      % [known_individuals_by_id[k[1]].id, k])
      logger.critical('disabling by_id on this one')
      bad_id.append(k[1])
      #sys.exit(1)
    else:
      known_individuals_by_id[k[1]] = indy
  for i in bad_id:
    del known_individuals_by_id[i]

  for k, v in known_individuals.iteritems():
    for pname in ['father', 'mother']:
      parent = getattr(v, pname)
      if parent:
        kp = (k[0], parent)
        if kp in known_individuals:
          setattr(v, pname, known_individuals[kp])
        else:
          logger.warn('%s %s of %s is not a known individual' %
                     (pname, kp, k))
          if parent in known_individuals_by_id:
            setattr(v, pname, known_individuals_by_id[parent])
            logger.warn('recovered by id -> %s' % (getattr(v, pname).id,))
          else:
            logger.error('cannot patch %s' % parent)
  return known_individuals


def fix_pedigree(ped, s_to_indy):
  def collapse(ped, synonyms):
    if len(synonyms) <= 1:
      return
    choosen = ped[synonyms[0]]
    for i in synonyms[1:]:
      for v in ped.values():
        if v.father and v.father.id == i:
          v.father = choosen
        if v.mother and v.mother.id == i:
          v.mother = choosen
      del ped[i]
  logger.info('started fixing pedigree')
  logger.info('patching orphans')
  for k, v in s_to_indy.iteritems():
    if k == 'NO-SAMPLE':
      continue
    if len(v) > 1:
      logger.warn('Multiple individuals for %s' % k)
      not_orphans = [i for i in v if not ped[i].is_orphan()]
      if len(not_orphans) <= 1:
        collapse(ped, v)
      else:
        logger.warn('\tcannot patch for %s' % k)
        for i in v:
          logger.warn('\t\t%s -> %s %s'
                      % (i,
                         ped[i].father.id if ped[i].father else None,
                         ped[i].mother.id if ped[i].mother else None))

  logger.info('done fixing pedigree')
  return ped


def map_to_plate_wells(by_sample):
  def split_on_plate_records(x):
    def disambiguate_plate(plate_name, place, technology):
      if plate_name:
        return plate_name
      else:
        return 'FAKE-%s_%s' % (place, technology)

    sample_label = '%s-%s' % (x['Sample_Name'], 'dna')
    splits = []
    for place in ['Lanusei', 'Pula', 'USA']:
      if x['Affy_%s' % place]:
        well = x['Well_%s' % place] if x['Well_%s' % place] else 'ZZZ'
        well = well if (len(well) > 1 and well[0].isalpha()
                        and well[1].isdigit()) else 'ZZZ'
        plate = disambiguate_plate(x['Affy_Plate_%s' % place],
                                   place,
                                   'Affymetrix')
        splits.append({
          'scanner' : 'affy-scanner-%s' % place,
          'scanner_location' : place,
          'plate': plate,
          'label':  well,
          'type': ('Affymetrix', 'Genome-Wide Human SNP Array', '6.0'),
          'data_sample': x['Affy_%s' % place],
          'place': place,
          'sample_label' : sample_label,
          })
    for place in ['Base', 'Duplicate']:
      if x['Illumina_%s' % place]:
        plate = disambiguate_plate(x['Illumina_Plate_%s' % place],
                                   place, 'Illumina')
        splits.append({
          'scanner' : 'illumina-scanner-%s' % place,
          'scanner_location' : 'Tramariglio',
          'plate': plate,
          'label':  'ZZZ',
          'type': ('Illumina', 'BeadChip', 'HUMAN1M-DUO'),
          'data_sample': x['Illumina_%s' % place],
          'sample_label' : sample_label,
          'place': place })
    return splits

  wells_of = {}

  for k, v in by_sample.iteritems():
    for r in split_on_plate_records(v[0]):
      wells_of.setdefault(r['plate'], []).append(r)

  titer_plates = {}

  for k, v in wells_of.iteritems():
    n_wells = len(v)
    if n_wells <  8*12:
      rows = 8
      columns = 12
    elif n_wells < 16*24:
      rows = 16
      columns = 24
    elif n_wells < 32*48:
      rows = 24
      columns = 48
    else:
      assert False

    v.sort(key=lambda _: _['label'])
    #--
    def well_label_of_index(index):
      row    = index / columns
      column = index % columns
      return '%s%02d' % (chr(ord('A') + row), column + 1)
    def index_of_well_label(label):
      # FIXME can handle A03. Will break on AA03
      assert label[0].isalpha() and label[1].isdigit()
      return (ord(label[0]) - ord('A'))*columns + int(label[1:]) - 1

    index = 0 # starts with A01
    for r in v:
      if r['label'] == 'ZZZ':
        r['label'] = well_label_of_index(index)
        index += 1
      else:
        index = index_of_well_label(r['label'])
        r['label'] = well_label_of_index(index_of_well_label(r['label']))
    titer_plates[k]= {'label' : k,
                      'rows'  : rows,
                      'columns' : columns,
                      'wells' : v}
  return titer_plates

def dump_pedigree(ped, study, ofname):

  fieldnames = 'study label gender father mother'.split()
  o = csv.DictWriter(open(ofname, 'w'), fieldnames, delimiter='\t')
  o.writeheader()
  for v in ped.values():
    o.writerow({'study': study,
                'label': v.label,
                'gender': v.gender,
                'father': v.father.label if v.father else 'None',
                'mother': v.mother.label if v.mother else 'None'
                })

def dump_blood_samples(by_sample, ped, study, ofname):
  fieldnames = ['study', 'label', 'individual_label', 'source_type',
                'vessel_type', 'vessel_content', 'vessel_status']
  o = csv.DictWriter(open(ofname, 'w'), fieldnames, delimiter='\t')
  o.writeheader()

  for k, v in by_sample.iteritems():
    ikey = (v[0]['Fam_ID'], v[0]['ID'])
    o.writerow({'study': study,
                'label': '%s-%s' % (k, 'bs'),
                'individual_label': ped[ikey].label,
                'source_type' : 'Individual',
                'vessel_type' : 'Tube',
                'vessel_content' : 'BLOOD',
                'vessel_status'  : 'CONTENTUSABLE'
                })

def dump_dna_samples(by_sample, study, ofname):
  fieldnames = ['study', 'label', 'sample_label', 'source_type',
                'vessel_type', 'vessel_content', 'vessel_status']
  o = csv.DictWriter(open(ofname, 'w'), fieldnames, delimiter='\t')
  o.writeheader()

  for k, v in by_sample.iteritems():
    o.writerow({'study': study,
                'label': '%s-%s' % (k, 'dna'),
                'sample_label': '%s-%s' % (k, 'dna'),
                'source_type' : 'Tube',
                'vessel_type' : 'Tube',
                'vessel_content' : 'DNA',
                'vessel_status'  : 'CONTENTUSABLE'
                })

def dump_titer_plates(titer_plates, study, ofname):
  fieldnames = ['study', 'label', 'rows', 'columns']
  o = csv.DictWriter(open(ofname, 'w'), fieldnames, delimiter='\t')
  o.writeheader()

  for k, v in titer_plates.iteritems():
    o.writerow({'study': study,
                'label': k,
                'rows' : v['rows'],
                'columns' : v['columns'],
                })

def dump_plate_wells(titer_plates, study, ofname):
  fieldnames = ['study', 'label', 'plate_label', 'sample_label']
  o = csv.DictWriter(open(ofname, 'w'), fieldnames, delimiter='\t')
  o.writeheader()

  for k, v in titer_plates.iteritems():
    for r in v['wells']:
      o.writerow({'study': study,
                  'label': r['label'],
                  'plate_label' : k,
                  'sample_label' : r['sample_label']
                  })

def dump_chips(titer_plates, study, ofname):
  fieldnames = ['study', 'device_type', 'label',
                'maker', 'model', 'release', 'location']
  o = csv.DictWriter(open(ofname, 'w'), fieldnames, delimiter='\t')
  o.writeheader()

  for k, v in titer_plates.iteritems():
    for r in v['wells']:
      o.writerow({'study': study,
                  'label': 'chip-%s-%s' % (k, r['label']),
                  'device_type': 'Chip',
                  'maker' : r['type'][0],
                  'model' : r['type'][1],
                  'release' : r['type'][2],
                  'location' : 'None'
                  })

def dump_scanners(titer_plates, study, ofname):
  fieldnames = ['study', 'device_type', 'label',
                'maker', 'model', 'release', 'location']
  o = csv.DictWriter(open(ofname, 'w'), fieldnames, delimiter='\t')
  o.writeheader()

  seen = {}
  for k, v in titer_plates.iteritems():
    for r in v['wells']:
      if r['scanner'] in seen:
        continue
      chip_maker = r['type'][0]
      chip_model = r['type'][1]
      if chip_maker == 'Affymetrix':
        maker = 'Affymetrix'
        model = 'GeneChip Scanner 3000'
        release = '7G'
      elif chip_maker == 'Illumina':
        maker = 'Illumina'
        model = 'iScan'
        release = 'A'
      else:
        assert False
      o.writerow({'study': study,
                  'label': r['scanner'],
                  'device_type': 'Scanner',
                  'maker' : maker,
                  'model' : model,
                  'release' : release,
                  'location' : r['scanner_location'],
                  })
      seen[r['scanner']] = True


def dump_data_sample(titer_plates, study, ofname):
  fieldnames = ['study', 'label',
                'well_label',
                'device_label',
                'device_type',
                'scanner_label']
  o = csv.DictWriter(open(ofname, 'w'), fieldnames, delimiter='\t')
  o.writeheader()

  for k, v in titer_plates.iteritems():
    for r in v['wells']:
      o.writerow({'study': study,
                  'label' : r['data_sample'],
                  'well_label': '%s:%s' % (k, r['label']),
                  'device_label': 'chip-%s-%s' % (k, r['label']),
                  'device_type': 'Chip',
                  'scanner_label': r['scanner'],
                  })

def main():
  parser = make_parser()
  args = parser.parse_args()
  #-
  f = csv.DictReader(args.ifile, delimiter='\t')
  all_records = [normalizer(args, r) for r in f]
  by_sample = map_to_sample(all_records)
  pedigree  = map_to_pedigree(all_records)

  sample_to_individual_map = {}
  for k, v in by_sample.iteritems():
    sample_to_individual_map[k] = [(r['Fam_ID'], r['ID']) for r in v]

  pedigree = fix_pedigree(pedigree, sample_to_individual_map)

  titer_plates = map_to_plate_wells(by_sample)

  #--
  dump_pedigree(pedigree, args.study, args.ofile_root + 'individuals.tsv')
  #--
  dump_blood_samples(by_sample, pedigree, args.study,
                     args.ofile_root + 'blood_samples.tsv')
  #--
  dump_dna_samples(by_sample, args.study,
                   args.ofile_root + 'dna_samples.tsv')

  dump_titer_plates(titer_plates, args.study,
                    args.ofile_root + 'titer_plates.tsv')

  dump_plate_wells(titer_plates, args.study,
                    args.ofile_root + 'plate_wells.tsv')

  dump_scanners(titer_plates, args.study,
                args.ofile_root + 'scanners.tsv')

  dump_chips(titer_plates, args.study,
             args.ofile_root + 'devices.tsv')

  dump_data_sample(titer_plates, args.study,
                   args.ofile_root + 'data_sample.tsv')


main()





