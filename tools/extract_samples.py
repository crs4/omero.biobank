"""

Extracts sample records from a 'Ilena format' tsv file.

from one row, it will generate:

   * a record that can map to a BloodSample
   * a record that can map to a DNASampleRecord
   * multiple PlateWell records
   * multiple AffymetrixCel, IlluminaXXX and IlluminaHiseq records

all records will be linked together using (study, label)
tuples. Barcodes are expected to be unique, however.

to accomodate ''importer'' importing strategy, we will split the output in 'per sample type' files.

"""
#---------------------------------------------------------------
import logging, time
LOG_FILENAME = 'extract_samples.log'
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

def individual_conversion_rule(study, x):
  y= {'study' : study}
  y['label'] = x['ID']
  y['gender'] = 'male' if x['Submitted_Gender'] == '1' else 'female'
  y['father'] = x['Father'] if not x['Father'] == '0' else 'None'
  y['mother'] = x['Mother'] if not x['Mother'] == '0' else 'None'
  return y

barcode_counter = 0
def blood_sample_conversion_rule(study, y, x):
  global barcode_counter
  y= {'study' : study, 'label' : '%s-bs-%s' % (study, x['Sample_Name']),
      'barcode' : '%s-%06d' % (study, barcode_counter),
      'individual_label' : y['label'],
      'initial_volume': 20,
      'current_volume': 20,
      'status': 'USABLE'}
  barcode_counter += 1
  return [y]

def dna_sample_conversion_rule(study, s, x):
  global barcode_counter
  print barcode_counter
  y= {'study' : study, 'label' : '%s-dna-%s' % (study, x['Sample_Name']),
      'barcode' : '%s-%06d' % (study, barcode_counter),
      'blood_sample_label' : s['label'],
      'initial_volume': 20, 'current_volume': 20,
      'nanodrop': 50, 'qp230260': 0.333, 'qp230280': 0.44,
      'status': 'USABLE'}
  barcode_counter += 1
  return [y]


def titer_plate_conversion_rule_helper(study, label):
  global barcode_counter
  y = {'study' : study, 'label' : label, 'barcode' : barcode_counter,
       'rows' : 32, 'columns' : 48, 'maker' : 'CRS4', 'model' : 'virtual'}
  barcode_counter += 1
  return y

known_plates = {}
def titer_plate_conversion_rule(study, r):
  pfields = ['Affymetrix_Plate_Pula', 'Affymetrix_Plate_Lanusei', 'Affymetrix_Plate_USA',
             'Illumina_Plate', 'Illumina_dup_Plate']
  tps = []
  for k in pfields:
    if r.has_key(k) and not r[k] == 'x':
      label = r[k]
      if not known_plates.has_key(label):
        tps.append(titer_plate_conversion_rule_helper(study, r[k]))
        known_plates[label] = 1
  return tps

#----------------------------------------
row_counters = {}
column_counters = {}
max_columns = 48
def convert_well_position(label, pos):
  pos = 'TBF' if pos == 'x' else pos
  if pos == 'TBF':
    c = column_counters.setdefault(label, 0)
    r = row_counters.setdefault(label, 0)
    if c == max_columns:
      r += 1
      column_counters[label] = 0
      row_counters[label] = r
      c = 0
    else:
      column_counters[label] += 1
  elif len(pos) <=3:
    if pos[0].isalpha():
      r = ord(pos[0]) - ord('A')
      c = int(pos[1:])
    else:
      r = ord(pos[-1]) - ord('A')
      c = int(pos[0:-1])
  else:
    raise ValueError('cannot convert %s %s' % (label, pos))
  return (r, c)

def plate_well_conversion_rule_helper(study, s, plate_label, plate_well):
  y = {'study' : study, 'volume':  0.1, 'dna_label' : s['label'],
       'plate_label' : plate_label,
       }
  y['row'], y['column'] = convert_well_position(plate_label, plate_well)
  y['label'] = '%s:[%d,%d]' % (plate_label, y['row'], y['column'])
  return y

def plate_well_conversion_rule(study, dna, x):
  samples = {}

  if not x['Affymetrix_Plate_Pula'] == 'x':
    samples['Affy_Pula']= plate_well_conversion_rule_helper(study, dna,
                                                       x['Affymetrix_Plate_Pula'],
                                                       x['Well_Pula'])
  if not x['Affymetrix_Plate_Lanusei'] == 'x':
    samples['Affy_Lanusei'] = plate_well_conversion_rule_helper(study, dna,
                                                           x['Affymetrix_Plate_Lanusei'],
                                                           x['Well_Lanusei'])
  if not x['Affymetrix_Plate_USA'] == 'x':
    samples['Affy_USA'] = plate_well_conversion_rule_helper(study, dna,
                                                       x['Affymetrix_Plate_USA'],
                                                       x['Well_USA'])
  if not x['Illumina_Plate'] == 'x':
    samples['Illumina'] = plate_well_conversion_rule_helper(study, dna,
                                                            x['Illumina_Plate'],
                                                            'TBF')
  if not x['Illumina_dup_Plate'] == 'x':
    samples['Illumina_dup'] = plate_well_conversion_rule_helper(study, dna,
                                                                x['Illumina_dup_Plate'],
                                                                'TBF')

  return samples

def data_sample_conversion_rule_helper(study, sample,
                                       data_sample_name, device_maker,
                                       device_model, device_release,
                                       device_name, specific=None):
  y = {'study' : study,
       'label' : data_sample_name,
       'sample_label' : sample['label'],
       'device_maker' : device_maker,
       'device_model' : device_model,
       'device_release' : device_release,
       'device_name'  : device_name,
       }
  if specific:
    y.update(specific)
  return y

def data_sample_conversion_rule_driver(study, dna_sample, plate_wells, x):
  samples = []
  if not x['Illumina_1M'] == 'x':
    samples.append(data_sample_conversion_rule_helper(study, plate_wells['Illumina'],
                                                      x['Illumina_1M'],
                                                      'Illumina', 'Human1M', '1.0',
                                                      'PortoConte-1'))

  if not x['Illumina_1M_duplicates'] == 'x':
    samples.append(data_sample_conversion_rule_helper(study, plate_wells['Illumina_dup'],
                                                      x['Illumina_1M_duplicates'],
                                                      'Illumina', 'Human1M', '1.0',
                                                      'PortoConte-1'))

  if not x['Affy_Pula'] == 'x':
    samples.append(data_sample_conversion_rule_helper(study, plate_wells['Affy_Pula'],
                                                      x['Affy_Pula'],
                                                      'Affymetrix', 'GenomeWideSNP_6', '1.0',
                                                      'Pula-1',
                                                      specific={'contrastQC' : x['cQC_Pula']}))

  if not x['Affy_Lanusei'] == 'x':
    samples.append(data_sample_conversion_rule_helper(study, plate_wells['Affy_Lanusei'],
                                                      x['Affy_Lanusei'],
                                                      'Affymetrix', 'GenomeWideSNP_6', '1.0',
                                                      'Lanusei-1',
                                                      specific={'contrastQC' : x['cQC_Lanusei']}))
  if not x['Affy_USA'] == 'x':
    samples.append(data_sample_conversion_rule_helper(study, plate_wells['Affy_USA'],
                                                      x['Affy_USA'],
                                                      'Affymetrix', 'GenomeWideSNP_6', '1.0',
                                                      'Affymetrix-inc-X',
                                                      specific={'contrastQC' : x['cQC_USA']}))
  if not x['Solexa_Sequencing_ID'] == 'x':
    samples.append(data_sample_conversion_rule_helper(study, dna_sample,
                                                      x['Solexa_Sequencing_ID'],
                                                      'Illumina', 'HiSeq_2000', '1.0',
                                                      'Pula-hiseq-1'))
  if not x['Sanger_Sequencing_ID'] == 'x':
     samples.append(data_sample_conversion_rule_helper(study, dna_sample,
                                                       x['Sanger_Sequencing_ID'],
                                                       'Applied Biosystems', 'Prism_XXX', '50.0',
                                                       'PortoConte-sanger-1'))
  return samples


def data_sample_conversion_rule(study, dna_sample, plate_wells, x):
  try:
    samples = data_sample_conversion_rule_driver(study, dna_sample, plate_wells, x)
  except KeyError, e:
    logger.error('missing sample information (%s) in record %s' % (e, x))
    samples = []
  return samples

def make_parser():
  parser = argparse.ArgumentParser(description="""
  An importer for Ilenia's style tsv files.
  """)
  parser.add_argument('-i', '--ifile', type=argparse.FileType('r'),
                        help='the input tsv file',
                        default=sys.stdin)
  parser.add_argument('-r', '--ofile-root', type=str,
                        help='the output tsv filename root',
                        default='extracted-')
  parser.add_argument('-s', '--study', type=str,
                        help='study label')
  parser.add_argument('-p', '--patch-missing-plate-info', action='store_true', default=False,
                      help="""if set, it will add fictitious TiterPlates and wells when needed to
                      mantain the dependency chain""")

  return parser


plate_counter = 0
def patch_missing_plate_info(x):
  global plate_counter
  #- patch missing Pula plate info
  if not x['Affy_Pula'] == 'x' and x['Affymetrix_Plate_Pula'] == 'x':
    x['Affymetrix_Plate_Pula'] = 'fake-plate-%03d' % plate_counter
    x['Well_Pula'] = 'A01'
    logger.warn('patched record [%s:%s] to %s[%s]' % (x['Fam_ID'], x['ID'],
                                                      x['Affymetrix_Plate_Pula'],
                                                      x['Well_Pula']))
    plate_counter += 1

def is_worth_analyzing(x):
  has_data = False
  for k in x.keys():
    if k.startswith('Affy_') and x[k] != 'x':
      has_data = True
    elif k.startswith('Illumina_') and x[k] != 'x':
      has_data = True
    elif k.startswith('Solexa_') and x[k] != 'x':
      has_data = True
    elif k.startswith('Sanger_') and x[k] != 'x':
      has_data = True
  return has_data

def normalizer(args, x):
  if x.has_key(None):
    del x[None]
  for k in x.keys():
    x[k] = 'x' if x[k] == '-' else x[k]
    if k.startswith('cQC_') and x[k] in ['TBF', 'x']:
      x[k] = 'None'
  if args.patch_missing_plate_info:
    patch_missing_plate_info(x)
  if not x['Affy_USA']  == 'x':
    x['Affy_USA'] = x['Affy_USA'] + '.CEL' if not x['Affy_USA'].upper().endswith('.CEL') else ''
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
  f = csv.DictReader(args.ifile, delimiter='\t')

  blood_samples = []
  dna_samples = []
  titer_plates = []
  plate_wells = []
  data_samples = []
  for x in f:
    x = normalizer(args, x)
    if not is_worth_analyzing(x):
      continue
    y = individual_conversion_rule(args.study, x)
    bss = blood_sample_conversion_rule(args.study, y, x)
    blood_samples.extend(bss)
    dns = dna_sample_conversion_rule(args.study, bss[0], x)
    dna_samples.extend(dns)
    tps = titer_plate_conversion_rule(args.study, x)
    titer_plates.extend(tps)
    pws = plate_well_conversion_rule(args.study, dns[0], x)
    plate_wells.extend(pws.values())
    dss = data_sample_conversion_rule(args.study, dns[0], pws, x)
    data_samples.extend(dss)
  #-
  dump_(args.ofile_root + 'blood_samples.tsv', blood_samples,
        fieldnames='study label barcode individual_label initial_volume current_volume status'.split())
  dump_(args.ofile_root + 'dna_samples.tsv', dna_samples,
        fieldnames='study label barcode blood_sample_label initial_volume current_volume status nanodrop qp230260 qp230280'.split())

  dump_(args.ofile_root + 'titer_plates.tsv', titer_plates,
        fieldnames='study label barcode rows columns maker model'.split())
  #-
  dump_(args.ofile_root + 'plate_wells.tsv', plate_wells,
        fieldnames='study label plate_label row column dna_label volume'.split())
  #--
  dump_(args.ofile_root + 'affy_gw.tsv',
        [x for x in data_samples
         if x['device_maker'] == 'Affymetrix' and x['device_model'] == 'GenomeWideSNP_6'],
        fieldnames='study label contrastQC sample_label device_maker device_model device_release device_name'.split())
  dump_(args.ofile_root + 'illumina_hu1M.tsv',
        [x for x in data_samples
         if x['device_maker'] == 'Illumina' and x['device_model'] == 'Human1M'],
        fieldnames='study label sample_label device_maker device_model device_release device_name'.split())
  dump_(args.ofile_root + 'illumina_hiseq.tsv',
        [x for x in data_samples
         if x['device_maker'] == 'Illumina' and x['device_model'] == 'HiSeq_2000'],
        fieldnames='study label sample_label device_maker device_model device_release device_name'.split())
  dump_(args.ofile_root + 'abi-prism.tsv',
        [x for x in data_samples
         if x['device_maker'] == 'Applied Biosystems' and x['device_model'] == 'Prism_XXX'],
        fieldnames='study label sample_label device_maker device_model device_release device_name'.split())
main()
