"""
This util generates a datasheet related to a TiterPlate that can be
used as an input file for GenomeStudio during the Final Report
building process.

Output file format will be like

Sample_ID   PLATE_barcode   PLATE_name   WELL_label   INDIVIDUAL_gender  Individual_VID   T1D_affected   MS_affected
1112        A9033ZEC        FOO_BAR_pl   A01          m                  V124151          True           False
1113        A9033ZEC        FOO_BAR_pl   A02          m                  V2151AA          False          False
1114        A9033ZEC        FOO_BAR_pl   A03          m                  V61236EF         False          True
.......

At the moment it is hardwired to IMMUNOCHIP study

"""

import csv, argparse, sys, os

from bl.vl.utils import LOG_LEVELS, get_logger
from bl.vl.kb import KnowledgeBase as KB
from bl.vl.kb.drivers.omero.ehr import EHR

DIAGNOSIS_ARCH = 'openEHR-EHR-EVALUATION.problem-diagnosis.v1'
DIAGNOSIS_FIELD = 'at0002.1'
T1D_ICD10 = 'icd10-cm:E10'
MS_ICD10 = 'icd10-cm:G35'

PLATE_COLUMNS = 12

STUDY_LABELS = ['IMMUNOCHIP', 'IMMUNOCHIP_DUPLICATI']

CSV_FIELDS = ['Sample_ID', 'PLATE_barcode', 'PLATE_name',
              'WELL_label', 'INDIVIDUAL_gender', 'INDIVIDUAL_vid',
              'T1D_affected', 'MS_affected']

def make_parser():
    parser = argparse.ArgumentParser(description='build a datasheet using the given plate barcode')
    parser.add_argument('--logfile', type=str, help='log file (default=stderr)')
    parser.add_argument('--loglevel', type=str, choices = LOG_LEVELS,
                        help='logging level (default=INFO)', default='INFO')
    parser.add_argument('-H', '--host', type=str, help='omero hostname',
                        default='localhost')
    parser.add_argument('-U', '--user', type=str, help='omero user',
                        default='root')
    parser.add_argument('-P', '--passwd', type=str, required=True,
                        help='omero password')
    parser.add_argument('--plates_list', type=str, required=True,
                        help='the list of barcodes related to the plates that must be processed. One barcode per line.')
    parser.add_argument('--out_dir', type=str, help='output files directory',
                        required=True)
    return parser

def get_wells_by_plate(plate_barcode, kb):
    query = '''SELECT pw FROM PlateWell pw 
               JOIN pw.container AS pl
               WHERE pl.barcode = :pl_barcode
               AND pw.status.value != :well_status'''
    wells = kb.find_all_by_query(query, {'pl_barcode' : plate_barcode, 'well_status' : 'DISCARDED'})
    w_lookup = {}
    for w in wells:
        w_lookup[w.slot] = w
    return w_lookup

def load_plate(plate_barcode, kb):
    query = 'SELECT pl FROM TiterPlate pl WHERE pl.barcode = :pl_barcode'
    plate = kb.find_all_by_query(query, {'pl_barcode' : plate_barcode})
    return plate[0] if len(plate) > 0 else None

def get_wells_enrolls_lookup(enrollments, kb):
    we_lookup = {}
    for e in enrollments:
        wells = kb.get_vessels_by_individual(e.individual, 'PlateWell')
        for w in wells:
            we_lookup.setdefault(w,[]).append(e)
    # Check that each well is connected only to one individual even for multiple wells
    for well, enrolls in we_lookup.iteritems():
        assert len(set([e.individual.id for e in enrolls])) == 1
    return we_lookup

def get_well_label(slot_position):
    if (slot_position % PLATE_COLUMNS) != 0:
        row = chr(ord('A') + (slot_position / PLATE_COLUMNS))
        col = slot_position % PLATE_COLUMNS
    else:
        row = chr(ord('A') + (slot_position / PLATE_COLUMNS) - 1)
        col = slot_position % PLATE_COLUMNS + 12
    return '%s%02d' % (row, col)

def get_affections(clinical_records):
    ehr = EHR(clinical_records)
    if ehr.matches(DIAGNOSIS_ARCH, DIAGNOSIS_FIELD, T1D_ICD10):
        t1d = 'True'
    else:
        t1d = 'False'
    if ehr.matches(DIAGNOSIS_ARCH, DIAGNOSIS_FIELD, MS_ICD10):
        ms = 'True'
    else:
        ms = 'False'
    return t1d, ms

def get_ichip_sample_code(enrollments, plate_barcode, logger):
    if len(enrollments) == 1:
        return enrollments[0].studyCode.split('|')[0]
    else:
        logger.debug('Found %d enrollments ---> %r' % (len(enrollments),
                                                      [e.studyCode for e in enrollments]))
        for enroll in enrollments:
            sample, barcode = enroll.studyCode.split('|')
            if barcode == plate_barcode:
                logger.debug('Selected sample id %s' % sample)
                return sample

def map_gender(individual):
    gmap = {'MALE' : 'male', 'FEMALE' : 'female'}
    return gmap[individual.gender.enum_label()]

def main(argv):
    parser = make_parser()
    args = parser.parse_args(argv)
    logger = get_logger("main", level=args.loglevel, filename=args.logfile)

    kb = KB(driver='omero')(args.host, args.user, args.passwd)
    
    # Load enrollments and individual (needed to build sample label and for gender field)
    enrolls = []
    for sl in STUDY_LABELS:
        logger.debug('Loading enrollments for study %s' % sl)
        enrolls.extend(kb.get_enrolled(kb.get_study(sl)))
        logger.debug('Fetched %d individuals' % len(enrolls))

    wells_lookup = get_wells_enrolls_lookup(enrolls, kb)

    logger.debug('Loading EHR records')
    ehr_records = kb.get_ehr_records('(valid == True)')
    ehr_records_map = {}
    for r in ehr_records:
        ehr_records_map.setdefault(r['i_id'], []).append(r)

    # Read plate barcodes
    with open(args.plates_list) as pl_list:
        barcodes = [row.strip() for row in pl_list]

    # Load plate
    for plate_barcode in barcodes:
        logger.info('Creating datasheet for plate %s' % plate_barcode)
        pl = load_plate(plate_barcode, kb)
        if not pl:
            logger.error('No plate with barcode %s exists, skipping it.' % (plate_barcode))
            continue

        # Load wells for selected plate
        pl_wells = get_wells_by_plate(plate_barcode, kb)

        with open(os.path.join(args.out_dir, '%s_datasheet.csv' % plate_barcode), 'w') as of:
            writer = csv.DictWriter(of, CSV_FIELDS, delimiter='\t')
            writer.writeheader()
            last_slot = 0
            for slot, well in sorted(pl_wells.iteritems()):    
                cl_record = ehr_records_map[wells_lookup[well][0].individual.id]
                t1d, ms = get_affections(cl_record)
                # Fill empty slots
                while(last_slot != slot-1):
                    last_slot += 1
                    writer.writerow({'Sample_ID' : 'X',
                                     'PLATE_barcode' : pl.barcode,
                                     'PLATE_name' : pl.label,
                                     'WELL_label' : get_well_label(last_slot),
                                     'INDIVIDUAL_gender' : 'X',
                                     'INDIVIDUAL_vid' : 'X',
                                     'T1D_affected' : 'X',
                                     'MS_affected' : 'X'})
                    
                writer.writerow({'Sample_ID' : get_ichip_sample_code(wells_lookup[well], pl.barcode, logger),
                                 'PLATE_barcode' : pl.barcode,
                                 'PLATE_name' : pl.label,
                                 'WELL_label' : well.label,
                                 'INDIVIDUAL_gender' : map_gender(wells_lookup[well][0].individual),
                                 'INDIVIDUAL_vid' : wells_lookup[well][0].individual.id,
                                 'T1D_affected' : t1d,
                                 'MS_affected' : ms})
                last_slot = slot


if __name__ == '__main__':
    main(sys.argv[1:])
