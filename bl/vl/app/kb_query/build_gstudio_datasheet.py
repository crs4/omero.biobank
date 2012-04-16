import csv, argparse

from bl.vl.app.importer.core import Core
from bl.vl.kb.drivers.omero.ehr import EHR

class BuildDatasheetApp(Core):

    DIAGNOSIS_ARCH = 'openEHR-EHR-EVALUATION.problem-diagnosis.v1'
    DIAGNOSIS_FIELD = 'at0002.1'
    T1D_ICD10 = 'icd10-cm:E10'
    MS_ICD10 = 'icd10-cm:G35'

    def __init__(self, host = None, user = None, passwd = None, keep_tokens = 1,
                 logger = None, study_label = None, operator = 'Alfred E. Neumann'):
        super(BuildDatasheetApp, self).__init__(host, user, passwd,
                                                keep_tokens = keep_tokens,
                                                study_label = study_label,
                                                logger = logger)

    def get_wells_by_plate(self, plate, ignore_discarded = False):
        query = '''SELECT pw FROM PlateWell pw
                   JOIN pw.container AS pl
                   WHERE pl.vid = :pl_vid'''
        if ignore_discarded:
            query += ' AND pw.status.value != :well_status'
        wells = self.kb.find_all_by_query(query, 
                                          {'pl_vid' : plate.id,
                                           'well_status' : self.kb.VesselStatus.DISCARDED.enum_label()})
        w_lookup = {}
        for w in wells:
            w_lookup[w.slot] = w
        return w_lookup

    def load_plate(self, plate_barcode):
        query = 'SELECT pl FROM TiterPlate pl WHERE pl.barcode = :pl_barcode'
        plate = self.kb.find_all_by_query(query, {'pl_barcode' : plate_barcode})
        return plate[0] if len(plate) > 0 else None

    def get_wells_inds_lookup(self, individuals):
        wi_lookup = {}
        for i in individuals:
            wells = self.kb.get_vessels_by_individual(i, 'PlateWell')
            for w in wells:
                wi_lookup.setdefault(w, set()).add(i)
        for well, ind in wi_lookup.iteritems():
            assert len(ind) == 1
        return wi_lookup

    def calculate_well_label(self, slot_position, plate_columns):
        if (slot_position % plate_columns) != 0:
            row = chr(ord('A') + (slot_position / plate_columns))
            col = slot_position % plate_columns
        else:
            row = chr(ord('A') + (slot_position / plate_columns) - 1)
            col = slot_position % plate_columns + plate_columns
        return '%s%02d' % (row, col)

    def get_affections(self, clinical_records):
        if len(clinical_records) == 0:
            return 'none', 'none'
        ehr = EHR(clinical_records)
        if ehr.matches(self.DIAGNOSIS_ARCH, self.DIAGNOSIS_FIELD, 
                       self.T1D_ICD10):
            t1d = 'True'
        else:
            t1d = 'False'
        if ehr.matches(self.DIAGNOSIS_ARCH, self.DIAGNOSIS_FIELD,
                       self.MS_ICD10):
            ms = 'True'
        else:
            ms = 'False'
        return t1d, ms

    def get_ehr_records_map(self, ehr_recs):
        ehr_map = {}
        for rec in ehr_recs:
            ehr_map.setdefault(rec['i_id'], []).append(rec)
        return ehr_map

    def get_empty_record(self, plate, slot_index):
        return {'PLATE_barcode' : plate.barcode, 
                'PLATE_label' : plate.label,
                'WELL_label' : self.calculate_well_label(slot_index,
                                                         plate.columns)}

    def dump(self, plate_barcode, out_file):
        self.logger.info('Loading plate %s' % plate_barcode)
        plate = self.load_plate(plate_barcode)
        if not plate:
            msg = 'Barcode %s is not related to a known plate' % plate_barcode
            self.logger.critical(msg)
            raise ValueError(msg)
        
        self.logger.info('Loading wells for plate %s' % plate.barcode)
        wells = self.get_wells_by_plate(plate)
        self.logger.info('Loaded %d wells' % len(wells))

        self.logger.info('Loading individuals')
        inds = self.kb.get_objects(self.kb.Individual)
        self.logger.info('Loaded %d individuals' % len(inds))

        self.logger.info('Building individuals-wells lookup table')
        wells_lookup = self.get_wells_inds_lookup(inds)

        self.logger.info('Loading clinical records')
        ehr_records = self.kb.get_ehr_records()
        ehr_records_map = self.get_ehr_records_map(ehr_records)
        self.logger.info('Clinical record loaded')

        self.logger.info('Writing output')
        writer = csv.DictWriter(out_file, delimiter='\t', restval='X',
                                fieldnames = ['Sample_ID', 'PLATE_barcode',
                                              'PLATE_label', 'WELL_label',
                                              'INDIVIDUAL_id', 'INDIVIDUAL_gender',
                                              'T1D_affected', 'MS_affected'])
        writer.writeheader()
        last_slot = 0
        for slot, well in sorted(wells.iteritems()):
            self.logger.debug('WELL: %s --- SLOT: %d' % (well.label, slot))
            try:
                cl_records = ehr_records_map[list(wells_lookup[well])[0].id]
            except KeyError, ke:
                self.logger.warning('Individual %s has no clinical records' % ke)
                cl_records = []
            t1d, ms = self.get_affections(cl_records)
            while(last_slot != slot-1):
                last_slot += 1
                self.logger.info('No data for well %s, filling with dummy record' % 
                                 self.calculate_well_label(last_slot, plate.columns))
                writer.writerow(self.get_empty_record(plate, last_slot))
            record = {'Sample_ID' : '%s|%s' % (plate.barcode, well.label),
                      'PLATE_barcode' : plate.barcode,
                      'PLATE_label' : plate.label,
                      'WELL_label' : well.label,
                      'INDIVIDUAL_gender' : list(wells_lookup[well])[0].gender.enum_label().lower(),
                      'INDIVIDUAL_id' : list(wells_lookup[well])[0].id}
            if t1d:
                record['T1D_affected'] = t1d
            if ms:
                record['MS_affected'] = ms
            writer.writerow(record)
            last_slot = slot

        out_file.close()
        self.logger.info('Job completed')



help_doc = """
Write GenomeStudio datasheet retrieving data related to the select
plate
"""

def make_parser(parser):
    parser.add_argument('-p', '--plate', type=str, required=True,
                        help='barcode of the plate')
    
def implementation(logger, host, user, passwd, args):
    app = BuildDatasheetApp(host = host, user = user, passwd = passwd,
                            keep_tokens = args.keep_tokens, logger = logger,
                            study_label = None)
    app.dump(args.plate, args.ofile)

def do_register(registration_list):
    registration_list.append(('gstudio_datasheet', help_doc, make_parser,
                              implementation))
