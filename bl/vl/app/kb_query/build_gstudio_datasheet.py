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

    def load_plate(self, plate_label):
        query = 'SELECT pl FROM TiterPlate pl WHERE pl.label = :pl_label'
        plate = self.kb.find_all_by_query(query, {'pl_label' : plate_label})
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

    def calculate_sentrix_position(self, slot_position):
        row = (slot_position % 6) + 1
        if ((slot_position % 12) in range(0,6)):
            col = 1
        else:
            col = 2    
        return 'R%02dC%02d' % (row, col)
    
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
        return { #'Sample_ID':'%s:%s' % (plate.barcode, self.calculate_well_label(slot_index,
#                                                                                plate.columns)),
#                'PLATE_barcode' : plate.barcode, 
                'Sample_Plate' : plate.label,
#                'Sample_Name':'%s:%s' % (plate.barcode, self.calculate_well_label(slot_index,
#                plate.columns)),
                'Sample_ID' : 'Empty',
                'Sample_Name' : 'Empty',
                'Gender' : 'Empty',
                'AMP_Plate' : 0,
                'SentrixPosition_A' : self.calculate_sentrix_position(slot_index-1),
                'Sample_Well' : self.calculate_well_label(slot_index,
                                                         plate.columns)}

    def dump(self, operator, plate_barcode, manifest, out_file):
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

#        self.logger.info('Loading clinical records')
#        ehr_records = self.kb.get_ehr_records()
#        ehr_records_map = self.get_ehr_records_map(ehr_records)
#        self.logger.info('Clinical record loaded')

        self.logger.info('Writing output')

        headerWriter = csv.writer(out_file, delimiter=';',
                                  quoting=csv.QUOTE_MINIMAL)
        headerWriter.writerow(['[Header]'])
        headerWriter.writerow(['Investigator Name', operator])
        headerWriter.writerow(['Project Name'])
        headerWriter.writerow(['Experiment Name'])
        headerWriter.writerow(['Date'])
        headerWriter.writerow(['[Manifests]'])
        headerWriter.writerow(['A', manifest])
        headerWriter.writerow(['[Data]'])
        #out_file.close()

        
        writer = csv.DictWriter(out_file, delimiter=';', restval='',
                                fieldnames = ['Sample_ID', 'Sample_Plate', 'Sample_Name',
                                              'Project', 'AMP_Plate', 'Sample_Well',
                                              'SentrixBarcode_A', 'SentrixPosition_A',
                                              'Scanner', 'Date_Scan', 'Replicate',
                                              'Parent1', 'Parent2', 'Gender'])
        writer.writeheader()
        last_slot = 0
        for slot, well in sorted(wells.iteritems()):
            self.logger.debug('WELL: %s --- SLOT: %d' % (well.label, slot))
#            try:
#                cl_records = ehr_records_map[list(wells_lookup[well])[0].id]
#            except KeyError, ke:
#                self.logger.warning('Individual %s has no clinical records' % ke)
#                cl_records = []
#            t1d, ms = self.get_affections(cl_records)
            while(last_slot != slot-1):
                last_slot += 1
                self.logger.info('No data for well %s, filling with dummy record' % 
                                 self.calculate_well_label(last_slot, plate.columns))
                writer.writerow(self.get_empty_record(plate, last_slot))
            record = {'Sample_ID' : '%s:%s' % (plate.barcode, well.label),
#                      'PLATE_barcode' : plate.barcode,
                      'Sample_Plate' : plate.label,
                      'Sample_Name' : '%s:%s' % (plate.barcode, well.label),
                      'Project' : '%s_OmniExpress' % (plate.label),
                      'AMP_Plate' : 0,
                      'Sample_Well' : well.label,
                      'SentrixPosition_A' : self.calculate_sentrix_position(last_slot),
                      'Gender' : list(wells_lookup[well])[0].gender.enum_label().upper()}
#                      'INDIVIDUAL_id' : list(wells_lookup[well])[0].id}
#            if t1d:
#                record['T1D_affected'] = t1d
#            if ms:
#                record['MS_affected'] = ms
            writer.writerow(record)
            last_slot = slot
        #Fill empty slots at the end of the plate
        while (last_slot != (plate.rows * plate.columns)):
            last_slot += 1
            self.logger.info('No data for well %s, filling with dummy record' %
                             self.calculate_well_label(last_slot, plate.columns))
            writer.writerow(self.get_empty_record(plate, last_slot))

        out_file.close()
        self.logger.info('Job completed')



help_doc = """
Write GenomeStudio datasheet retrieving data related to the select
plate
"""

def make_parser(parser):
    parser.add_argument('-p', '--plate', type=str, required=True,
                        help='barcode of the plate')
    parser.add_argument('--manifest', type=str, required=True,
                        help='manifest file of genotyping')
    
def implementation(logger, host, user, passwd, args):
    app = BuildDatasheetApp(host = host, user = user, passwd = passwd,
                            keep_tokens = args.keep_tokens, logger = logger,
                            study_label = None)
    app.dump(args.operator, args.plate, args.manifest, args.ofile)

def do_register(registration_list):
    registration_list.append(('gstudio_datasheet', help_doc, make_parser,
                              implementation))
