import csv, argparse, sys

from bl.vl.app.importer.core import Core
from bl.vl.kb.drivers.omero.vessels import VesselStatus
from bl.vl.kb.drivers.omero.ehr import EHR

class BuildPlateDataSamplesDetails(Core):

    VESSEL_STATUS = [VesselStatus.CONTENTCORRUPTED.enum_label(),
                     VesselStatus.CONTENTUSABLE.enum_label(),
                     VesselStatus.DISCARDED.enum_label(),
                     VesselStatus.UNKNOWN.enum_label(),
                     VesselStatus.UNUSABLE.enum_label(),
                     VesselStatus.UNUSED.enum_label()]

    DIAGNOSIS_ARCH = 'openEHR-EHR-EVALUATION.problem-diagnosis.v1'
    DIAGNOSIS_FIELD = 'at0002.1'
    T1D_ICD10 = 'icd10-cm:E10'
    MS_ICD10 = 'icd10-cm:G35'
    NEFRO_ICD10 = 'icd10-cm:E23.2'

    def __init__(self, host = None, user = None, passwd = None, keep_tokens = 1,
                 logger = None, study_label = None, operator = 'Alfred E. Neumann'):
        super(BuildPlateDataSamplesDetails, self).__init__(host, user, passwd,
                                                           keep_tokens = keep_tokens,
                                                           study_label = study_label,
                                                           logger = logger)
        self.ACTION_TYPES = [self.kb.ActionOnVessel,
                             self.kb.ActionOnDataCollectionItem]
        self.TARGET_TYPES = [self.kb.PlateWell,
                             self.kb.DataCollectionItem]

    def load_plate(self, plate_barcode):
        query = 'SELECT pl FROM TiterPlate pl WHERE pl.barcode = :pl_barcode'
        plate = self.kb.find_all_by_query(query, {'pl_barcode' : plate_barcode})
        return plate[0] if len(plate) > 0 else None

    def load_genotype_data_samples_lookup(self):
        self.logger.info('Loading Data Samples lookup table')
        acts = []
        for at in self.ACTION_TYPES:
            acts.extend(self.kb.get_objects(at))
        targets = []
        for tt in self.TARGET_TYPES:
            targets.extend(self.kb.get_objects(tt))
        gdsamples = self.kb.get_objects(self.kb.GenotypeDataSample)
        lookup = {}
        for gds in gdsamples:
            lookup.setdefault(gds.action.target, []).append(gds)
        self.logger.info('Data Samples lookup table loaded')
        return lookup

    def get_connected_data_samples(self, well, dsamples_lookup):
        try:
            return dsamples_lookup[well]
        except KeyError:
            return []

    def calculate_well_label(self, slot_position, plate_columns):
        if (slot_position % plate_columns) != 0:
            row = chr(ord('A') + (slot_position / plate_columns))
            col = slot_position % plate_columns
        else:
            row = chr(ord('A') + (slot_position / plate_columns) - 1)
            col = slot_position % plate_columns + plate_columns
        return '%s%02d' % (row, col)

    def get_empty_record(self, plate, slot_index):
        return {'PLATE_barcode' : plate.barcode,
                'PLATE_label' : plate.label,
                'WELL_label' : self.calculate_well_label(slot_index,
                                                         plate.columns),
                'WELL_status' : 'UNKNOWN OR EMPTY'}

    def load_collection_filter(self, vessels_collection):
        coll_items = self.kb.get_vessels_collection_items(vessels_collection)
        return [vci.vessel.id for vci in coll_items if vci.vessel.OME_TABLE == 'PlateWell']

    def load_plate_wells_lookup(self, plate, wells_filter, vessels_type_filter = None):
        self.logger.info('Loading wells for plate %s (barcode %s)' % (plate.label,
                                                                      plate.barcode))
        if wells_filter:
            wells = [w for w in self.kb.get_wells_by_plate(plate) if w.id in wells_filter]
        else:
            wells = list(self.kb.get_wells_by_plate(plate))
        if vessels_type_filter:
            wells = [w for w in wells if w.status.enum_label() \
                         not in vessels_type_filter]
        self.logger.info('Loaded %d wells' % len(wells))
        wells_map = {}
        for w in wells:
            wells_map[w.slot] = w
        return wells_map

    def load_study_map(self, study_label):
        st = self.kb.get_study(study_label)
        if not st:
            msg = 'Study %s unknown' % study_label
            self.logger.error(msg)
            raise ValueError(msg)
        enrolled = self.kb.get_enrolled(st)
        st_map = {}
        for en in enrolled:
            st_map[en.individual] = en
        return st_map

    def get_individual(self, plate_well):
        return self.kb.dt.get_connected(plate_well, self.kb.Individual)[0]

    def load_ehr_map(self):
        self.logger.info('Loading EHR informations')
        ehr_records = self.kb.get_ehr_records()
        self.logger.info('Loaded %d clinical records' % len(ehr_records))
        ehr_map = {}
        for r in ehr_records:
            ehr_map.setdefault(r['i_id'], []).append(r)
        return ehr_map

    def get_affections(self, ehr_records):
        if len(ehr_records) == 0:
            return None, None, None
        t1d = False
        ms = False
        nefro = False
        ehr = EHR(ehr_records)
        if ehr.matches(self.DIAGNOSIS_ARCH, self.DIAGNOSIS_FIELD, self.T1D_ICD10):
            t1d = True
        if ehr.matches(self.DIAGNOSIS_ARCH, self.DIAGNOSIS_FIELD, self.MS_ICD10):
            ms = True
        if ehr.matches(self.DIAGNOSIS_ARCH, self.DIAGNOSIS_FIELD, self.NEFRO_ICD10):
            nefro = True
        return t1d, ms, nefro

    def dump(self, plate_barcodes, fetch_all, vessels_collection, 
             ignore_types, map_study, out_file):
        if not plate_barcodes and not fetch_all:
            raise ValueError('At least one between the --plate and --fetch_all parameters must be submitted')
        
        if ignore_types:
            type_filter = ignore_types.split(',')
            for tf in type_filter:
                if tf not in self.VESSEL_STATUS:
                    msg = '%s is not a legal VesselStatus' % tf
                    self.logger.critical(msg)
                    sys.exit(msg)
        else:
            type_filter = None

        if map_study:
            study_map = self.load_study_map(map_study)
        else:
            study_map = None

        if vessels_collection:
            vcoll = self.kb.get_vessels_collection(vessels_collection)
            if vcoll is None:
                msg = 'Unable to find VesselsCollection object with label %s' % vessels_collection
                self.logger.warning(msg)
                sys.exit()
            else:
                wells_filter = self.load_collection_filter(vcoll)
                self.logger.info('Loaded %d wells to apply as filter' % len(wells_filter))
                if len(wells_filter) == 0:
                    msg = 'Filter is empty, nothing to do'
                    self.logger.warning(msg)
                    sys.exit(0)
        else:
            wells_filter = None

        ehr_map = self.load_ehr_map()
        
        self.logger.info('Loading plates')
        if fetch_all:
            plates = [pl for pl in self.kb.get_objects(self.kb.TiterPlate) \
                          if pl.barcode]
        else:
            plates = []
            for bc in plate_barcodes.split(','):
                self.logger.info('Loading plate %s' % bc)
                plates.append(self.load_plate(bc))
        if len(plates) == 0:
            msg = 'No plates found'
            self.logger.critical(msg)
            raise ValueError(msg)
        else:
            self.logger.info('Loaded %d plates' % len(plates))

        gds_lookup = self.load_genotype_data_samples_lookup()

        plates_lookup = {}
        for pl in plates:
            plates_lookup[pl] = self.load_plate_wells_lookup(pl, wells_filter, type_filter)

        field_names = ['PLATE_barcode', 'PLATE_label', 'WELL_label', 'WELL_status',
                       'DATA_SAMPLE_label']
        if map_study:
            field_names.extend(['INDIVIDUAL_study_code'])
        field_names.extend(['INDIVIDUAL_gender', 'T1D_affection', 'MS_affection',
                            'NEFRO_affection'])
        writer = csv.DictWriter(out_file, delimiter='\t', restval = 'X',
                                fieldnames = field_names)
        writer.writeheader()
        for pl, wmap in plates_lookup.iteritems():
            last_slot = 0
            for slot, well in sorted(wmap.iteritems()):
                self.logger.debug('WELL %s --- SLOT %d' % (well.label, slot))
                while(last_slot < slot - 1):
                    last_slot += 1
                    self.logger.debug('No data for well %s, filling with a dummy record' %
                                      self.calculate_well_label(last_slot, pl.columns))
                    writer.writerow(self.get_empty_record(pl, last_slot))
                record = {'PLATE_barcode' : pl.barcode,
                          'PLATE_label' : pl.label,
                          'WELL_label' : well.label,
                          'WELL_status' : well.status.enum_label()}
                dsamples = self.get_connected_data_samples(well, gds_lookup)
                self.logger.info('Retrieved %d data samples for well %s' % (len(dsamples),
                                                                            well.label))
                if len(dsamples) > 0:
                    record['DATA_SAMPLE_label'] = dsamples[0].label
                ind = self.get_individual(well)
                record['INDIVIDUAL_gender'] = ind.gender.enum_label()
                if map_study:
                    try:
                        record['INDIVIDUAL_study_code'] = "{}:{}".format(study_map[ind].study.label,
                                                                         study_map[ind].studyCode)
                    except KeyError, ke:
                        self.logger.debug('Individual %s has no enrollment in %s' % (ind.id,
                                                                                     map_study))
                try:
                    t1d, ms, nefro = self.get_affections(ehr_map[ind.id])
                    if not t1d is None:
                        record['T1D_affection'] = t1d
                    if not ms is None:
                        record['MS_affection'] = ms
                    if not nefro is None:
                        record['NEFRO_affection'] = nefro
                except KeyError, ke:
                    self.logger.debug('Unable to find EHR lookup for %s' % ind.id)
                writer.writerow(record)
                last_slot = slot
            # Fill empty records if last_slot != plate.rows * plate.columns
            while (last_slot != (pl.rows * pl.columns)):
                last_slot += 1
                self.logger.debug('No data for well %s, filling with a dummy record' %
                                  self.calculate_well_label(last_slot, pl.columns))
                writer.writerow(self.get_empty_record(pl, last_slot))

        out_file.close()
        self.logger.info('Job completed')

help_doc = """
Write the status of a TiterPlate, retrieving PlateWell and DataSamples
connected to them.
"""

def make_parser(parser):
    parser.add_argument('-p', '--plates', type=str, help = 'one or more barcodes separated by a comma')
    parser.add_argument('--fetch_all', action='store_true',
                        help='retrieve all plates with a barcode, this parameter overrides the --plate')
    parser.add_argument('--vessels_collection', type=str,
                        help='vessels collection label used as a filter, wells that no belog to this collection will be treated as empty')
    parser.add_argument('--ignore_types', type=str,
                        help='a comma separated list of VesselStatus, vessels that match with one of these parameters will be treated as empty. Legal values are %r' % BuildPlateDataSamplesDetails.VESSEL_STATUS)
    parser.add_argument('--map_study', type=str,
                        help='retrieve enrollment codes for the specified study and add "study" and "study_code" columns to the output file')

def implementation(logger, host, user, passwd, args):
    app = BuildPlateDataSamplesDetails(host = host, user = user,
                                       passwd = passwd, study_label = None,
                                       keep_tokens = args.keep_tokens,
                                       logger = logger)
    app.dump(args.plates, args.fetch_all, args.vessels_collection, 
             args.ignore_types, args.map_study, args.ofile)

def do_register(registration_list):
    registration_list.append(('plate_data_samples', help_doc, make_parser,
                              implementation))
