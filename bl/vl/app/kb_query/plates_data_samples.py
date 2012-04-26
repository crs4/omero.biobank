import csv, argparse, sys

from bl.vl.app.importer.core import Core

class BuildPlateDataSamplesDetails(Core):

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

    def load_plate_wells_lookup(self, plate, wells_filter):
        self.logger.info('Loading wells for plate %s (barcode %s)' % (plate.label,
                                                                      plate.barcode))
        if wells_filter:
            wells = [w for w in self.kb.get_wells_by_plate(plate) if w.id in wells_filter]
        else:
            wells = list(self.kb.get_wells_by_plate(plate))
        self.logger.info('Loaded %d wells' % len(wells))
        wells_map = {}
        for w in wells:
            wells_map[w.slot] = w
        return wells_map

    def dump(self, plate_barcodes, fetch_all, vessels_collection, out_file):
        if not plate_barcodes and not fetch_all:
            raise ValueError('At least one between the --plate and --fetch_all parameters must be submitted')
        
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
            plates_lookup[pl] = self.load_plate_wells_lookup(pl, wells_filter)

        writer = csv.DictWriter(out_file, delimiter='\t', restval = 'X',
                                fieldnames = ['PLATE_barcode', 'PLATE_label',
                                              'WELL_label', 'WELL_status',
                                              'DATA_SAMPLE_label'])
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

def implementation(logger, host, user, passwd, args):
    app = BuildPlateDataSamplesDetails(host = host, user = user,
                                       passwd = passwd, study_label = None,
                                       keep_tokens = args.keep_tokens,
                                       logger = logger)
    app.dump(args.plates, args.fetch_all, args.vessels_collection, 
             args.ofile)

def do_register(registration_list):
    registration_list.append(('plate_data_samples', help_doc, make_parser,
                              implementation))
