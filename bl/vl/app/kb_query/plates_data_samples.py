import csv, argparse

from bl.vl.app.importer.core import Core

class BuildPlateDataSamplesDetails(Core):
    def __init__(self, host = None, user = None, passwd = None, keep_tokens = 1,
                 logger = None, study_label = None, operator = 'Alfred E. Neumann'):
        super(BuildPlateDataSamplesDetails, self).__init__(host, user, passwd,
                                                           keep_tokens = keep_tokens,
                                                           study_label = study_label,
                                                           logger = logger)

    def load_plate(self, plate_barcode):
        query = 'SELECT pl FROM TiterPlate pl WHERE pl.barcode = :pl_barcode'
        plate = self.kb.find_all_by_query(query, {'pl_barcode' : plate_barcode})
        return plate[0] if len(plate) > 0 else None

    def get_connected_data_samples(self, well):
        query = '''SELECT ds FROM DataSample ds
                   JOIN ds.action action WHERE
                   action.id IN (SELECT act.id FROM ActionOnVessel act
                                 JOIN act.target tg
                                 WHERE tg.vid = :target_id)
                '''
        return self.kb.find_all_by_query(query, {'target_id' : well.id})

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

    def dump(self, plate_barcode, out_file):
        self.logger.info('Loading plate %s' % plate_barcode)
        plate = self.load_plate(plate_barcode)
        if not plate:
            msg = 'Barcode %s is not related to a known plate' % plate_barcode
            self.logger.critical(msg)
            raise ValueError(msg)

        self.logger.info('Loading wells for plate %s' % plate.barcode)
        wells = list(self.kb.get_wells_by_plate(plate))
        self.logger.info('Loaded %d wells' % len(wells))
        wells_map = {}
        for w in wells:
            wells_map[w.slot] = w

        writer = csv.DictWriter(out_file, delimiter='\t', restval = 'X',
                                fieldnames = ['PLATE_barcode', 'PLATE_label',
                                              'WELL_label', 'WELL_status',
                                              'DATA_SAMPLE_label'])
        writer.writeheader()
        last_slot = 0
        for slot, well in sorted(wells_map.iteritems()):
            self.logger.debug('WELL %s --- SLOT %d' % (well.label, slot))
            while(last_slot < slot - 1):
                last_slot += 1
                self.logger.debug('No data for well %s, filling with a dummy record' %
                                  self.calculate_well_label(last_slot, plate.columns))
                writer.writerow(self.get_empty_record(plate, last_slot))
            record = {'PLATE_barcode' : plate.barcode,
                      'PLATE_label' : plate.label,
                      'WELL_label' : well.label,
                      'WELL_status' : well.status.enum_label()}
            dsamples = self.get_connected_data_samples(well)
            self.logger.info('Retrieved %d data samples for well %s' % (len(dsamples),
                                                                        well.label))
            if len(dsamples) > 0:
                record['DATA_SAMPLE_label'] = dsamples[0].label
            writer.writerow(record)
            last_slot = slot
        # Fill empty records if last_slot != plate.rows * plate.columns
        while (last_slot != (plate.rows * plate.columns)):
            last_slot += 1
            self.logger.debug('No data for well %s, filling with a dummy record' %
                              self.calculate_well_label(last_slot, plate.columns))
            writer.writerow(self.get_empty_record(plate, last_slot))

        out_file.close()
        self.logger.info('Job completed')

help_doc = """
Write the status of a TiterPlate, retrieving PlateWell and DataSamples
connected to them.
"""

def make_parser(parser):
    parser.add_argument('-p', '--plate', type=str, required=True,
                        help = 'plate\'s barcode')

def implementation(logger, host, user, passwd, args):
    app = BuildPlateDataSamplesDetails(host = host, user = user,
                                       passwd = passwd, study_label = None,
                                       keep_tokens = args.keep_tokens,
                                       logger = logger)
    app.dump(args.plate, args.ofile)

def do_register(registration_list):
    registration_list.append(('plate_data_samples', help_doc, make_parser,
                              implementation))
