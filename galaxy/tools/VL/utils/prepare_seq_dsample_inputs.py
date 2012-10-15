# This tool produces files that can be used as input to import
# * samples
# * flowcells
# * lanes
# * laneslots
# within OMERO.biobang using import applications.
# If the optional 'study-output-file' parameter is given as input, the
# script will produce the input file for a new study definition.

import csv, sys, argparse, logging

LOG_FORMAT = '%(asctime)s|%(levelname)-8s|%(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'
LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

def make_parser():
    parser = argparse.ArgumentParser(description='split sequencing samplesheet')
    parser.add_argument('--logfile', type=str, help='log file (default=stderr)')
    parser.add_argument('--loglevel', type=str, choices=LOG_LEVELS,
                        help='logging level', default='INFO')
    parser.add_argument('--in-file', '-i', type=str, required=True,
                        help='input file')
    parser.add_argument('--tubes-out-file', type=str,
                        help='output file containing tube definitions',
                        default='./tubes_def.tsv')
    parser.add_argument('--flowcells-out-file', type=str,
                        help='output file containing flowcell definitions',
                        default='./flowcells_def.tsv')
    parser.add_argument('--lanes-out-file', type=str,
                        help='output file containing lane definitions',
                        default='./lanes_def.tsv')
    parser.add_argument('--laneslots-out-file', type=str,
                        help='output file containing laneslot definitions',
                        default='./laneslots_def.tsv')
    parser.add_argument('--study-label', type=str, required=True,
                        help='study label')
    parser.add_argument('--study-output-file', type=str,
                        help='output file containing study definition')
    return parser


def get_samplesheet_translator(samplesheet_type='default'):
    translator = {'default' : {'flowcell_id' : 'FCID',
                               'tube_id'     : 'SampleID',
                               'lane_id'     : 'Lane',
                               'sample_tag'  : 'Index',
                               'protocol'    : 'Recipe',
                               'operator'    : 'Operator'}
                  }
    return translator[samplesheet_type]

def write_tubes_file(records, study_label, translator, ofile, logger=None):
    ofile_fields = ['study', 'label', 'vessel_type', 'vessel_content',
                    'vessel_status', 'source', 'source_type']
    with open(ofile, 'w') as out_file:
        writer = csv.DictWriter(out_file, ofile_fields, delimiter='\t')
        writer.writeheader()
        tubes_def = set([r[translator['tube_id'].strip()] for r in records])
        for x in tubes_def:
            writer.writerow({'study'          : study_label,
                             'label'          : x,
                             'vessel_type'    : 'Tube',
                             'vessel_content' : 'DNA',
                             'vessel_status'  : 'UNKNOWN',
                             'source'         : 'None',
                             'source_type'    : 'NO_SOURCE'})

def write_flowcells_file(records, study_label, translator, ofile, logger=None):
    ofile_fields = ['study', 'label', 'barcode', 'container_status',
                    'number_of_slots', 'options']
    with open(ofile, 'w') as out_file:
        writer = csv.DictWriter(out_file, ofile_fields, delimiter='\t')
        writer.writeheader()
        flowcells_def = set([r[translator['flowcell_id'].strip()] for r in records])
        for x in flowcells_def:
            writer.writerow({'study'            : study_label,
                             'label'            : x,
                             'barcode'          : x,
                             'container_status' : 'INSTOCK',
                             'number_of_slots'  : '8',
                             'options'          : 'protocol=%s,operator=%s' % (r[translator['protocol']],
                                                                               r[translator['operator']])})


def write_lanes_file(records, study_label, translator, ofile, logger=None):
    ofile_fields = ['study', 'flow_cell', 'slot', 'container_status',
                    'options']
    with open(ofile, 'w') as out_file:
        writer = csv.DictWriter(out_file, ofile_fields, delimiter='\t')
        writer.writeheader()
        lanes_def = set([(r[translator['flowcell_id']].strip(),
                          r[translator['lane_id'].strip()]) 
                         for r in records])
        for x in lanes_def:
            writer.writerow({'study'            : study_label,
                             'flow_cell'        : x[0],
                             'slot'             : x[1],
                             'container_status' : 'INSTOCK',
                             'options'          : 'protocol=%s,operator=%s' % (r[translator['protocol']],
                                                                               r[translator['operator']])})
        

def write_laneslots_file(records, study_label, translator, ofile, logger=None):
    ofile_fields = ['study', 'lane', 'tag', 'content', 'source',
                    'source_type', 'options']
    with open(ofile, 'w') as out_file:
        writer = csv.DictWriter(out_file, ofile_fields, delimiter='\t')
        writer.writeheader()
        laneslots_def = set([('%s:%s' % (r[translator['flowcell_id']].strip(),
                                         r[translator['lane_id']].strip()),
                              r[translator['sample_tag']].strip(),
                              r[translator['tube_id']].strip()) for r in records])
        for x in laneslots_def:
            writer.writerow({'study'       : study_label,
                             'lane'        : x[0],
                             'tag'         : x[1],
                             'content'     : 'DNA',
                             'source'      : x[2],
                             'source_type' : 'Tube',
                             'options'     : 'protocol=%s,operator=%s' % (r[translator['protocol']],
                                                                          r[translator['operator']])})

def write_study_file(study_label, ofile, logger=None):
    ofile_fields = ['label']
    with open(ofile, 'w') as out_file:
        writer = csv.DictWriter(out_file, ofile_fields, delimiter='\t')
        writer.writeheader()
        writer.writerow({'label' : study_label})


def main(argv):
    parser = make_parser()
    args = parser.parse_args(argv)

    log_level = getattr(logging, args.loglevel)
    kwargs = {'format'  : LOG_FORMAT,
              'datefmt' : LOG_DATEFMT,
              'level'   : log_level}
    if args.logfile:
        kwargs['filename'] = args.logfile
    logging.basicConfig(**kwargs)
    logger = logging.getLogger('prepare_seq_dsample_inputs')

    with open(args.in_file) as f:
        logger.info('Loading data from file %s' % args.in_file)
        reader = csv.DictReader(f, delimiter='\t')
        recs = [r for r in reader]
    translator = get_samplesheet_translator()

    if args.study_output_file:
        logger.info('Writing Study definition file %s' % args.study_output_file)
        write_study_file(args.study_label, args.study_output_file, logger)
        logger.info('Done writing file %s' % args.study_output_file)

    logger.info('Writing Tube definitions file %s' % args.tubes_out_file)
    write_tubes_file(recs, args.study_label, translator,
                     args.tubes_out_file, logger)
    logger.info('Done writing file %s' % args.tubes_out_file)

    logger.info('Writing FlowCell definitions file %s' % args.flowcells_out_file)
    write_flowcells_file(recs, args.study_label, translator,
                         args.flowcells_out_file, logger)
    logger.info('Done writing file %s' % args.flowcells_out_file)

    logger.info('Writing Lane definitions file %s' % args.lanes_out_file)
    write_lanes_file(recs, args.study_label, translator,
                     args.lanes_out_file, logger)
    logger.info('Done writing file %s' % args.lanes_out_file)

    logger.info('Writing LaneSlot definitions file %s' % args.laneslots_out_file)
    write_laneslots_file(recs, args.study_label, translator,
                         args.laneslots_out_file, logger)
    logger.info('Done writing file %s' % args.laneslots_out_file)


if __name__ == '__main__':
    main(sys.argv[1:])
