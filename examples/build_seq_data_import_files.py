"""
Prepare all the files to create the omero objects to collect sequencing data
The objects chain is:
tube->laneslot->lane->flowcell->seqoutput->datasample->dataobjects

Information are retrieved from Omero and iRODS

Will be used a generic_BGI_scanner device and will be created a single SeqOutput
All the dataSample will linked to it.

"""

import argparse
import csv
import sys

from bl.vl.kb import KnowledgeBase as KB
from bl.vl.utils import LOG_LEVELS, get_logger

def make_parser():
    parser = argparse.ArgumentParser(description='Prepare import files')
    parser.add_argument('--logfile', type=str, help='log file (default=stderr)')
    parser.add_argument('--loglevel', type=str, help='logger level',
                        choices=LOG_LEVELS, default='INFO')
    parser.add_argument('--study', type=str, help='Study label', required=True)
    parser.add_argument('-H', '--host', type=str, help='Omero hostname')
    parser.add_argument('-U', '--user', type=str, help='Omero user')
    parser.add_argument('-P', '--passwd', type=str, help='Omero password')
    parser.add_argument('--tubes_file', type=str, help='Tubes import file',
                        required=True)
    return parser


def get_inds_enrolled_into_a_study(kb, study):
    enrolls = kb.get_enrolled(kb.get_by_label(kb.Study, study))
    return [{'studyCode': x.studyCode, 'individual_id': x.individual.id}
            for x in enrolls]


def write_csv(logger, filename, csv_header, records_map):
    logger.debug('Writing CSV file %s' % filename)
    with open(filename, 'w') as f:
        writer = csv.DictWriter(f, csv_header,
                                delimiter='\t', quotechar='"',
                                restval='None')
        writer.writeheader()
        for k, v in records_map.iteritems():
            writer.writerow(v)
    return


def generate_tubes_map(study_label, inds_map):
    prefix = "_".join([study_label, 'tube'])
    i = 0
    tubes_map = {}
    for _  in inds_map:
        i += 1
        tubes_map[i] = {'study'         : study_label,
                        'label'         : "_".join([prefix, "{0:04d}".format(i)]),
                        'vessel_type'   : 'Tube',
                        'vessel_content': 'DNA',
                        'vessel_status' : 'UNKNOWN',
                        'source'        : _['individual_id'],
                        'source_type'   : 'Individual'})
    return tubes_map


def main(argv):
    parser = make_parser()
    args = parser.parse_args(argv)
    logger = get_logger('prepare_imprt_files', level=args.loglevel,
                        filename=args.logfile)

    kb = KB(driver='omero')(args.host, args.user, args.passwd)
    study = args.study

    # create tubes import files
    tubes_file = args.tubes_file
    tubes_map = generate_tubes_map(study, get_inds_enrolled_into_a_study(kb,
                                                                         study))
    tubes_header = ['label', 'source']
    write_csv(logger, tubes_file, tubes_header, tubes_map)



if __name__ == '__main__':
    main(sys.argv[1:])
