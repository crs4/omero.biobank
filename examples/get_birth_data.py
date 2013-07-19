# Expected input file should be like
#
# individual_id	                        enrollments
# V0DF5CAECE4ABA43EA96F089DF72159426	CASI_T1D:3342.1
# V02B7F863CD41244A5A3F4123107680A51	CASI_T1D:554.4
# V097F57BA9AC864DAD8A35649AAD8FE7A2	CONTROLLI:5759.1,GWAS:1450,IMMUNOCHIP:000000002389|A0933XN6

import csv, sys, argparse
from datetime import datetime

from bl.vl.utils import LOG_LEVELS, get_logger
from bl.vl.kb import KnowledgeBase as KB
import bl.vl.utils.ome_utils as vlu


def make_parser():
    parser = argparse.ArgumentParser(description='retrieves birth informations using enrollments IDs as lookup')
    parser.add_argument('--logfile', type=str, help='log file (default=stderr)')
    parser.add_argument('--loglevel', type=str, choices=LOG_LEVELS,
                        help='logging level', default='INFO')
    parser.add_argument('-H', '--host', type=str, help='omero hostname')
    parser.add_argument('-U', '--user', type=str, help='omero user')
    parser.add_argument('-P', '--passwd', type=str, help='omero password')
    parser.add_argument('--in_file', type=str, required=True,
                        help='input file with enrollments lookup')
    parser.add_argument('--match_files', type=str, required=True,
                        help='output file for good matches')
    parser.add_argument('--no_matches_file', type=str,
                        help='output file with a list of non matching individuals')
    return parser

def format_date(date_obj, date_fmt = '%d/%m/%Y'):
    return date_obj.strftime(date_fmt)

def get_enrollments_lookup(kb, logger):
    logger.info('Loading Enrollments')
    enrolls = kb.get_objects(kb.Enrollment)
    lookup_tb = {}
    for en in enrolls:
        lookup_tb['%s:%s' % (en.study.label, en.studyCode)] = en
    logger.info('Loaded %d Enrollments' % len(lookup_tb))
    return lookup_tb

def main(argv):
    parser = make_parser()
    args = parser.parse_args(argv)
    logger = get_logger("main", level=args.loglevel, filename=args.logfile)

    try:
        host = args.host or vlu.ome_host()
        user = args.user or vlu.ome_user()
        passwd = args.passwd or vlu.ome_passwd()
    except ValueError, ve:
        logger.critical(ve)
        sys.exit(ve)

    kb = KB(driver='omero')(host, user, passwd)
    
    logger.info('Loading Demographic data')
    demogs = kb.get_objects(kb.Demographic)
    logger.info('Loaded %d items' % len(demogs))

    logger.info('Loading Individuals')
    inds = kb.get_objects(kb.Individual)
    logger.info('Loaded %d individuals' % len(inds))

    demogs_lookup = {}
    for d in demogs:
        demogs_lookup[d.individual] = d

    enrolls_map = get_enrollments_lookup(kb, logger)

    in_file = open(args.in_file)
    reader = csv.DictReader(in_file, delimiter='\t')
    bd_matches_file = open(args.match_files, 'w')
    bd_writer = csv.DictWriter(bd_matches_file, ['individual', 'birth_date',
                                                 'birth_place_name', 'birth_place_code',
                                                 'birth_place_district'],
                               delimiter='\t', restval='')
    bd_writer.writeheader()
    if args.no_matches_file:
        no_bd_matches_file = open(args.no_matches_file, 'w')
        no_bd_writer = csv.DictWriter(no_bd_matches_file, reader.fieldnames, delimiter='\t')
        no_bd_writer.writeheader()

    for row in reader:
        enrolls = row['enrollments'].split(',')
        for en in enrolls:
            try:
                ind = enrolls_map[en].individual
                bdate = format_date(datetime.fromtimestamp(demogs_lookup[ind].birthDate))
                bplace = demogs_lookup[ind].birthPlace
                if bdate != '01/01/9999':
                    record = {'individual' : en,
                              'birth_date' : bdate}
                    if bplace.name != 'UNKNOWN':
                        record['birth_place_name'] = bplace.name
                        record['birth_place_code'] = bplace.istatCode
                        if bplace.OME_TABLE == 'City':
                            record['birth_place_district'] = bplace.districtLabel
                    bd_writer.writerow(record)
                    break
            except KeyError:
                logger.debug('No match for enrollment %s' % en)
        else:
            if 'no_bd_writer' in locals():
                no_bd_writer.writerow(row)

    in_file.close()
    bd_matches_file.close()
    if 'no_bd_matches_file' in locals():
        no_bd_matches_file.close()
    logger.info('Job completed')
    

if __name__ == '__main__':
    main(sys.argv[1:])
