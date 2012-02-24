import logging, csv, os, sys, argparse

from bl.vl.kb import KnowledgeBase as KB

def make_parser():
    parser = argparse.ArgumentParser(description='Retrieve all enrollments')
    parser.add_argument('--logfile', type=str, help='log file (default=stderr)')
    parser.add_argument('--loglevel', type=str, choices = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help='logging level', default='INFO')
    parser.add_argument('--host', type=str, help='omero hostname',
                        default='localhost')
    parser.add_argument('--user', type=str, help='omero user',
                        default='root')
    parser.add_argument('--passwd', type=str, help='omero password',
                        required=True)
    parser.add_argument('--ofile', type=str, help='output file path',
                        required=True)
    return parser

def main(argv):
    parser = make_parser()
    args = parser.parse_args(argv)

    # This is a temporary hack!!!
    to_be_ignored = ['IMMUNOCHIP_DISCARDED', 'CASI_MS_CSM_TMP']

    logformat = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    loglevel  = getattr(logging, args.loglevel)
    if args.logfile:
        logging.basicConfig(filename=args.logfile, format=logformat, level=loglevel)
    else:
        logging.basicConfig(format=logformat, level=loglevel)
        logger = logging.getLogger()
        
    try:
        out_file_path = args.ofile
    except IndexError:
        logger.error('Mandatory field missing.')
        parser.print_help()
        sys.exit(2)

    # Create the KnowledgeBase object
    kb = KB(driver='omero')(args.host, args.user, args.passwd)

    # Retrieve all studies from omero
    studies = kb.get_objects(kb.Study)
    studies = [s for s in studies if s.label not in to_be_ignored]
    logging.info('Retrieved %d studies from database' % len(studies))

    csv_header = ['individual_uuid']
    enrolls_map = {}
    # For each study, retrieve all enrollments
    for s in studies:
        logging.info('Retrieving enrollments for study %s' % s.label)
        enrolls = kb.get_enrolled(s)
        logging.info('%s enrollments retrieved' % len(enrolls))
        if len(enrolls) > 0:
            logging.debug('Building lookup dictionary....')
            csv_header.append(s.label) # Add study label to CSV header
            for e in enrolls:
                enrolls_map.setdefault(e.individual.omero_id, {})['individual_uuid'] = e.individual.id
                enrolls_map[e.individual.omero_id][s.label] = e.studyCode
        else:
            logging.debug('No enrollments found, skip study %s' % s.label)
            
    # Write to CSV file
    logging.debug('Writing CSV file %s' % out_file_path)
    with open(out_file_path, 'w') as f:
        writer = csv.DictWriter(f, csv_header,
                                delimiter='\t', quotechar='"',
                                restval = 'None')
        writer.writeheader()
        for k, v in enrolls_map.iteritems():
            writer.writerow(v)

if __name__ == '__main__':
    main(sys.argv[1:])
