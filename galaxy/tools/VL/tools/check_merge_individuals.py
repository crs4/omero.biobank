import sys, csv, argparse, logging, os
from collections import Counter

from bl.vl.kb import KnowledgeBase as KB
import bl.vl.kb.drivers.omero.utils as vlu

LOG_FORMAT = '%(asctime)s|%(levelname)-8s|%(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'
LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

def make_parser():
    parser = argparse.ArgumentParser(description='check data that will be passed to the merge_individuals tool')
    parser.add_argument('--logfile', type=str, help='log file (default=stderr)')
    parser.add_argument('--loglevel', type=str, choices=LOG_LEVELS,
                        help='logging level (default=INFO)', default='INFO')
    parser.add_argument('-H', '--host', type=str, help='omero hostname')
    parser.add_argument('-U', '--user', type=str, help='omero user')
    parser.add_argument('-P', '--passwd', type=str, help='omero password')
    parser.add_argument('--in_file', type=str, required=True,
                        help='input file')
    parser.add_argument('--out_file', type=str, required=True,
                        help='output file')
    return parser

def get_invalid_vids(records):
    logger = logging.getLogger()

    records_map = {}
    invalid_vids = []

    for rec in records:
        for k,v in rec.iteritems():
            records_map.setdefault(k, []).append(v)
    # Check for duplicated sources
    ct = Counter()
    for x in records_map['source']:
        ct[x] += 1
    for k, v in ct.iteritems():
        if v > 1:
            logger.error('ID %s appears %d times as source, this ID has been marked as invalid' % (k, v))
            invalid_vids.append(k)
    # Check for VIDs that appear bots in 'source' and 'target' fields
    sources = set(records_map['source'])
    targets = set(records_map['target'])
    commons = sources.intersection(targets)
    for c in commons:
        logger.error('ID %s appears both in \'source\' and \'target\' columns, this ID has been marked as invalid' % c)
        invalid_vids.append(c)
        
    return set(invalid_vids)

def check_row(row, individuals):
    logger = logging.getLogger()
    try:
        source = individuals[row['source']]
        logger.debug('%s is a valid Individual ID' % source.id)
        target = individuals[row['target']]
        logger.debug('%s is a valid Individual ID' % target.id)
        return True
    except KeyError, ke:
        logger.error('%s is not a valid Individual ID' % ke)
        return False
        

def main(argv):
    parser = make_parser()
    args = parser.parse_args(argv)

    log_level = getattr(logging, args.loglevel)
    kwargs = {'format': LOG_FORMAT,
              'datefmt': LOG_DATEFMT,
              'level' : log_level}
    if args.logfile:
        kwargs['filename'] = args.logfile
    logging.basicConfig(**kwargs)
    logger = logging.getLogger()

    try:
        host = args.host or vlu.ome_host()
        user = args.user or vlu.ome_user()
        passwd = args.passwd or vlu.ome_passwd()
    except ValueError, ve:
        logger.critical(ve)
        sys.exit(ve)

    kb = KB(driver='omero')(host, user, passwd)
    
    logger.info('Preloading all individuals')
    inds = kb.get_objects(kb.Individual)
    logger.info('Loaded %d individuals' % len(inds))
    inds_map = {}
    for i in inds:
        inds_map[i.id] = i

    with open(args.in_file) as infile, open(args.out_file, 'w') as outfile:
        reader = csv.DictReader(infile, delimiter='\t')
        records = [row for row in reader]
        invalid_vids = get_invalid_vids(records)
        
        writer = csv.DictWriter(outfile, reader.fieldnames, delimiter='\t')
        writer.writeheader()

        for record in records:
            if record['source'] in invalid_vids or record['target'] in invalid_vids:
                logger.error('Skipping record %r because at least one ID was marked as invalid' % record)
            else:
                if check_row(record, inds_map):
                    writer.writerow(record)
                    logger.debug('Record %r written in output file' % record)
                    

if __name__ == '__main__':
    main(sys.argv[1:])
