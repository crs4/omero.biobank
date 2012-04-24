import logging, csv, argparse, sys, os

from bl.vl.kb import KnowledgeBase as KB
import bl.vl.kb.drivers.omero.utils as vlu

LOG_FORMAT = '%(asctime)s|%(levelname)-8s|%(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'
LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

COLLECTION_TYPES = {'VesselsCollection' : 'VesselsCollectionItem',
                    'DataCollection'    : 'DataCollectionItem'}

def make_parser():
    parser = argparse.ArgumentParser(description='remove elements from a Vessels or Data Collection')
    parser.add_argument('--logfile', type=str, help='log file (default=stderr)')
    parser.add_argument('--loglevel', type=str, choices=LOG_LEVELS,
                        help='logging level', default='INFO')
    parser.add_argument('-H', '--host', type=str, help='omero hostname')
    parser.add_argument('-U', '--user', type=str, help='omero user')
    parser.add_argument('-P', '--passwd', type=str, help='omero password')
    parser.add_argument('-I', '--ifile', type=str, required=True,
                        help='list of collection items that will be removed')
    parser.add_argument('--collection_type', type=str, required=True,
                        choices=COLLECTION_TYPES.keys(),
                        help='type of the collection')
    parser.add_argument('--collection_label', type=str, required=True,
                        help='label of the collection')

    return parser

def load_collection(coll_type, coll_label, kb):
    query = 'SELECT coll FROM %s coll WHERE coll.label = :coll_label' % coll_type
    coll = kb.find_all_by_query(query, {'coll_label' : coll_label})
    return coll[0] if len(coll) > 0 else None

def load_collection_items(collection, coll_type, kb):
    if COLLECTION_TYPES[coll_type] == 'VesselsCollectionItem':
        citems = kb.get_vessels_collection_items(collection)
    elif COLLECTION_TYPES[coll_type] == 'DataCollectionItem':
        citems =  kb.get_data_collection_items(collection)
    else:
        raise ValueError('Unknown data collection type %s' % COLLECTION_TYPES[coll_type])
    ci_map = {}
    for ci in citems:
        ci_map[ci.id] = ci
    return ci_map


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
    logger = logging.getLogger()

    try:
        host   = args.host or vlu.ome_host()
        user   = args.user or vlu.ome_user()
        passwd = args.passwd or vlu.ome_passwd()
    except ValueError, ve:
        logger.critical(ve)
        sys.exit(ve)

    kb = KB(driver='omero')(host, user, passwd)
    logging.info('Loading collection %s from %s' % (args.collection_label,
                                                    args.collection_type))
    coll = load_collection(args.collection_type, args.collection_label, kb)
    if not coll:
        msg = 'No %s found with label %s' % (args.collection_type,
                                             args.collection_label)
        logger.error(msg)
        sys.exit(msg)
    logging.info('Loading items from collection')
    coll_items = load_collection_items(coll, args.collection_type, kb)
    logging.info('Fetched %d elements' % len(coll_items))

    with open(args.ifile) as infile:
        reader = csv.DictReader(infile, delimiter='\t')
        to_be_deleted = [row['collection_item'] for row in reader]
    logger.info('Found %d items to be deleted' % len(to_be_deleted))

    for tbd in to_be_deleted:
        try:
            kb.delete(coll_items[tbd])
            logger.info('%s with ID %s deleted' % (COLLECTION_TYPES[args.collection_type],
                                                   tbd))
        except KeyError, ke:
            logger.warning('No %s related to ID %s' % (COLLECTION_TYPES[args.collection_type],
                                                       ke))
    logger.info('Job completed')


if __name__ == '__main__':
    main(sys.argv[1:])
