import sys, csv, argparse, logging, json, time

from bl.vl.kb import KnowledgeBase as KB

LOG_FORMAT = '%(asctime)s|%(levelname)-8s|%(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'
LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

def make_parser():
    parser = argparse.ArgumentParser(description='set parents of the selected individuals to None')
    parser.add_argument('--logfile', type=str, help='log file (default=stderr)')
    parser.add_argument('--loglevel', type=str, choices=LOG_LEVELS,
                        help='logging level (default=INFO)', default='INFO')
    parser.add_argument('-H', '--host', type=str, help='omero hostname',
                        default='localhost')
    parser.add_argument('-U', '--user', type=str, help='omero user',
                        default='root')
    parser.add_argument('-P', '--passwd', type=str, required=True,
                        help='omero password')
    parser.add_argument('-O', '--operator', type=str, required=True,
                        help='action operator')
    parser.add_argument('--inds_list', type=str, required=True,
                        help='list of the individuals')
    return parser

def drop_parents(individual, operator, kb):
    logger = logging.getLogger()
    backup = {}
    if individual.father:
        logger.info('Removing father (ID %s) for individual %s' % (individual.father.id,
                                                                   individual.id))
        backup['father'] = individual.father.id
        individual.father = None
    if individual.mother:
        logger.info('Removing mother (ID %s) for individual %s' % (individual.mother.id,
                                                                   individual.id))
        backup['mother'] = individual.mother.id
        individual.mother = None
    if len(backup.items()) > 0:
        update_object(individual, backup, operator, kb)
        return individual
    else:
        logger.warning('No update needed for individual %s' % individual.id)
        return None

def build_action_setup(label, backup, kb):
    logger = logging.getLogger()
    logger.debug('Creating a new ActionSetup with label %s and backup %r' % (label,
                                                                             backup))
    conf = {
        'label': label,
        'conf': json.dumps({'backup' : backup})
        }
    asetup = kb.factory.create(kb.ActionSetup, conf)
    return asetup

def update_object(obj, backup_values, operator, kb):
    logger = logging.getLogger()
    logger.debug('Building ActionOnAction for object %s' % obj.id)
    act_setup = build_action_setup('drop-parents-%f' % time.time(),
                                   backup_values, kb)
    aoa_conf = {
        'setup': act_setup,
        'actionCategory': kb.ActionCategory.UPDATE,
        'operator': operator,
        'target': obj.lastUpdate if obj.lastUpdate else obj.action,
        'context': obj.action.context
        }
    logger.debug('Updating object with new ActionOnAction')
    obj.lastUpdate = kb.factory.create(kb.ActionOnAction, aoa_conf)


def main(argv):
    parser = make_parser()
    args = parser.parse_args(argv)

    log_level = getattr(logging, args.loglevel)
    kwargs = {'format': LOG_FORMAT,
              'datefmt': LOG_DATEFMT,
              'level': log_level}
    if args.logfile:
        kwargs['filename'] = args.logfile
    logging.basicConfig(**kwargs)
    logger = logging.getLogger()

    kb = KB(driver='omero')(args.host, args.user, args.passwd)

    logger.info('Retrieving individuals')
    inds = kb.get_objects(kb.Individual)
    logger.info('Retrieved %d individuals' % len(inds))
    inds_lookup = {}
    for i in inds:
        inds_lookup[i.id] = i

    with open(args.inds_list) as in_file:
        to_be_updated = []
        reader = csv.DictReader(in_file, delimiter='\t')
        for row in reader:
            try:
                ind = drop_parents(inds_lookup[row['individual']], args.operator, kb)
                if ind:
                    to_be_updated.append(ind)
            except KeyError:
                logger.error('%s is not a valid individual id' % row['individual'])
    
    logger.debug('Updating %d individuals' % len(to_be_updated))
    kb.save_array(to_be_updated)

if __name__ == '__main__':
    main(sys.argv[1:])
