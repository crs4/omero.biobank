# The tool changes the source of an object inside the system.
# Expected input file format is
#
# target      new_source
# V1415515    V1241441
# V1351124    V1511141
# .....
#
# Where target is the object whose source will be changed with the
# new_source object.  New source type will be specified using the
# command line option.

import logging, csv, argparse, sys, os, json, time

from bl.vl.kb import KnowledgeBase as KB
import bl.vl.kb.drivers.omero.utils as vlu
import omero
import omero.model

LOG_FORMAT = '%(asctime)s|%(levelname)-8s|%(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'
LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']


def make_parser():
    parser = argparse.ArgumentParser(description='change the source for given items')
    parser.add_argument('--logfile', type=str, help='log file (default=stderr)')
    parser.add_argument('--loglevel', type=str, choices=LOG_LEVELS,
                        help='logging level', default='INFO')
    parser.add_argument('-H', '--host', type=str, help='omero hostname')
    parser.add_argument('-U', '--user', type=str, help='omero user')
    parser.add_argument('-P', '--passwd', type=str, help='omero password')
    parser.add_argument('--operator', type=str, required=True,
                        help='operator username')
    parser.add_argument('--in_file', type=str, required=True,
                        help='list of items with new sources')
    parser.add_argument('--target_type', type=str, required=True,
                        help='type of the target objects')
    parser.add_argument('--source_type', type=str, required=True,
                        help='type of the new source objects')
    return parser


def do_check(records, targets, sources, 
             target_type, source_type,
             kb, logger):
    logger.info('Starting consistency checks')
    src_map = dict([(s.id, s) for s in sources])
    trg_map = dict([(t.id, t) for t in targets])
    good_records = []
    targets = {}
    sources = {}
    for i, r in enumerate(records):
        if r['target'] not in trg_map:
            logger.warning('No %s with ID %s, rejecting record %d' % (target_type,
                                                                      r['target'], i))
            continue
        if r['new_source'] not in src_map:
            logger.warning('No %s with ID %s, rejecting record %d' % (source_type,
                                                                      r['new_source'], i))
            continue
        targets[r['target']] = trg_map[r['target']]
        sources[r['new_source']] = src_map[r['new_source']]
        good_records.append(r)
    logger.info('Done with consistency checks')
    return good_records, targets, sources


def update_data(records, targets, sources, operator, act_conf,
                kb, logger, batch_size = 500):
    def get_chunk(batch_size, records):
        offset = 0
        while len(records[offset:]) > 0:
            yield records[offset:offset+batch_size]
            offset += batch_size
    dev = get_device(kb, logger)
    for i, recs in enumerate(get_chunk(batch_size, records)):
        logger.info('Updating batch %d' % i)
        batch_to_save = []
        for r in recs:
            target = targets[r['target']]
            # Build the ActionOnAction backup object
            if not target.lastUpdate:
                last_action = target.action
            else:
                last_action = target.lastUpdate
            old_action = target.action
            asconf = {'backup' : {'action' : old_action.id}}
            aslabel = 'updater.update_source_item-%f' % time.time()
            backup = build_action(operator, old_action.context,
                                  dev, last_action, aslabel,
                                  asconf, kb, logger)
            target.lastUpdate = backup
            # Build the Action in order to attach the new source to
            # the target object
            new_source = sources[r['new_source']]
            asconf = act_conf
            aslabel = 'updater.update_source_item-%f' % time.time()
            new_act = build_action(operator, old_action.context,
                                   dev, new_source, aslabel,
                                   asconf, kb, logger)
            target.action = new_act
            batch_to_save.append(target)
        kb.save_array(batch_to_save)


def build_action(operator, context, device, target,
                 action_setup_label, action_setup_conf,
                 kb, logger):
    if action_setup_label:
        asetup = get_action_setup(action_setup_label, action_setup_conf,
                                  kb, logger)
    else:
        asetup = None
    aconf = {
        'device' : device,
        'actionCategory' : kb.ActionCategory.IMPORT,
        'operator' : 'operator',
        'context' : context,
        'target' : target,
        }
    if asetup:
        aconf['setup'] = asetup
        action = kb.factory.create(retrieve_action_type(target, kb), aconf)
    return action


def retrieve_action_type(target, kb):
    tklass = target.ome_obj.__class__.__name__
    for i, k in enumerate(target.ome_obj.__class__.__mro__):
        if k is omero.model.IObject:
            tklass = target.ome_obj.__class__.__mro__[i-1].__name__
    if tklass == 'Vessel':
        return kb.ActionOnVessel
    elif tklass == 'Individual':
        return kb.ActionOnIndividual
    elif tklass == 'DataSample':
        return kb.ActionOnDataSample
    elif tklass == 'DataCollectionItem':
        return kb.ActionOnDataCollectionItem
    elif tklass == 'Action':
        return kb.ActionOnAction
    elif tklass == 'VLCollection':
        return kb.ActionOnCollection
    else:
        raise ValueError('No Action related to %s klass' % tklass)
            

def get_action_setup(label, conf, kb, logger):
    asetup_conf = {
        'label' : label,
        'conf' : json.dumps(conf),
        }
    asetup = kb.factory.create(kb.ActionSetup, asetup_conf)
    return asetup


def get_device(kb, logger):
    dev_model = 'UPDATE'
    dev_maker = 'CRS4'
    dev_release = '0.1'
    dev_label = 'updater-%s.update_source_item' % dev_release
    device = kb.get_device(dev_label)
    if not device:
        logger.debug('No device with label %s, creating one' % dev_label)
        conf = {
            'maker' : dev_maker,
            'model' : dev_model,
            'release' : dev_release,
            'label' : dev_label,
            }
        device = kb.factory.create(kb.Device, conf).save()
    return device


def find_action_setup_conf(args):
    action_setup_conf = {}
    for x in dir(args):
        if not (x.startswith('_') or x.startswith('func')):
            action_setup_conf[x] = getattr(args, x)
    if 'passwd' in action_setup_conf:
        action_setup_conf.pop('passwd') # Storing passwords into an
                                        # Omero obj is not a great idea...
    return action_setup_conf


def main(argv):
    parser = make_parser()
    args = parser.parse_args(argv)

    log_level = getattr(logging, args.loglevel)
    kwargs = {'format' : LOG_FORMAT,
              'datefmt' : LOG_DATEFMT,
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
    logging.info('Loading data from input file')
    with open(args.in_file) as f:
        reader = csv.DictReader(f, delimiter='\t')
        records = list(reader)
    logging.info('Loaded %d records' % len(records))

    logger.info('Loading %s type objects' % args.target_type)
    targets = kb.get_objects(getattr(kb, args.target_type))
    logger.info('Loaded %d objects' % len(targets))
    if len(targets) == 0:
        msg = 'No targets loaded from the system, nothing to do'
        logger.critical(msg)
        sys.exit(msg)

    logger.info('Loading %s type objects' % args.source_type)
    sources = kb.get_objects(getattr(kb, args.source_type))
    logger.info('Loaded %d objects' % len(sources))
    if len(sources) == 0:
        msg = 'No sources loaded from the system, nothing to do'
        logger.critical(msg)
        sys.exit(msg)

    logger.info('Loading Action type objects')
    acts = kb.get_objects(kb.Action)
    logger.info('Loaded %d objects' % len(acts))

    records, targets, sources = do_check(records, targets, sources,
                                         args.target_type, args.source_type,
                                         kb, logger)
    if len(records) == 0:
        msg = 'No records passed consistency checks, nothing to do'
        logger.critical(msg)
        sys.exit(msg)

    aconf = find_action_setup_conf(args)

    update_data(records, targets, sources, args.operator, 
                aconf, kb, logger)
        
    logger.info('Job completed')


if __name__ == '__main__':
    main(sys.argv[1:])
