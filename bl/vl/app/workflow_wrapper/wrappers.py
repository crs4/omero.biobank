from galaxy_wrapper import GalaxyWrapper
import logging

LOG_FORMAT = '%(asctime)s|%(levelname)-8s|%(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'

def get_logger(log_format, log_date_format, log_level,
               filename = None):
    loglevel = getattr(logging, log_level)
    kwargs = {'format'  : log_format,
              'datefmt' : log_date_format,
              'level'   : loglevel}
    if filename:
        kwargs['filename'] = filename
    logging.basicConfig(**kwargs)
    logger = logging.getLogger()
    return logger


def run_datasets_import(history, items, action_context, no_dataobjects=False,
                        async=False, driver='galaxy', conf=None,
                        delete_history=False, purge_history=False,
                        logger=None):
    if not logger:
        logger = get_logger(LOG_FORMAT, LOG_DATEFMT, 'INFO')
    # Consistency check for items
    if len(items) == 0:
        msg = 'Empty list, nothing to import'
        logger.error(msg)
        raise ValueError(msg)
    base_type = type(items[0])
    for i in items[1:]:
        if type(i) != base_type:
            msg = 'Found object with type :%s, expected type is %s' % (type(i),
                                                                       base_type)
            logger.error(msg)
            raise ValueError(msg)
    if driver == 'galaxy':
        gw = GalaxyWrapper(conf, logger)
        history_details = gw.run_datasets_import(history, items, action_context,
                                                 no_dataobjects, async)
        if delete_history and not async:
            gw.delete_history(history_details['history'], purge_history)
    else:
        msg = 'Driver %s not supported' % driver
        logger.error(msg)
        raise RuntimeError(msg)


def run_flowcell_from_samplesheet_import(samplesheet_data, action_context,
                                         namespace = None, async = False,
                                         driver = 'galaxy', conf = None,
                                         delete_history = False, purge_history = False,
                                         logger = None):
    if not logger:
        logger = get_logger(LOG_FORMAT, LOG_DATEFMT, 'INFO')
    if driver == 'galaxy':
        gw = GalaxyWrapper(conf, logger)
        history_details = gw.run_flowcell_from_samplesheet_import(samplesheet_data, action_context,
                                                                  namespace, async)
        if delete_history and not async:
            gw.delete_history(history_details['history'], purge_history)
    else:
        msg = 'Driver %s not supported' % driver
        logger.error(msg)
        raise RuntimeError(msg)
