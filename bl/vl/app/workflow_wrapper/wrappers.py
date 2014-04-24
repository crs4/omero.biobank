from bl.vl.utils import get_logger
from galaxy_wrapper import GalaxyWrapper


def run_datasets_import(history, items, action_context, no_dataobjects=False,
                        async=False, driver='galaxy', conf=None,
                        delete_history=False, purge_history=False,
                        logger=None):
    if not logger:
        logger = get_logger("dataset_import", level='INFO')
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
        logger.info('Start import of dataset')
        history_details, library_id = gw.run_datasets_import(history, items, action_context,
                                                             no_dataobjects, async)
        logger.info('Import completed. HISTORY_ID: %s --- LIBRARY_ID: %s', history_details['history'],
                    library_id)
        logger.info('DELETE_HISTORY: %s --- ASYNC: %s', delete_history, async)
        if delete_history and not async:
            logger.info('Deleting history and library')
            gw.delete_history(history_details['history'], purge_history)
            gw.delete_library(library_id)
            logger.info('Deletion completed')
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
        logger = get_logger("flowcell_import", level='INFO')
    if driver == 'galaxy':
        gw = GalaxyWrapper(conf, logger)
        logger.info('Start import of flowcell')
        history_details, library_id = gw.run_flowcell_from_samplesheet_import(samplesheet_data, action_context,
                                                                              namespace, async)
        logger.info('Import completed. HISTORY_ID: %s --- LIBRARY_ID: %s', history_details['history'],
                    library_id)
        logger.info('DELETE_HISTORY: %s --- ASYNC: %s', delete_history, async)
        if delete_history and not async:
            logger.info('Deleting history and library')
            gw.delete_history(history_details['history'], purge_history)
            gw.delete_library(library_id)
            logger.info('Deletion completed')
    else:
        msg = 'Driver %s not supported' % driver
        logger.error(msg)
        raise RuntimeError(msg)
