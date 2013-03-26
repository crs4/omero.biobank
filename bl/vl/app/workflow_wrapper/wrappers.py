from galaxy_wrapper import GalaxyWrapper

def run_datasets_import(history, items, action_context, async = False,
                        driver = 'galaxy', conf_file_path = None):
    # Consistency check for items
    if len(items) == 0:
        raise ValueError('Empty list, nothing to import')
    base_type = type(items[0])
    for i in items[1:]:
        if type(i) != base_type:
            raise ValueError('Found object with type :%s, expected type is %s' % (type(i),
                                                                                  base_type))
    if driver == 'galaxy':
        gw = GalaxyWrapper(conf_file_path)
        gw.run_datasets_import(history, items, action_context, async)
    else:
        raise RuntimeError('Driver %s not supported' % driver)


def run_flowcell_from_samplesheet_import(samplesheet_data, action_context,
                                         namespace = None, async = False,
                                         driver = 'galaxy', conf_file_path = None):
    if driver == 'galaxy':
        gw = GalaxyWrapper(conf_file_path)
        gw.run_flowcell_from_samplesheet_import(samplesheet_data, action_context,
                                                namespace, async)
    else:
        raise RuntimeError('Driver %s not supported' % driver)
