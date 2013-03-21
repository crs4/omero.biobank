from galaxy_wrapper import GalaxyWrapper

def run_datasets_import(history, items, action_context,
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
        gw.run_datasets_import(history, items, action_context)
    else:
        raise RuntimeError('Drivers %s not supported' % driver)
