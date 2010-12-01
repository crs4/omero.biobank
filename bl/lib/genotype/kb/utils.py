import imp


def KBError(Exception):
  pass


def get_kb_module(kb_module_name):
  fp, pathname, description = imp.find_module(kb_module_name)
  try:
    module = imp.load_module(kb_module_name, fp, pathname, description)
    return module
  except ImportError:
    raise KBError("No such kb module: %s" % kb_module_name)
  finally:
    fp.close()
