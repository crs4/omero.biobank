import imp
import bl.lib.genotype.kb as kb


def get_kb_module(kb_module_name):
  fp, pathname, description = imp.find_module(kb_module_name)
  try:
    module = imp.load_module(kb_module_name, fp, pathname, description)
    return module
  except ImportError:
    raise kb.KBError("No such kb module: %s" % kb_module_name)
  finally:
    fp.close()
