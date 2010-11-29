import drivers


def KBError(Exception):
  pass


def get_kb_class(kb_class):
  try:
    return getattr(drivers, kb_class)
  except AttributeError:
    raise KBError("No such kb class: %s" % kb_class)
