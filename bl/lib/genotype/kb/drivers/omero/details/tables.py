import omero
import omero_Tables_ice
import omero_SharedResources_ice
import bl.lib.genotype.kb as kb
import numpy as np

def get_table(session, table_name, logger):
  s = session
  qs = s.getQueryService()
  ofile = qs.findByString('OriginalFile', 'name', table_name, None)
  if not ofile:
    logger.error('the requested %s table is missing' % table_name)
    raise kb.KBError('the requested %s table is missing' % table_name)
  r = s.sharedResources()
  t = r.openTable(ofile)
  return t

def convert_to_np(d):
  def convert_type(o):
    if isinstance(o, omero.grid.LongColumn):
      return 'i8'
    elif isinstance(o, omero.grid.DoubleColumn):
      return 'f8'
    elif isinstance(o, omero.grid.BoolColumn):
      return 'b'
    elif isinstance(o, omero.grid.StringColumn):
      return '|S%d' % o.size
  record_type = [(c.name, convert_type(c)) for c in d.columns]
  npd = np.zeros(len(d.columns[0].values), dtype=record_type)
  for c in d.columns:
    npd[c.name] = c.values
  return npd


