import omero
import omero_Tables_ice
import omero_SharedResources_ice
import bl.lib.genotype.kb as kb
import vl.lib.utils as vlu
import numpy as np


def create_snp_gdo_repository_table(session, file_name, N, logger):
  logger.debug('starting creation %s %s' % (file_name, N))
  s = session
  r = s.sharedResources()
  m = r.repositories()
  i = m.descriptions[0].id.val
  t = r.newTable(i, file_name)
  vid      = omero.grid.StringColumn('vid', 'gdo VID', len(vlu.make_vid()), None)
  op_vid   = omero.grid.StringColumn('op_vid', 'Last operation that modified this row',
                                     len(vlu.make_vid()), None)
  probs    = omero.grid.StringColumn('probs',
                                     'np.zeros((2,N), dtype=np.float32).tostring()',
                                     2*N*4, None)
  confidence = omero.grid.StringColumn('confidence',
                                       'np.zeros((N,), dtype=np.float32).tostring()',
                                       N*4, None)
  t.initialize([vid, op_vid, probs, confidence])
  logger.debug('done with creation %s %s' % (file_name, N))
  return t


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


