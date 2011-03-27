import omero
import omero_Tables_ice
import omero_SharedResources_ice

import vl.lib.utils as vlu
import bl.lib.sample.kb as kb

import numpy as np

import logging

logger = logging.getLogger('create_and_destroy')

# Right now, the only way to 'delete' an omeroTable is to remove the OriginalFile entry
# from the database, the file containing the table in not delete from the file system
def delete_table(server, user, passwd, file_name):
  logger.debug('starting deleting op')
  c = omero.client(server)
  s = c.createSession(user, passwd)
  try:
    qs = s.getQueryService()
    ofiles = qs.findAllByString('OriginalFile', 'name', file_name, True, None)
    us = s.getUpdateService()
    for o in ofiles:
      us.deleteObject(o)
  finally:
    c.closeSession()
  logger.debug('done with deleting op')

def does_table_exists(server, user, passwd, file_name):
  logger.debug('starting check on table %s' % file_name)
  c = omero.client(server)
  s = c.createSession(user, passwd)
  try:
    qs = s.getQueryService()
    ofiles = qs.findAllByString('OriginalFile', 'name', file_name, True, None)
  finally:
    c.closeSession()
  logger.debug('done with check on %s' % file_name)
  return len(ofiles) > 0

def create_table(server, user, passwd, file_name, fields):
  logger.debug('starting creation %s %s %s %s' % (server, user, passwd, file_name))
  c = omero.client(server)
  s = c.createSession(user, passwd)
  try:
    r = s.sharedResources()
    m = r.repositories()
    i = m.descriptions[0].id.val
    t = r.newTable(i, file_name)
    t.initialize(fields)
  finally:
    c.closeSession()
  logger.debug('done with creation %s %s %s %s' % (server, user, passwd, file_name))
  return t

def create_snp_definition_table(server, user, passwd, file_name):
  vid      = omero.grid.StringColumn('vid', 'This marker VID', len(vlu.make_vid()), None)
  source   = omero.grid.StringColumn('source', 'Origin of this marker definition.', 16, None)
  context  = omero.grid.StringColumn('context', 'Context of definition.', 16, None)
  label    = omero.grid.StringColumn('label', 'Label of marker in the definition context.', 16, None)
  rs_label = omero.grid.StringColumn('rs_label', 'dbSNP_id if available', 16, None)
  mask     = omero.grid.StringColumn('mask', 'SNP definition mask in the format <FLANK>[A/B]<FLANK>', 69, None)
  op_vid   = omero.grid.StringColumn('op_vid', 'Last operation that modified this row', len(vlu.make_vid()), None)
  t = create_table(server, user, passwd, file_name, [vid, source, context, label, rs_label, mask, op_vid])
  return t

def create_snp_alignment_table(server, user, passwd, file_name):
  marker_vid = omero.grid.StringColumn('marker_vid', 'VID of the aligned marker.', len(vlu.make_vid()), None)
  ref_genome = omero.grid.StringColumn('ref_genome', 'Reference alignment genome.', 16, None)
  chromosome = omero.grid.LongColumn('chromosome',
                                     'Chromosome where this alignment was found. 1-22, 23(X) 24(Y)', None)
  pos        = omero.grid.LongColumn('pos', "Position on the chromosome. Starting from 5'", None)
  global_pos = omero.grid.LongColumn('global_pos', "Global position in the genome. (chr*10**10 + pos)", None)
  strand     = omero.grid.BoolColumn('strand', 'Aligned on reference strand', None)
  # I know that this is in principle a bool, but what happens if we have more than two alleles?
  allele     = omero.grid.StringColumn('allele', 'Allele found at this position (A/B)', 1, None)
  copies     = omero.grid.LongColumn('copies', "Number of copies found for this marker within this alignment op.", None)
  op_vid   = omero.grid.StringColumn('op_vid', 'Last operation that modified this row', len(vlu.make_vid()), None)
  return create_table(server, user, passwd, file_name,
                      [marker_vid, ref_genome, chromosome, pos, global_pos, strand, allele, copies, op_vid])

def create_snp_set_table(server, user, passwd, file_name):
  vid        = omero.grid.StringColumn('vid', 'Set VID', len(vlu.make_vid()), None)
  marker_vid = omero.grid.StringColumn('marker_vid', 'Marker VID', len(vlu.make_vid()), None)
  marker_indx = omero.grid.LongColumn('marker_indx', "Ordered position of this marker within the set", None)
  allele_flip = omero.grid.BoolColumn('allele_flip', 'Is this technology flipping our A/B allele convention?',
                                        None)
  op_vid   = omero.grid.StringColumn('op_vid', 'Last operation that modified this row', len(vlu.make_vid()), None)
  return create_table(server, user, passwd, file_name,[vid, marker_vid, marker_indx, allele_flip, op_vid])

def create_snp_set_def_table(server, user, passwd, file_name):
  vid    = omero.grid.StringColumn('vid', 'Set VID', len(vlu.make_vid()), None)
  maker  = omero.grid.StringColumn('maker', 'Maker identifier.', 32, None)
  model  = omero.grid.StringColumn('model', 'Model identifier.', 32, None)
  op_vid = omero.grid.StringColumn('op_vid', 'Last operation that modified this row',
                                   len(vlu.make_vid()), None)
  return create_table(server, user, passwd, file_name, [vid, maker, model, op_vid])

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



