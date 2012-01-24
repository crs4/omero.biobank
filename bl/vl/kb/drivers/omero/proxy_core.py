import time, logging
logger = logging.getLogger("proxy_core")
import itertools as it
import numpy as np

import omero
import omero.rtypes as ort
import omero_sys_ParametersI as osp
import omero_ServerErrors_ice  # magically adds exceptions to the omero module
import omero_Tables_ice
import omero_SharedResources_ice

import bl.vl.kb as kb
from wrapper import ome_wrap
from utils import ome_hash


BATCH_SIZE = 5000


def convert_type(o):
  if isinstance(o, omero.grid.LongColumn):
    return 'i8'
  elif isinstance(o, omero.grid.DoubleColumn):
    return 'f8'
  elif isinstance(o, omero.grid.BoolColumn):
    return 'b'
  elif isinstance(o, omero.grid.StringColumn):
    return '|S%d' % o.size


def convert_coordinates_to_np(d):
  record_type = [(c.name, convert_type(c)) for c in d.columns]
  npd = np.zeros(len(d.columns[0].values), dtype=record_type)
  for c in d.columns:
    npd[c.name] = c.values
  return npd


def convert_to_numpy_record_type(d):
  return [(c.name, convert_type(c)) for c in d]


def convert_from_numpy(x):
  if isinstance(x, np.int64):
    return int(x)
  else:
    return x


class ProxyCore(object):
  """
  A knowledge base implemented as a driver for OMERO.

  NOTE: keeping an open session leads to bad performance, because the
  Java garbage collector is called automatically and
  unpredictably. You cannot force garbage collection on an open
  session unless you are using Java. For this reason, we open a new
  session for each new operation on the database and close it when we
  are done, forcing the server to release the allocated memory.

  FIXME: in the future, low-level omero access should be provided by a
  common set of core libraries.
  """

  OME_TABLE_COLUMN = {'string' : omero.grid.StringColumn,
                      'long'   : omero.grid.LongColumn,
                      'double' : omero.grid.DoubleColumn,
                      'bool'   : omero.grid.BoolColumn,}

  _CACHE = {}

  def store_to_cache(self, obj):
    self.__class__._CACHE[ome_hash(obj.ome_obj)] = obj

  def del_from_cache(self, ome_obj):
    try:
      del self.__class__._CACHE[ome_hash(ome_obj)]
    except KeyError:
      pass

  def get_from_cache(self, ome_obj):
    return self._CACHE.get(ome_hash(ome_obj))

  def clear_cache(self):
    self.__class__._CACHE.clear()

  def __init__(self, host, user, passwd, group=None, session_keep_tokens=1):
    self.user = user
    self.passwd = passwd
    self.group_name = group
    self.client = omero.client(host)
    self.session_keep_tokens = session_keep_tokens
    self.transaction_tokens = 0
    self.current_session = None
    self.logger = logger

  def __del__(self):
    if self.current_session:
      self.client.closeSession()

  def change_group(self, group_name):
    self.group_name = group_name
    self.transaction_tokens = 0
    self.disconnect()

  def connect(self):
    if not self.current_session:
      self.current_session = self.client.createSession(self.user, self.passwd)
      self.transaction_tokens = self.session_keep_tokens
      if self.group_name:
        a = self.current_session.getAdminService()
        try:
          g = a.lookupGroup(self.group_name)
          self.current_session.setSecurityContext(g)
        except omero.ApiUsageException, aue:
          raise ValueError(aue.message)
    self.transaction_tokens -= 1
    return self.current_session

  def disconnect(self):
    if self.transaction_tokens <= 0:
      self.client.closeSession()
      self.current_session = None
      self.transaction_tokens = 0

  def ome_query_params(self, conf):
    params = osp.ParametersI()
    for k in conf.keys():
      params.add(k, conf[k])
    return params

  def ome_operation(self, operation, action, *action_args):
    session = self.connect()
    try:
      try:
        service = getattr(session, operation)()
      except AttributeError:
        raise kb.KBError("%r kb operation not supported" % operation)
      try:
        result = getattr(service, action)(*action_args)
      except AttributeError:
        raise kb.KBError("%r kb action not supported on operation %r" %
                         (action, operation))
    finally:
      self.disconnect()
    return result

  def find_all_by_query(self, query, params, factory):
    if params:
      xpars = {}
      for k,v in params.iteritems():
        xpars[k] = ome_wrap(*v) if type(v) == tuple else ome_wrap(v)
      pars = self.ome_query_params(xpars)
    else:
      pars = None
    result = self.ome_operation("getQueryService", "findAllByQuery",
                                query, pars)
    return [] if result is None else [factory.wrap(r) for r in result]

  def update_by_example(self, o):
    res = self.ome_operation('getQueryService', 'findByExample', o.ome_obj)
    if not res:
      raise ValueError('cannot update %s by example'  % o)
    o.ome_obj = res
    o.proxy = self

  # FIXME this is a hack
  def reload_object(self, o, fields=None):
    def load_ome_obj(ome_obj):
      tbl = ome_obj.__class__.__name__[:-1]
      res = self.ome_operation("getQueryService", "get",
                               tbl, ome_obj.id.val)
      if not res:
        raise ValueError('cannot load ome_obj %s'  % ome_obj)
      return res
    res = load_ome_obj(o.ome_obj)
    if fields:
      for f in fields:
        x = load_ome_obj(getattr(res, f))
        setattr(res, f, x)
    o.ome_obj = res
    o.proxy = self

  def save(self, obj):
    """
    Save and return a kb object.
    """
    try:
      result = self.ome_operation("getUpdateService", "saveAndReturnObject",
                                  obj.ome_obj)
    except omero.ValidationException, e:
      msg = 'omero.ValidationException: %s' % e.message
      self.logger.error(msg)
      self.logger.error('omero.ValidationException object: %s' % type(obj))
      raise kb.KBError(msg)
    obj.ome_obj = result
    self.store_to_cache(obj)
    return obj

  def save_array(self, array):
    """
    Save and return an array of kb objects.
    """
    try:
      result = self.ome_operation("getUpdateService", "saveAndReturnArray",
                                  [obj.ome_obj for obj in array])
    except omero.ValidationException, e:
      msg = 'omero.ValidationException: %s' % e.message
      self.logger.error(msg)
      raise kb.KBError(msg)
    if len(result) != len(array):
      raise kb.KBError('bad return array len')
    for o, v in it.izip(array, result):
      o.ome_obj = v
      self.store_to_cache(o)
    return array

  def delete(self, kb_obj):
    """
    Delete a kb object.
    """
    try:
      result = self.ome_operation("getUpdateService", "deleteObject",
                                  kb_obj.ome_obj)
    except omero.ValidationException:
      raise kb.KBError("object is referenced by one or more objects")
    except omero.ApiUsageException, e:
      raise kb.KBError("trying to delete non-persistent object")
    else:
      self.del_from_cache(kb_obj.ome_obj)
    return result

  #----------------------------------------------------------------------------
  #----------------------------------------------------------------------------
  #-- TABLES SUPPORT
  #----------------------------------------------------------------------------
  #----------------------------------------------------------------------------

  def _list_table_copies(self, table_name):
    return self.ome_operation('getQueryService',
                              'findAllByString', 'OriginalFile',
                              'name', table_name, True, None)

  def get_table(self, table_name):
    s = self.connect()
    try:
      ofiles = self._list_table_copies(table_name)
    finally:
      self.disconnect()
    if len(ofiles) != 1:
       raise kb.KBError('the requested %s table is missing' % table_name)
    r = s.sharedResources()
    t = r.openTable(ofile)
    return t
    if len(ofiles) != 1:
      raise ValueError('get_table: cannot resolve %s' % table_name)
    return

  def delete_table(self, table_name):
    """
    This method only removes OriginalFile table entry from database.
    For actual file removal run on the server:
    $OMERO_HOME/bin/omero admin cleanse $OMERO_DATA_DIR
    """
    try:
      self.connect()
      ofiles = self._list_table_copies(table_name)
      for o in ofiles:
        self.ome_operation('getUpdateService' , 'deleteObject', o)
    finally:
      self.disconnect()

  def table_exists(self, table_name):
    try:
      ofiles = self._list_table_copies(table_name)
    finally:
      self.disconnect()
    return len(ofiles) > 0

  def create_table(self, table_name, fields):
    ofields = [self.OME_TABLE_COLUMN[f[0]](*f[1:]) for f in fields]
    s = self.connect()
    try:
      r = s.sharedResources()
      m = r.repositories()
      i = m.descriptions[0].id.val
      t = r.newTable(i, table_name)
      t.initialize(ofields)
    finally:
      self.disconnect()
    return t

  def _get_table(self, session, table_name):
    s = session
    qs = s.getQueryService()
    ofile = qs.findByString('OriginalFile', 'name', table_name, None)
    if not ofile:
      raise kb.KBError('the requested %s table is missing' % table_name)
    r = s.sharedResources()
    t = r.openTable(ofile)
    return t

  def get_table_rows_iterator(self, table_name, batch_size=100):
    # TODO add error checking
    def iter_on_rows(t, n_cols):
      i, N = 0, t.getNumberOfRows()
      while i < N:
        j = min(N, i + batch_size)
        v = t.read(range(n_cols), i, j)
        Z = convert_coordinates_to_np(v)
        for k in range(j - i):
          yield Z[k]
        i = j
      self.disconnect()  # this closes the connect() below
    s = self.connect()
    t = self._get_table(s, table_name)
    col_objs = t.getHeaders()
    return iter_on_rows(t, len(col_objs))

  def __convert_col_names_to_indices(self, table, col_names):
    col_objs = table.getHeaders()
    if col_names:
      col_numbers = []
      by_name = dict(((c.name, i) for i, c in enumerate(col_objs)))
      for name in col_names:
        if name in by_name:
          col_numbers.append(by_name[name])
        else:
          raise ValueError('%s not in table' % name)
    else:
      col_numbers = range(len(col_objs))
    return col_numbers

  def get_table_rows(self, table_name, selector, col_names=None,
                     batch_size=BATCH_SIZE):
    """
    selector can be either a selection or a list of selections. In
    the latter case, it is interpreted as an 'or' condition between
    the list elements.
    """
    s = self.connect()
    try:
      t = self._get_table(s, table_name)
      col_numbers = self.__convert_col_names_to_indices(t, col_names)
      if selector:
        res = self.__get_table_rows_selected(t, selector, col_numbers,
                                             batch_size)
      else:
        res = self.__get_table_rows_bulk(t, col_numbers, batch_size)
    finally:
      self.disconnect()
    return res

  def __get_table_rows_selected(self, table, selector, col_numbers, batch_size):
    res, row_read, max_row = [], 0, table.getNumberOfRows()
    if isinstance(selector, str):
      selector = [selector]
    while row_read < max_row:
      for s in selector:
        ids = table.getWhereList(s, {}, row_read, row_read + batch_size, 1)
        if ids:
          d = table.slice(col_numbers, ids)
          res.append(convert_coordinates_to_np(d))
      row_read += batch_size
    return np.concatenate(tuple(res)) if res else []

  def __get_table_rows_bulk(self, table, col_numbers, batch_size=BATCH_SIZE):
    res, row_read, max_row = [], 0, table.getNumberOfRows()
    while row_read < max_row:
      d = table.read(col_numbers, row_read, row_read + batch_size)
      if d:
        res.append(convert_coordinates_to_np(d))
      row_read += batch_size
    return np.concatenate(tuple(res)) if res else []

  def __get_table_rows_slice(self, table, row_numbers, col_numbers, batch_size):
    res, n_rows, row_read = [], len(row_numbers), 0
    while row_read < n_rows:
      ids = row_numbers[row_read : (row_read + batch_size)]
      if ids:
        d = table.slice(col_numbers, ids)
        res.append(convert_coordinates_to_np(d))
      row_read += batch_size
    return np.concatenate(tuple(res)) if res else []

  def get_table_slice(self, table_name, row_numbers, col_names=None,
                      batch_size=BATCH_SIZE):
    s = self.connect()
    try:
      t = self._get_table(s, table_name)
      col_numbers = self.__convert_col_names_to_indices(t, col_names)
      res = self.__get_table_rows_slice(t, row_numbers, col_numbers, batch_size)
    finally:
      self.disconnect()
    return res
  
  def get_table_headers(self, table_name):
    col_objs = None
    s = self.connect()
    try:
      t = self._get_table(s, table_name)
      col_objs = t.getHeaders()
    finally:
      self.disconnect()
    if col_objs:
      return convert_to_numpy_record_type(col_objs)

  def add_table_row(self, table_name, row):
    if hasattr(row, 'dtype'):
      dtype = row.dtype
      row = dict([(k, convert_from_numpy(row[k])) for k in dtype.names])
    def stream(row):
      for i in range(1):
        yield row
    return self.add_table_rows_from_stream(table_name, stream(row), 10)

  def add_table_rows(self, table_name, rows, batch_size=BATCH_SIZE):
    dtype = rows.dtype
    def stream(rows):
      for r in rows:
        yield dict([(k, convert_from_numpy(r[k])) for k in dtype.names])
    return self.add_table_rows_from_stream(table_name, stream(rows),
                                           batch_size=batch_size)

  def add_table_rows_from_stream(self, table_name, stream,
                                 batch_size=BATCH_SIZE):
    return self.__extend_table(table_name, self.__load_batch, stream,
                               batch_size=batch_size)

  def __extend_table(self, table_name, batch_loader, records_stream,
                     batch_size=BATCH_SIZE):
    s = self.connect()
    try:
      t = self._get_table(s, table_name)
      col_objs = t.getHeaders()
      batch = batch_loader(records_stream, col_objs, batch_size)
      while batch:
        t.addData(batch)
        col_objs = t.getHeaders()
        batch = batch_loader(records_stream, col_objs, batch_size)
    finally:
      self.disconnect()

  def __load_batch(self, records_stream, col_objs, chunk_size):
    v = {}
    for r in it.islice(records_stream, chunk_size):
      for k in r.keys():
        v.setdefault(k,[]).append(r[k])
    if len(v) == 0:
      return None
    for o in col_objs:
      o.values = v[o.name]
    return col_objs

  def update_table_row(self, table_name, selector, row):
    s = self.connect()
    try:
      t = self._get_table(s, table_name)
      idxs = t.getWhereList(selector, {}, 0, t.getNumberOfRows(), 1)
      self.logger.debug('\tselector %s results in %s' % (selector, idxs))
      if not len(idxs) == 1:
        raise ValueError('selector %s does not yield a single row' % selector)
      self.logger.debug('\tselected idx: %s' % idxs)
      data = t.readCoordinates(idxs)
      self.logger.debug('\tdata read: %s' % data)
      self.__update_data_contents(data, row)
      self.logger.debug('\tupdated data: %s' % data)
      t.update(data)
    finally:
      self.disconnect()

  def __update_data_contents(self, data, row):
    assert len(data.rowNumbers) == 1
    if hasattr(row, 'dtype'):
      dtype = row.dtype
      row = dict([(k, convert_from_numpy(row[k])) for k in dtype.names])
    for o in data.columns:
      if row.has_key(o.name):
        o.values[0] = row[o.name]
