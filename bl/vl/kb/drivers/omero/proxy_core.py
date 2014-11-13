# BEGIN_COPYRIGHT
# END_COPYRIGHT

from bl.vl.utils import get_logger

import itertools as it
import numpy as np

import omero
from omero_version import omero_version
import omero.rtypes as ort
import omero_sys_ParametersI as osp
import omero_ServerErrors_ice  # magically adds exceptions to the omero module
import omero_Tables_ice
import omero_SharedResources_ice

import bl.vl.kb as kb
from bl.vl.utils.ome_utils import ome_hash

from wrapper import ome_wrap


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
  elif isinstance(o, omero.grid.FloatArrayColumn):
    return '(%d,)float32' % o.size
  elif isinstance(o, omero.grid.DoubleArrayColumn):
    return '(%d,)float64' % o.size
  elif isinstance(o, omero.grid.LongArrayColumn):
    return '(%d,)int64' % o.size

def dtype_to_ome_table_column(name, dtype):
  if dtype in [np.int32, np.int64]:
    return omero.grid.LongColumn(name, '')
  elif dtype in [np.float32, np.float64]:
    return omero.grid.DoubleColumn(name, '')
  elif dtype in [np.bool, np.bool8]:
    return omero.grid.BoolColumn(name, '')
  elif dtype.kind == 'S':
    return omero.grid.StringColumn(name, '', dtype.itemsize)
    

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
  """

  OME_TABLE_COLUMN = {
    'string': omero.grid.StringColumn,
    'long': omero.grid.LongColumn,
    'double': omero.grid.DoubleColumn,
    'bool': omero.grid.BoolColumn,
    'float_array': omero.grid.FloatArrayColumn,
    'double_array': omero.grid.DoubleArrayColumn,
    'long_array': omero.grid.LongArrayColumn,
    }
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

  def __check_omero_version(self):
    s = self.connect()
    conf = s.getConfigService()
    server_version = conf.getConfigValue('omero.version')
    client_version = omero_version
    self.disconnect()
    if server_version != client_version:
      raise kb.KBError(
        'OMERO client version %s doesn\'t match server version %s' %
        (client_version, server_version))

  def __init__(self, host, user, passwd, group=None, session_keep_tokens=1,
               check_ome_version=True):
    self.logger = get_logger('bl.vl.kb.drivers.omero.proxy_core')
    self.user = user
    self.passwd = passwd
    self.group_name = group
    self.client = omero.client(host)
    for h in self.logger.root.handlers:
      self.logger.root.removeHandler(h)
    self.session_keep_tokens = session_keep_tokens
    self.transaction_tokens = 0
    self.current_session = None
    if check_ome_version:
        self.__check_omero_version()
    self.context_managers = []

  def __del__(self):
    if self.current_session:
      self.client.closeSession()

  def push_context_manager(self, ctx_manager):
    self.context_managers.append(ctx_manager)

  def pop_context_manager(self):
    self.context_managers.pop()

  def change_group(self, group_name):
    if not self.current_session:
      self.connect()
    a = self.current_session.getAdminService()
    try:
      g = a.lookupGroup(group_name)
      self.current_session.setSecurityContext(g)
    except omero.ApiUsageException:
      raise kb.KBError('%s is not a valid group name' % group_name)
    except omero.SecurityViolation:
      raise kb.KBPermissionError('user %s is not a member of group %s' %
                                 (self.user, group_name))

  def change_to_user_default_group(self):
    if not self.current_session:
      self.connect()
    a = self.current_session.getAdminService()
    exp = a.lookupExperimenter(self.user)
    self.current_session.setSecurityContext(a.getDefaultGroup(exp.id._val))

  def change_to_session_default_group(self):
    if self.group_name:
      self.change_group(self.group_name)
    else:
      self.change_to_user_default_group()

  def get_current_group(self):
    """
    Return the group's name and the group's object related to the group currently
    connected to the user. If a connection is not opened yet, return None, None
    """
    if not self.current_session:
      return None, None
    else:
      a = self.current_session.getAdminService()
      ev_context = a.getEventContext()
      return ev_context.groupName, self._get_group(ev_context.groupName)

  def _get_group(self, group_name):
    if not self.current_session:
      raise kb.KBError('Connection to OMERO server is closed')
    else:
      a = self.current_session.getAdminService()
      try:
        return a.lookupGroup(group_name)
      except omero.ApiUsageException:
        raise kb.KBError('There is not group with name %s' % group_name)

  def _get_group_id(self, group_name):
    return self._get_group(group_name).id._val

  def is_group_leader(self, group_name=None):
    """
    Check if the current user is leader of the group labeled group_name, if no
    group_name is provided, check against the group in which the user is currently
    logged in
    """
    self.connect()
    a = self.current_session.getAdminService()
    ev_context = a.getEventContext()
    if not group_name:
      group_id = ev_context.groupId
    else:
      group_id = self._get_group_id(group_name)
    return group_id in ev_context.leaderOfGroups

  def is_member_of_group(self, group_name):
    self.connect()
    a = self.current_session.getAdminService()
    ev_context = a.getEventContext()
    group_id = self._get_group_id(group_name)
    return (group_id in ev_context.leaderOfGroups) or \
         (group_id in ev_context.memberOfGroups)

  def get_object_owner(self, obj):
    self.connect()
    a = self.current_session.getAdminService()
    return a.getExperimenter(obj.ome_obj.details.owner.id._val)._omeName._val

  def get_object_group(self, obj):
    self.connect()
    a = self.current_session.getAdminService()
    return a.getGroup(obj.ome_obj.details.group.id._val)._name._val

  def connect(self):
    if not self.current_session:
      self.current_session = self.client.createSession(self.user, self.passwd)
      self.transaction_tokens = self.session_keep_tokens
      if self.group_name:
        self.change_group(self.group_name)
    self.transaction_tokens -= 1
    return self.current_session

  def disconnect(self):
    if self.transaction_tokens <= 0:
      self.client.closeSession()
      self.current_session = None
      self.transaction_tokens = 0

  def start_keep_alive(self, timeout=300):
    self.client.enableKeepAlive(timeout)
    self.client.startKeepAlive()

  def stop_keep_alive(self):
    self.client.stopKeepAlive()

  def ome_query_params(self, conf):
    params = osp.ParametersI()
    for k in conf.keys():
      params.add(k, conf[k])
    return params

  def ome_operation(self, operation, action, *action_args):
    session = self.connect()
    # try:
    try:
      service = getattr(session, operation)()
    except AttributeError:
      raise kb.KBError("%r kb operation not supported" % operation)
    try:
      result = getattr(service, action)(*action_args)
    except AttributeError:
      raise kb.KBError("%r kb action not supported on operation %r" %
                       (action, operation))
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
    Save and return a KB object.
    """
    try:
      # check if we are saving a new object or if we are updating an
      # existing one
      obj_update = obj.is_mapped()
      result = self.ome_operation("getUpdateService", "saveAndReturnObject",
                                  obj.ome_obj)
    except omero.ValidationException, e:
      msg = 'omero.ValidationException: %s' % e.message
      self.logger.error(msg)
      self.logger.error('omero.ValidationException object: %s' % type(obj))
      raise kb.KBError(msg)
    obj.ome_obj = result
    self.store_to_cache(obj)
    obj.__dump_to_graph__(obj_update)
    if self.context_managers:
      self.context_managers[-1].register(obj)
    return obj

  def save_array(self, array):
    """
    Save and return an array of KB objects.
    """
    update = [obj.is_mapped() for obj in array]
    try:
      result = self.ome_operation("getUpdateService", "saveAndReturnArray",
                                  [obj.ome_obj for obj in array])
    except omero.ValidationException, e:
      msg = 'omero.ValidationException: %s' % e.message
      self.logger.error(msg)
      raise kb.KBError(msg)
    if len(result) != len(array):
      raise kb.KBError('bad return array len')
    for o, v, u in it.izip(array, result, update):
      o.ome_obj = v
      self.store_to_cache(o)
      o.__dump_to_graph__(u)
      if self.context_managers:
        self.context_managers[-1].register(o)
    return array

  def delete(self, kb_obj):
    """
    Delete a KB object.
    """
    kb_obj.__precleanup__()
    try:
      result = self.ome_operation("getUpdateService", "deleteObject",
                                  kb_obj.ome_obj)
    except omero.ValidationException:
      raise kb.KBError("object is referenced by one or more objects")
    except omero.ApiUsageException:
      raise kb.KBError("trying to delete non-persistent object")
    except omero.SecurityViolation:
      raise kb.KBError("deletion of the object not allowed")
    else:
      self.del_from_cache(kb_obj.ome_obj)
      kb_obj.__cleanup__()
      if self.context_managers:
        self.context_managers[-1].deregister(kb_obj)
    return result

  #----------------------------------------------------------------------------
  #----------------------------------------------------------------------------
  #-- TABLES SUPPORT
  #----------------------------------------------------------------------------
  #----------------------------------------------------------------------------

  def _list_table_copies(self, table_name):
    return self.ome_operation('getQueryService', 'findAllByString',
                              'OriginalFile', 'name', table_name, True, None)

  # def get_table(self, table_name):
  #   s = self.connect()
  #   # try:
  #   ofiles = self._list_table_copies(table_name)
  #   # finally:
  #   #   self.disconnect()
  #   if len(ofiles) != 1:
  #      raise kb.KBError('the requested %s table is missing' % table_name)
  #   r = s.sharedResources()
  #   t = r.openTable(ofile)
  #   return t
  #   if len(ofiles) != 1:
  #     raise ValueError('get_table: cannot resolve %s' % table_name)
  #   return

  def delete_table(self, table_name):
    """
    This method only removes the OriginalFile table entry from database.
    
    For actual file removal run, on the server:

    .. code-block:: bash

      ${OMERO_HOME}/bin/omero admin cleanse ${OMERO_DATA_DIR}
    """
    # try:
    self.connect()
    ofiles = self._list_table_copies(table_name)
    for o in ofiles:
      self.ome_operation('getUpdateService' , 'deleteObject', o)
    # finally:
    #   self.disconnect()

  def table_exists(self, table_name):
    # try:
    ofiles = self._list_table_copies(table_name)
    # finally:
      # self.disconnect()
    return len(ofiles) > 0

  def get_number_of_rows(self, table_name):
    "returns the number of rows of table table_name"
    session = self.connect()
    table = self._get_table(session, table_name)
    return table.getNumberOfRows()

  @staticmethod
  def _load_columns(table, records):
    columns = table.getHeaders()
    for c in columns:
      c.values = records[c.name].tolist()
    return columns
    
  def store_as_a_table(self, table_name, records, batch_size=10000):
    """
    Creates a new omero table called table_name and store in it the
    contents of records, a numpy records array.
    """
    if not hasattr(records, 'dtype') or records.dtype.type != np.void:
      raise ValueError('records is not a numpy records array')
    dtype = records.dtype
    fields = [dtype_to_ome_table_column(k, dtype.fields[k][0])
              for k in records.dtype.names]
    table = self._create_table(table_name, fields)
    offset = 0
    while offset < len(records):
      table.addData(self._load_columns(table, 
                                       records[offset: offset + batch_size]))
      offset += batch_size
    
  def read_whole_table(self, table_name, batch_size=10000):
    """
    Reads all data contained in the omero table called table_name and
    return result as a numpy records array.
    """
    session = self.connect()
    table = self._get_table(session, table_name)
    n_rows = table.getNumberOfRows()
    columns = table.getHeaders()
    dtype = [(c.name, convert_type(c)) for c in columns]
    records = np.zeros(n_rows, dtype=dtype)
    offset = 0
    while offset < n_rows:
      next_offset = offset + batch_size
      data = table.read(range(len(columns)), offset, next_offset)
      block = records[offset:next_offset]
      for c in data.columns:
        block[c.name] = c.values
      offset = next_offset
    return records
    
  def create_table(self, table_name, fields):
    ofields = [self.OME_TABLE_COLUMN[f[0]](*f[1:]) for f in fields]
    return self._create_table(table_name, ofields)
    
  def _create_table(self, table_name, fields):
    s = self.connect()
    # try:
    r = s.sharedResources()
    m = r.repositories()
    i = m.descriptions[0].id.val
    t = r.newTable(i, table_name)
    t.initialize(fields)
    # finally:
    #   self.disconnect()
    return t

  def _get_table(self, session, table_name):
    s = session
    qs = s.getQueryService()
    ofile = qs.findByString('OriginalFile', 'name', table_name, None)
    if not ofile:
      raise kb.KBError('the requested %s table is missing' % table_name)
    r = s.sharedResources()
    t = r.openTable(ofile)
    if not t:
      raise ValueError("failed to retrieve table '%s'" % table_name)
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
      # self.disconnect()  # this closes the connect() below
    if not self.current_session:
        self.connect()
    t = self._get_table(self.current_session, table_name)
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

  def get_table_rows(self, table_name, selector=None, col_names=None,
                     batch_size=BATCH_SIZE):
    """
    selector can be one of None, a selection or a list of selections. In
    the latter case, it is interpreted as an 'or' condition between
    the list elements.
    """
    s = self.connect()
    # try:
    t = self._get_table(s, table_name)
    col_numbers = self.__convert_col_names_to_indices(t, col_names)
    if selector is None:
      res = self.__get_table_rows_bulk(t, col_numbers, batch_size)      
    else:
      res = self.__get_table_rows_selected(t, selector, col_numbers,
                                           batch_size)
    # finally:
    #   self.disconnect()
    return res

  def get_table_rows_by_indices(self, table_name, indices=None, col_names=None,
                                batch_size=BATCH_SIZE):
    """
    indices must be either None or a list of integer values.
    """
    s = self.connect()
    # try:
    t = self._get_table(s, table_name)
    col_numbers = self.__convert_col_names_to_indices(t, col_names)
    if indices is None:
      res = self.__get_table_rows_bulk(t, col_numbers, batch_size)      
    else:
      res = self.__get_table_rows_by_indices(t, indices, col_numbers,
                                             batch_size)
    # finally:
    #   self.disconnect()
    return res

  def __get_table_rows_by_indices(self, table, row_indices, col_numbers,
                                  batch_size):
    d = table.slice(col_numbers, row_indices)
    res = [convert_coordinates_to_np(d)]
    return np.concatenate(tuple(res)) if res else []
    
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
      ids = row_numbers[row_read:(row_read+batch_size)]
      if ids:
        d = table.slice(col_numbers, ids)
        res.append(convert_coordinates_to_np(d))
      row_read += batch_size
    return np.concatenate(tuple(res)) if res else []

  def get_table_slice(self, table_name, row_numbers, col_names=None,
                      batch_size=BATCH_SIZE):
    s = self.connect()
    # try:
    t = self._get_table(s, table_name)
    col_numbers = self.__convert_col_names_to_indices(t, col_names)
    res = self.__get_table_rows_slice(t, row_numbers, col_numbers, batch_size)
    # finally:
    #   self.disconnect()
    return res
  
  def get_table_headers(self, table_name):
    col_objs = None
    s = self.connect()
    # try:
    t = self._get_table(s, table_name)
    col_objs = t.getHeaders()
    # finally:
    #   self.disconnect()
    if col_objs:
      return convert_to_numpy_record_type(col_objs)

  def add_table_row(self, table_name, row):
    if hasattr(row, 'dtype'):
      dtype = row.dtype
      row = dict([(k, convert_from_numpy(row[k])) for k in dtype.names])
    return self.add_table_rows_from_stream(table_name, iter([row]), 10)

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
    if not self.current_session:
        self.connect()
    indices = []
    # try:
    t = self._get_table(self.current_session, table_name)
    col_objs = t.getHeaders()
    batch = batch_loader(records_stream, col_objs, batch_size)
    # First index of the new batch of rows is the number of rows
    # already stored into the table
    first_index = t.getNumberOfRows()
    while batch:
      t.addData(batch)
      indices.extend(range(first_index, t.getNumberOfRows()))
      col_objs = t.getHeaders()
      batch = batch_loader(records_stream, col_objs, batch_size)
      first_index = t.getNumberOfRows()
    # finally:
    #   self.disconnect()
    return indices

  def __load_batch(self, records_stream, col_objs, chunk_size):
    v = {}
    for r in it.islice(records_stream, chunk_size):
      for k, x in r.iteritems():
        v.setdefault(k, []).append(x)
    if len(v) == 0:
      return None
    for o in col_objs:
      o.values = v[o.name]
    return col_objs

  def update_table_row(self, table_name, selector, row):
    if not self.current_session:
        self.connect()
    # try:
    t = self._get_table(self.current_session, table_name)
    idxs = t.getWhereList(selector, {}, 0, t.getNumberOfRows(), 1)
    self.logger.debug('\tselector %s results in %s' % (selector, idxs))
    if not len(idxs) == 1:
      raise ValueError('selector %s does not yield a single row' % selector)
    self.logger.debug('\tselected idx: %s' % idxs)
    data = t.readCoordinates(idxs)
    self.__update_data_contents(data, row)
    t.update(data)
    # finally:
    #   self.disconnect()

  def update_table_rows(self, table_name, selector, update_items):
    if not self.current_session:
        self.connect()
    # try:
    t = self._get_table(self.current_session, table_name)
    idxs = t.getWhereList(selector, {}, 0, t.getNumberOfRows(), 1)
    self.logger.debug('\tselector %s results in %s' % (selector, idxs))
    if len(idxs) == 0:
      self.logger.debug('\tno rows to update') 
      return
    data = t.readCoordinates(idxs)
    cols = [c.name for c in data.columns]
    for x in update_items.keys():
      if x not in cols:
        raise ValueError('%s is not a valid field for table %s' % (x, table_name))
    for dc in data.columns:
      if dc.name in update_items.keys():
        for x in range(0, len(dc.values)):
          self.logger.debug(
            '\tcolumn :%s  -> setting value to %s (old value %s)' % 
            (dc.name, update_items[dc.name], dc.values[x]))
          dc.values[x] = update_items[dc.name]
    self.logger.debug('\trecords have been modified')
    t.update(data)
    self.logger.debug('\tdata update complete')
    # finally:
    #   self.disconnect()

  def __update_data_contents(self, data, row):
    assert len(data.rowNumbers) == 1
    if hasattr(row, 'dtype'):
      dtype = row.dtype
      row = dict([(k, convert_from_numpy(row[k])) for k in dtype.names])
    for o in data.columns:
      if row.has_key(o.name):
        o.values[0] = row[o.name]
