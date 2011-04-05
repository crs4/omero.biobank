import omero
import omero.rtypes as ort
import omero_sys_ParametersI as osp
import omero_ServerErrors_ice  # magically adds exceptions to the omero module

import omero_Tables_ice
import omero_SharedResources_ice

import bl.lib.sample.kb as kb

import numpy as np

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

class ProxyCore(object):
  """
  A knowledge base implemented as a driver for OMERO.

  NOTE: keeping an open session leads to bad performances, because the
  Java garbage collector is called automatically and
  unpredictably. You cannot force garbage collection on an open
  session unless you are using Java. For this reason, we open a new
  session for each new operation on the database and close it when we
  are done, forcing the server to release the allocated memory.

  FIXME: in the future, low-level omero access should be provided by a
  common set of core libraries.
  """
  WRAPPING = {'timestamp' : ort.rtime,
              'string'    : ort.rstring,
              'float'     : ort.rfloat,
              'int'       : ort.rint,
              'boolean'   : ort.rbool}

  OME_TABLE_COLUMN = {'string' : omero.grid.StringColumn,
                      'long'   : omero.grid.LongColumn,
                      }

  def __init__(self, host, user, passwd):
    self.user = user
    self.passwd = passwd
    self.client = omero.client(host)

  def connect(self):
    return self.client.createSession(self.user, self.passwd)

  def disconnect(self):
    self.client.closeSession()

  def ome_wrap(self, v, wtype=None):
    return self.WRAPPING[wtype](v) if wtype else ort.wrap(v)

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

  def save(self, obj):
    """
    Save and return a kb object.
    """
    try:
      result = self.ome_operation("getUpdateService", "saveAndReturnObject",
                                  obj.ome_obj)
    except omero.ValidationException, e:
      print 'omero.ValidationException: %s' % e.message
      print type(obj)
      obj.__handle_validation_errors__()
    return obj.__class__(result)

  def delete(self, kb_obj):
    """
    Delete a kb object.
    """
    try:
      result = self.ome_operation("getUpdateService", "deleteObject",
                                    kb_obj.ome_obj)
    except omero.ApiUsageException:
      raise kb.KBError("trying to delete non-persistent object")
    except omero.ValidationException:
      raise kb.KBError("object does not exist")
    return result

  #-- TABLES SUPPORT

  def _list_table_copies(self, file_name):
    return self.ome_operation('getQueryService',
                              'findAllByString', 'OriginalFile',
                              'name', file_name, True, None)

  def get_table(self, file_name):
    try:
      ofiles = self._list_table_copies(file_name)
    finally:
      self.disconnect()

    if len(ofile) != 1:
      raise kb.KBError('the requested %s table is missing' % table_name)
    r = s.sharedResources()
    t = r.openTable(ofile)
    return t

    if len(ofiles) != 1:
      raise ValueError('get_table: cannot resolve %s' % file_name)
    return

  def delete_table(self, file_name):
    """
    FIXME: Actual file removal is left to something else...
    """
    try:
      ofiles = self._list_table_copies(file_name)
      for o in ofiles:
        self.ome_operation('getUpdateService' , 'deleteObject', o)
    finally:
      self.disconnect()

  def table_exists(self, file_name):
    try:
      ofiles = self._list_table_copies(file_name)
    finally:
      self.disconnect()
    return len(ofiles) > 0

  def create_table(self, file_name, fields):
    ofields = [self.OME_TABLE_COLUMN[f[0]](*f[1:]) for f in fields]
    s = self.connect()
    try:
      r = s.sharedResources()
      m = r.repositories()
      i = m.descriptions[0].id.val
      t = r.newTable(i, file_name)
      t.initialize(fields)
    finally:
      self.disconnect()
    return t

