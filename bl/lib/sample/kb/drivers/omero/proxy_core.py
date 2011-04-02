import omero
import omero.rtypes as ort
import omero_sys_ParametersI as osp
import omero_ServerErrors_ice  # magically adds exceptions to the omero module

import bl.lib.sample.kb as kb


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
      print dir(obj)
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
