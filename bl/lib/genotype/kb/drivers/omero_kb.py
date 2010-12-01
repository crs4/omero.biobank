import time

import omero
import omero.model as om
import omero.rtypes as ort
import omero_sys_ParametersI as op
import omero_ServerErrors_ice  # magically adds exceptions to the omero module

import vl.lib.utils as vl_utils

import bl.lib.genotype.kb as kb


class OmeroWrapper(object):

  OME_TABLE = None

  @classmethod
  def get_ome_type(klass):
    return getattr(om, "%sI" % klass.OME_TABLE)
  
  def __init__(self, ome_obj):
    super(OmeroWrapper, self).__setattr__("ome_obj", ome_obj)

  def __getattr__(self, name):
    return ort.unwrap(getattr(self.ome_obj, name))

  # WARNING: the 'wrap' function performs only basic type
  # conversions. Override this when more sophisticated conversions are
  # required (e.g., timestamps or computed results)
  def __setattr__(self, name, value):
    return setattr(self.ome_obj, name, ort.wrap(value))

  @property
  def id(self):
    return self.vid


class Study(OmeroWrapper, kb.Study):

  OME_TABLE = "Study"

  def __init__(self, from_=None):
    ome_type = Study.get_ome_type()
    if isinstance(from_, ome_type):
      ome_study = from_
    else:
      label = from_
      ome_study = ome_type()
      ome_study.vid = ort.rstring(vl_utils.make_vid())
      if label is not None:
        ome_study.label = ort.rstring(label)
      ome_study.startDate = vl_utils.time2rtime(time.time())
    super(Study, self).__init__(ome_study)


class Proxy(kb.Proxy):
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
  def __init__(self, host, user, passwd):
    self.user = user
    self.passwd = passwd
    self.client = omero.client(host)

  def __connect(self):
    return self.client.createSession(self.user, self.passwd)

  def __disconnect(self):
    self.client.closeSession()

  def __ome_operation(self, operation, action, *action_args):
    session = self.__connect()
    try:
      service = getattr(session, operation)()
    except AttributeError:
      raise kb.KBError("%r kb operation not supported" % operation)
    try:
      result = getattr(service, action)(*action_args)
    except AttributeError:
      raise kb.KBError("%r kb action not supported on operation %r" %
                       (action, operation))    
    self.__disconnect()
    return result

  def get_study_by_label(self, value):
    """
    Return the study object labeled 'value' or None if nothing matches 'value'.
    """
    result = self.__ome_operation("getQueryService", "findByString",
                                  Study.OME_TABLE, "label", value)    
    return None if result is None else Study(result)

  def save_study(self, kb_study):
    """
    Save and return a study object.
    """
    try:
      result = self.__ome_operation("getUpdateService", "saveAndReturnObject",
                                    kb_study.ome_obj)
    except omero.ValidationException:
      if kb_study.label is None:
        raise kb.KBError("study label can't be None")
      else:
        raise kb.KBError("a study with label %r already exists" %
                         kb_study.label)
      self.__disconnect()
    return Study(result)
