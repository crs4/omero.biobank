import time

import omero
import omero_ServerErrors_ice  # magically adds exceptions to the omero module

import bl.lib.genotype.kb as kb


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

  def connect(self):
    return self.client.createSession(self.user, self.passwd)

  def disconnect(self):
    self.client.closeSession()

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


# #--- SAVE/DELETE examples, not used for now ------------------------------#
#   def get_study_by_label(self, value):
#     """
#     Return the study object labeled 'value' or None if nothing matches 'value'.
#     """
#     result = self.__ome_operation("getQueryService", "findByString",
#                                   Study.OME_TABLE, "label", value)
#     return None if result is None else Study(result)

#   def save_study(self, kb_study):
#     """
#     Save and return a study object.
#     """
#     try:
#       result = self.__ome_operation("getUpdateService", "saveAndReturnObject",
#                                     kb_study.ome_obj)
#     except omero.ValidationException:
#       if kb_study.label is None:
#         raise kb.KBError("study label can't be None")
#       else:
#         raise kb.KBError("a study with label %r already exists" %
#                          kb_study.label)
#     return Study(result)

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
