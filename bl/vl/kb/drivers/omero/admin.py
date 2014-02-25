# BEGIN_COPYRIGHT
# END_COPYRIGHT

from bl.vl.kb import KBPermissionError

class Admin(object):

  def __init__(self, kb):
    self.kb = kb

  def set_group_owner(self, group, user):
    """
    .. code-block:: python

       kb = KB(driver='omero')('localhost', 'root', 'ROOT_PASSWD')
       kb.admin.set_group_owner('group_name', 'foouser')
    """
    if not self.kb.current_session:
      self.kb.connect()
    a = self.kb.current_session.getAdminService()
    ouser = a.lookupExperimenter(user)
    ogroup = a.lookupGroup(group)
    a.setGroupOwner(ogroup, ouser)

  def move_to_common_space(self, objs):
    """
    .. code-block:: python

       kb = KB(driver='omero')('localhost', 'foouser', 'foouser_PASSWD')
       studies = kb.get_objects(kb.Study)
       kb.admin.move_to_common_space(studies)
    """
    if not self.kb.current_session:
      self.kb.connect()
    a = self.kb.current_session.getAdminService()
    if self.kb.is_group_leader():
      a.moveToCommonSpace([o.ome_obj for o in objs])
    else:
      raise KBPermissionError('User %s is not leader of group %s, can\'t move objects to common space' %
                              (self.kb.user, self.kb.get_current_group()[0]))
