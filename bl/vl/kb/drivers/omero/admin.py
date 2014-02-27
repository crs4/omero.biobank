# BEGIN_COPYRIGHT
# END_COPYRIGHT

from bl.vl.kb import KBPermissionError

import omero
from omero_model_PermissionsI import PermissionsI as Perm

class Admin(object):

  """
  The three permission levels work as follow:
  * GROUP_PERM_PRIVATE: each user of the group can read\write only the data produced by
                        himself
  * GROUP_PERM_READ_ONLY: users can read data produced by other users in the same group
                          but only the user that created the data can update or delete them,
                          users can also read data from OMERO.tables created by other users
                          in the same group but can't write new rows in the tables
  * GROUP_PERM_READ_WRITE: users can read\write\delete data created by other users in the
                           same group and can also write data in OMERO.tables created by one
                           user of the same group
  """
  GROUP_PERM_PRIVATE = 'rw----'
  GROUP_PERM_READ_ONLY = 'rwra--'
  GROUP_PERM_READ_WRITE = 'rwrw--'

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

  def change_group_permissions(self, group_name, perms):
    """
    only group owners and admins can change group permissions
    admin can change permissions for all the groups
    owners can change permissions only for the group they are leaders of

    .. code-block:: python

       kb = KB(driver='omero')('localhost', 'root', 'ROOT_PASSWD')
       kb.admin.change_group_permissions('foogroup', kb.admin.GROUP_PERM_xxx)
    """
    if perms not in (self.GROUP_PERM_PRIVATE, self.GROUP_PERM_READ_ONLY,
                     self.GROUP_PERM_READ_WRITE):
      raise ValueError('Permission string "%s" is not a valid one' % perms)
    else:
      if not self.kb.current_session:
        self.kb.connect()
      try:
          p = Perm(perms)
          g = self.kb._get_group(group_name)
          a = self.kb.current_session.getAdminService()
          a.changePermissions(g, p)
      except omero.SecurityViolation:
        raise KBPermissionError('User %s is not allowed to change group permissions for group %s' %
                                (self.kb.user, group_name))
