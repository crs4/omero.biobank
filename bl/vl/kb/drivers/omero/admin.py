class Admin(object):
  """
  .. todo::

     add admin docs

  """

  def __init__(self, kb):
    self.kb = kb

  def set_group_owner(self, group, user):
    """

    .. code-block:: python

       kb = KB(driver='omero')('localhost', 'root', 'ROOT_PASSWD')
       kb.admin.set_group_owner('group_name', 'foouser')

    """
    c = self.kb.connect()
    a = c.getAdminService()
    ouser  = a.lookupExperimenter(user)
    ogroup = a.lookupGroup(group)
    a.setGroupOwner(ogroup, ouser)
    self.kb.disconnect()

  def move_to_common_space(self, objs):
    """

    .. code-block:: python

       kb = KB(driver='omero')('localhost', 'foouser', 'foouser_PASSWD')
       studies = kb.get_objects(kb.Study)
       kb.admin.move_to_common_space(studies)

    """
    c = self.kb.connect()
    a = c.getAdminService()
    a.moveToCommonSpace([o.ome_obj for o in objs])
