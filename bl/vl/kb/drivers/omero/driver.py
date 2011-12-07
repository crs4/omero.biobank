from proxy import proxy
from actions import Action
from object_factory import ObjectFactory


class Driver(Proxy):
  
  def __init__(self, host, user, passwd, group=None):
    super(Driver, self).__init__(host, user, passwd, group)
    self.object_factory = ObjectFactory(self)

  def get_all_instances(self, klass):
    table_name = klass.get_ome_table()
    res = self.ome_operation("getQueryService", "findAll", table_name, None)
    return [self.object_factory.wrap(x) for x in res]
