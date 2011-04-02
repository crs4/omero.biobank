import time

from bl.lib.sample.kb.drivers.omero.proxy_core import ProxyCore
from bl.lib.sample.kb.drivers.omero.sample     import BloodSample

from individual import Individual
from enrollment import Enrollment
from action     import ActionOnIndividual


class Proxy(ProxyCore):
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

  Individual = Individual
  Enrollment = Enrollment
  ActionOnIndividual = ActionOnIndividual

  def get_gender_table(self):
    res = self.ome_operation("getQueryService", "findAll", "Gender", None)
    return dict([(x._value._val, x) for x in res])

  def get_blood_sample(self, individual):
    query = """select bs from ActionOnIndividual a, BloodSample as bs
                      join a.target as t join bs.action as bsa
               where t.vid = :i_id and a.id = bs.action.id"""
    pars = self.ome_query_params({'i_id' : self.ome_wrap(individual.id)})
    result = self.ome_operation("getQueryService", "findByQuery", query, pars)
    return BloodSample(result) if result else result

  def get_dna_sample(self, individual):
    pass


