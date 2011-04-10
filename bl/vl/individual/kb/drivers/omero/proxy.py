import time

import omero.rtypes as ort
from bl.vl.sample.kb.drivers.omero.proxy_indexed import ProxyIndexed
from bl.vl.sample.kb.drivers.omero.sample     import BloodSample
from bl.vl.sample.kb.drivers.omero.sample     import DNASample

from individual import Individual
from enrollment import Enrollment
from action     import ActionOnIndividual


class Proxy(ProxyIndexed):
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

  ProxyIndexed.INDEXED_TARGET_TYPES.extend([Individual])

  def get_gender_table(self):
    res = self.ome_operation("getQueryService", "findAll", "Gender", None)
    return dict([(x._value._val, x) for x in res])

  def get_blood_samples(self, individual):
    """
    blood_sample_vids = ikb.get_blood_samples(individual)
    """
    query = """select bs
               from ActionOnIndividual a, BloodSample as bs
               join  a.target  as t
               join  bs.action as bsa
               where t.vid = :i_id
                     and a.id = bs.action.id
            """
    pars = self.ome_query_params({'i_id' : self.ome_wrap(individual.id)})
    results = self.ome_operation("getQueryService", "findAllByQuery", query, pars)
    return [(ort.unwrap(bs.vid), ort.unwrap(bs.id)) for bs in results]

  def get_blood_sample(self, individual):
    """
    FIXME this is a rather gross implementation...
    """
    bss = self.get_blood_samples(individual)
    assert len(bss) <= 1

    if len(bss) == 0:
      return None
    result = self.ome_operation("getQueryService", "get", "BloodSample", bss[0][1])
    return BloodSample(result)


  def get_dna_sample(self, individual):
    query = """select dna
               from ActionOnIndividual aoi, BloodSample as bs,
                    ActionOnSample     aos, DNASample as dna
               join aoi.target as ti
               join bs.action  as bsa
               join aos.target as ts
               join dna.action as da
               where ti.vid = :i_id
                     and aoi.id = bs.action.id
                     and bs.id = ts.id
                     and aos.id = dna.action.id
             """
    pars = self.ome_query_params({'i_id' : self.ome_wrap(individual.id)})
    result = self.ome_operation("getQueryService", "findByQuery", query, pars)
    if not result:
      return result
    action = self.ome_operation("getQueryService",
                                "get", "ActionOnSample",
                                result.action._id._val)
    result.action = action
    target = self.ome_operation("getQueryService",
                                "get", "BloodSample",
                                action.target._id._val)
    action.target = target
    return DNASample(result)


