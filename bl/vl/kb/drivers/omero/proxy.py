
# This is actually used in the meta class magic
import omero.model as om

from proxy_core import ProxyCore

from wrapper import ObjectFactory, MetaWrapper

import action
import vessels
import objects_collections
import data_samples
import actions_on_target
import individual

import wrapper as wp

KOK = MetaWrapper.__KNOWN_OME_KLASSES__

class Proxy(ProxyCore):
  """
  An omero driver for KB.

  """

  def __init__(self, host, user, passwd, session_keep_tokens=1):
    super(Proxy, self).__init__(host, user, passwd, session_keep_tokens)
    self.factory = ObjectFactory(proxy=self)
    #-- learn
    for k in KOK:
      klass = KOK[k]
      setattr(self, klass.get_ome_table(), klass)


  # UTILITY FUNCTIONS
  # =================

  def get_device(self, label):
    """
    """
    query = 'select d from Device d where d.label = :label'
    pars = self.ome_query_params({'label' : wp.ome_wrap(label, wp.STRING)})
    result = self.ome_operation("getQueryService", "findByQuery", query, pars)
    return None if result is None else self.factory.wrap(result)

  def get_action_setup(self, label):
    query = 'select a from ActionSetup a where a.label = :label'
    pars = self.ome_query_params({'label' : wp.ome_wrap(label, wp.STRING)})
    result = self.ome_operation("getQueryService", "findByQuery", query, pars)
    return None if result is None else self.factory.wrap(result)

  def get_study(self, label):
    """
    Return the study object labeled 'label' or None if nothing matches 'label'.
    """
    query = 'select st from Study st where st.label = :label'
    pars = self.ome_query_params({'label' : wp.ome_wrap(label, wp.STRING)})
    result = self.ome_operation("getQueryService", "findByQuery", query, pars)
    return None if result is None else self.factory.wrap(result)

  def get_enrolled(self, study):
    query = """select e
               from Enrollment e join fetch e.study as s
               where s.label = :slabel
               """
    pars = self.ome_query_params({'slabel' :
                                  wp.ome_wrap(study.label, wp.STRING)})
    result = self.ome_operation("getQueryService", "findAllByQuery",
                                query, pars)
    return [self.factory.wrap(e) for e in result]

  def get_enrollment(self, study, ind_label):
    query = """select e
               from Enrollment e join fetch e.study as s
               where e.studyCode = :ilabel and s.id = :sid
               """
    pars = self.ome_query_params({'ilabel' : wp.ome_wrap(ind_label),
                                  'sid'    : wp.ome_wrap(study.omero_id)})
    result = self.ome_operation("getQueryService", "findByQuery", query, pars)
    return None if result is None else self.factory.wrap(result)

  def get_vessels(self, klass=vessels.Vessel, content=None):
    if not issubclass(klass, self.Vessel):
      raise ValueError('klass should be a subclass of Vessel')
    if not content:
      query = "select v from %s v" % klass.get_ome_table()
      pars = None
    elif isinstance(content, self.VesselContent):
      # FIXME uhmm, there is something conceptually broken in the enum
      # handling...
      value = content.ome_obj.value._val
      query = """select v from %s v join fetch v.content as c
                 where c.value = :cvalue
              """ % klass.get_ome_table()
      pars = self.ome_query_params({'cvalue' : wp.ome_wrap(value)})
    else:
      raise ValueError('content should be an instance of VesselContent')
    results = self.ome_operation('getQueryService', 'findAllByQuery', query,
                                 pars)
    return [self.factory.wrap(v) for v in results]

  def get_containers(self, klass=objects_collections.Container):
    if not issubclass(klass, self.Container):
      raise ValueError('klass should be a subclass of Container')
    query = "select v from %s v" % klass.get_ome_table()
    pars = None
    results = self.ome_operation('getQueryService', 'findAllByQuery', query,
                                 pars)
    return [self.factory.wrap(v) for v in results]








