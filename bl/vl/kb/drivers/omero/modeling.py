import wrapper as wp

class ModelingAdapter(object):
  def __init__(self, kb):
    self.kb = kb

  #---------------------------------------------------------------------
  def get_device(self, label):
    """
    """
    query = 'select d from Device d where d.label = :label'
    pars = self.kb.ome_query_params({'label' : wp.ome_wrap(label, wp.STRING)})
    result = self.kb.ome_operation("getQueryService", "findByQuery",
                                   query, pars)
    return None if result is None else self.kb.factory.wrap(result)

  def get_action_setup(self, label):
    query = 'select a from ActionSetup a where a.label = :label'
    pars = self.kb.ome_query_params({'label' : wp.ome_wrap(label, wp.STRING)})
    result = self.kb.ome_operation("getQueryService", "findByQuery",
                                   query, pars)
    return None if result is None else self.kb.factory.wrap(result)

  def get_study(self, label):
    """
    Return the study object labeled 'label' or None if nothing matches 'label'.
    """
    query = 'select st from Study st where st.label = :label'
    pars = self.kb.ome_query_params({'label' : wp.ome_wrap(label, wp.STRING)})
    result = self.kb.ome_operation("getQueryService", "findByQuery",
                                   query, pars)
    return None if result is None else self.kb.factory.wrap(result)

  def get_objects(self, klass):
    query = "select o from %s o" % klass.get_ome_table()
    pars = None
    results = self.kb.ome_operation('getQueryService', 'findAllByQuery',
                                    query, pars)
    return [self.kb.factory.wrap(o) for o in results]

  def get_enrolled(self, study):
    query = """select e
               from Enrollment e join fetch e.study as s
               where s.label = :slabel
               """
    pars = self.kb.ome_query_params({'slabel' :
                                     wp.ome_wrap(study.label, wp.STRING)})
    result = self.kb.ome_operation("getQueryService", "findAllByQuery",
                                   query, pars)
    return [self.kb.factory.wrap(e) for e in result]

  def get_enrollment(self, study, ind_label):
    query = """select e
               from Enrollment e join fetch e.study as s
               where e.studyCode = :ilabel and s.id = :sid
               """
    pars = self.kb.ome_query_params({'ilabel' : wp.ome_wrap(ind_label),
                                     'sid'    : wp.ome_wrap(study.omero_id)})
    result = self.kb.ome_operation("getQueryService", "findByQuery",
                                   query, pars)
    return None if result is None else self.kb.factory.wrap(result)

  def get_vessels(self, klass, content):
    if not issubclass(klass, self.kb.Vessel):
      raise ValueError('klass should be a subclass of Vessel')
    if not content:
      query = "select v from %s v" % klass.get_ome_table()
      pars = None
    elif isinstance(content, self.kb.VesselContent):
      # FIXME uhmm, there is something conceptually broken in the enum
      # handling...
      value = content.ome_obj.value._val
      query = """select v from %s v join fetch v.content as c
                 where c.value = :cvalue
              """ % klass.get_ome_table()
      pars = self.kb.ome_query_params({'cvalue' : wp.ome_wrap(value)})
    else:
      raise ValueError('content should be an instance of VesselContent')
    results = self.kb.ome_operation('getQueryService', 'findAllByQuery',
                                    query, pars)
    return [self.kb.factory.wrap(v) for v in results]

  def get_containers(self, klass):
    if not issubclass(klass, self.kb.Container):
      raise ValueError('klass should be a subclass of Container')
    query = "select v from %s v" % klass.get_ome_table()
    pars = None
    results = self.kb.ome_operation('getQueryService', 'findAllByQuery',
                                    query, pars)
    return [self.kb.factory.wrap(v) for v in results]

  def get_snp_markers_set(self, maker, model, release):
    query = """select ms
               from SNPMarkersSet ms
               where ms.maker = :maker
                     and ms.model = :model
                     and ms.release = :release
               """
    pars = self.kb.ome_query_params({'maker' : wp.ome_wrap(maker),
                                     'model' : wp.ome_wrap(model),
                                     'release' : wp.ome_wrap(release)})
    result = self.kb.ome_operation("getQueryService", "findByQuery",
                                   query, pars)
    return None if result is None else self.kb.factory.wrap(result)

  def snp_markers_set_exists(self, maker, model, release):
    return not self.get_snp_markers_set(maker, model, release) is None


  #-------------------------------------------------------------------------

