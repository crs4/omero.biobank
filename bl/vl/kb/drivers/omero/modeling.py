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

  def get_vessel(self, label):
    """
    Return the Vessel object labeled 'label' or None if nothing matches 'label'.
    a label 'foo:A1' is interpreted as well 'A1' of plate 'foo'.
    """
    parts = label.split(':')
    if len(parts) == 1:
      query = 'select t from Tube t where t.label = :label'
      pars = self.kb.ome_query_params({'label' : wp.ome_wrap(label, wp.STRING)})
      result = self.kb.ome_operation("getQueryService", "findByQuery",
                                     query, pars)
      return None if result is None else self.kb.factory.wrap(result)
    elif len(parts) == 2:
      query = """select pw from PlateWell pw join fetch pw.container as ct
                 where pw.label = :wlabel and ct.label = :clabel
                 """
      pars = self.kb.ome_query_params({'clabel' : wp.ome_wrap(parts[0],
                                                              wp.STRING),
                                       'wlabel' : wp.ome_wrap(parts[1],
                                                              wp.STRING),
                                       })
      result = self.kb.ome_operation("getQueryService", "findByQuery",
                                     query, pars)
      return None if result is None else self.kb.factory.wrap(result)
    else:
      raise ValueError('Bad label %s value' % label)

  def get_data_collection(self, label):
    """
    Return the DataCollection object labeled 'label' or
    None if nothing matches 'label'.
    """
    query = 'select dc from DataCollection dc where dc.label = :label'
    pars = self.kb.ome_query_params({'label' : wp.ome_wrap(label, wp.STRING)})
    result = self.kb.ome_operation("getQueryService", "findByQuery",
                                   query, pars)
    return None if result is None else self.kb.factory.wrap(result)

  def get_objects(self, klass):
    query = "from %s o" % klass.get_ome_table()
    pars = None
    results = self.kb.ome_operation('getQueryService', 'findAllByQuery',
                                    query, pars)
    return [self.kb.factory.wrap(o) for o in results]

  def get_enrolled(self, study):
    query = """select e
               from Enrollment e
               join fetch e.study as s
               join fetch e.individual as i
               where s.label = :slabel
               """
    pars = self.kb.ome_query_params({'slabel' :
                                     wp.ome_wrap(study.label, wp.STRING)})
    result = self.kb.ome_operation("getQueryService", "findAllByQuery",
                                   query, pars)
    return [self.kb.factory.wrap(e) for e in result]

  def get_data_objects(self, sample):
    query = """select do
               from DataObject do
               join fetch do.sample as s
               where s.id  = :sid
               """
    pars = self.kb.ome_query_params({'sid' :
                                     wp.ome_wrap(sample.omero_id, wp.LONG)})
    result = self.kb.ome_operation("getQueryService", "findAllByQuery",
                                   query, pars)
    return [self.kb.factory.wrap(i) for i in result]

  def get_data_collection_items(self, dc):
    query = """select i
               from DataCollectionItem i
               join fetch i.dataSample as s
               join fetch i.dataCollection as dc
               where dc.id  = :dcid
               """
    pars = self.kb.ome_query_params({'dcid' :
                                     wp.ome_wrap(dc.omero_id, wp.LONG)})
    result = self.kb.ome_operation("getQueryService", "findAllByQuery",
                                   query, pars)
    return [self.kb.factory.wrap(i) for i in result]

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

  def get_snp_markers_set(self, label, maker, model, release):
    if label:
      query = """select ms
                 from SNPMarkersSet ms
                 where ms.label = :label
                 """
      pars = self.kb.ome_query_params({'label' : wp.ome_wrap(label)})
    else:
      if not (marker and model and release):
        raise ValueError('maker model and release should be all provided')
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

  def snp_markers_set_exists(self, label, maker, model, release):
    return not self.get_snp_markers_set(label, maker, model, release) is None


  #-------------------------------------------------------------------------

