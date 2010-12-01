import time

import omero
import omero.model as om
import omero.rtypes as ort
import omero_sys_ParametersI as op

import vl.lib.utils as vl_utils

import bl.lib.genotype.kb as kb


class OmeroWrapper(object):
  
  def __init__(self, ome_obj):
    super(OmeroWrapper, self).__setattr__("ome_obj", ome_obj)

  def __getattr__(self, name):
    return ort.unwrap(getattr(self.ome_obj, name))

  # WARNING: the 'wrap' function performs only basic type
  # conversions. Override this when more sophisticated conversions are
  # required (e.g., timestamps or computed results)
  def __setattr__(self, name, value):
    return setattr(self.ome_obj, name, ort.wrap(value))

  @property
  def id(self):
    return self.vid


class Study(OmeroWrapper, kb.Study):

  def __init__(self, label=None):
    ome_study = om.StudyI()
    ome_study.vid = ort.rstring(vl_utils.make_vid())
    if label is not None:
      ome_study.label = ort.rstring(label)
    ome_study.startDate = vl_utils.time2rtime(time.time())
    super(Study, self).__init__(ome_study)


class KnowledgeBase(kb.KnowledgeBase):
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

  def __connect(self):
    return self.client.createSession(self.user, self.passwd)

  def __disconnect(self):
    self.client.closeSession()

  def get_study_by_label(self, label):
    """
    Return the study object labeled 'label' or None if nothing matches 'label'.
    """
    session = self.__connect()
    qs = session.getQueryService()
    study = qs.findByString("Study", "label", label)
    self.__disconnect()
    return Study(study)

  def create_study(self, label):
    """
    Create, save and return a study object labeled 'label'.
    """
    session = self.__connect()
    us = session.getUpdateService()
    study = om.StudyI()
    study.setVid(ort.rstring(vl_utils.make_vid()))
    study.setLabel(ort.rstring(label))
    study.setStartDate(ort.rtime(vl_utils.time2rtime(time.time())))
    study = us.saveAndReturnObject(study)
    self.__disconnect()
    return self.__get_vid(study)
