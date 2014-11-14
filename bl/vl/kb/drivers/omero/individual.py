# BEGIN_COPYRIGHT
# END_COPYRIGHT

import omero.model as om
import omero.rtypes as ort

import wrapper as wp
from action import Study, Action
from actions_on_target import ActionOnAction
from utils import assign_vid, make_unique_key
from bl.vl.kb import KBError


class Gender(wp.OmeroWrapper):
  
  OME_TABLE = 'Gender'
  __enums__ = ['MALE', 'FEMALE']


class Individual(wp.OmeroWrapper):
  
  OME_TABLE = 'Individual'
  __fields__ = [('vid',   wp.VID, wp.REQUIRED),
                ('gender', Gender, wp.REQUIRED),
                ('father', wp.SELF_TYPE, wp.OPTIONAL),
                ('mother', wp.SELF_TYPE, wp.OPTIONAL),
                ('fatherTrusted', wp.BOOLEAN, wp.OPTIONAL),
                ('motherTrusted', wp.BOOLEAN, wp.OPTIONAL),
                ('action', Action, wp.REQUIRED),
                ('lastUpdate', ActionOnAction, wp.OPTIONAL)]

  def __preprocess_conf__(self, conf):
    return assign_vid(conf)

  def __update_constraints__(self):
    if self.father and self.father == self:
      self.reload()
      raise KBError('CONFIGURATION ERROR: individual set as its own father')
    if self.mother and self.mother == self:
      self.reload()
      raise KBError('CONFIGURATION ERROR: individual set as its own mother')


class ActionOnIndividual(Action):
  
  OME_TABLE = 'ActionOnIndividual'
  __fields__ = [('target', Individual, 'required')]


class Enrollment(wp.OmeroWrapper):
  
  OME_TABLE = 'Enrollment'
  __fields__ = [('vid', wp.VID, wp.REQUIRED),
                ('individual', Individual, wp.REQUIRED),
                ('study', Study, wp.REQUIRED),
                ('studyCode', wp.STRING, wp.REQUIRED),
                #  Multi-field unique keys
                #  stCodeUK = STUDY-VID_STUDYCODE
                ('stCodeUK', wp.STRING, wp.REQUIRED),
  __do_not_serialize__ = ['stCodeUK']

  def __preprocess_conf__(self, conf):
    if not 'stCodeUK' in conf:
      svid = conf['study'].vid
      conf['stCodeUK'] = make_unique_key(self.get_namespace(),
                                         svid, conf['studyCode'])
    return assign_vid(conf)

  def __update_constraints__(self):
    st_code_uk = make_unique_key(self.get_namespace(),
                                 self.study.id, self.studyCode)
    setattr(self.ome_obj, 'stCodeUK',
            self.to_omero(self.__fields__['stCodeUK'][0], st_code_uk))
