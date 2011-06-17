import omero.model as om
import omero.rtypes as ort

import bl.vl.utils as vu
import bl.vl.utils.ome_utils as vou
import wrapper as wp

from action              import Study, Action
from actions_on_target   import ActionOnAction

from utils import assign_vid, make_unique_key

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
                # stIndUK = STUDY-VID_INDIVIDUAL-ID
                ('stIndUK',  wp.STRING, wp.REQUIRED)]

  def __preprocess_conf__(self, conf):
    if not 'stCodeUK' in conf:
      svid = conf['study'].vid
      conf['stCodeUK'] = make_unique_key(svid, conf['studyCode'])
    if not 'stIndUK' in conf:
      svid = conf['study'].vid
      ivid = conf['individual'].vid
      conf['stIndUK'] = make_unique_key(svid, ivid)
    return conf
