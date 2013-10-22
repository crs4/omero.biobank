# BEGIN_COPYRIGHT
# END_COPYRIGHT

import omero.model as om
import omero.rtypes as ort

import wrapper as wp
from individual import Gender, Individual, Enrollment
from action import Study, Action
from actions_on_target import ActionOnAction
from location import Location
from utils import assign_vid, make_unique_key


class Demographic(wp.OmeroWrapper):
  
  OME_TABLE = 'Demographic'
  __fields__ = [('vid',   wp.VID, wp.REQUIRED),
                ('name',  wp.STRING, wp.REQUIRED),
                ('surname',  wp.STRING, wp.REQUIRED),
                ('gender', Gender, wp.REQUIRED),
                ('birthPlace', Location, wp.REQUIRED),
                ('birthDate', wp.TIMESTAMP, wp.REQUIRED),
                ('deathDate', wp.TIMESTAMP, wp.OPTIONAL),
                ('livingPlace', Location, wp.OPTIONAL),
                ('livingAddress', wp.STRING, wp.OPTIONAL),
                ('nationalIDCode', wp.STRING, wp.OPTIONAL),
                ('phone1', wp.STRING, wp.OPTIONAL),
                ('phone1', wp.STRING, wp.OPTIONAL),
                ('email', wp.STRING, wp.OPTIONAL),
                ('individual', Individual, wp.REQUIRED),
                # Multi-field unique key
                # demogUK = NAME_SURNAME_BIRTHDATE_GENDER-ID_BIRTHPLACE-VID
                ('demogUK', wp.STRING, wp.REQUIRED),
                ('action', Action, wp.REQUIRED),
                ('lastUpdate', ActionOnAction, wp.OPTIONAL)]
  __do_not_serialize__ = ['demogUK']

  def __preprocess_conf__(self, conf):
    conf = assign_vid(conf)
    if not 'demogUK' in conf:
      conf['demogUK'] = make_unique_key(conf['name'], conf['surname'],
                                        conf['birthdate'],
                                        conf['gender'].omero_id,
                                        conf['birthPlace'].id)
    return conf

  def __update_constraints__(self):
    uk = make_unique_key(self.name, self.surname, self.birthdate,
                         self.gender.omero_id, self.birthPlace.id)
    seattr(self.ome_obj, 'demogUK',
           self.to_omero(self.__fields__['demogUK'][0], uk))


class InformedConsent(wp.OmeroWrapper):
  
  OME_TABLE = 'InformedConsent'
  __fields__ = [('vid', wp.VID, wp.REQUIRED),
                ('label', wp.STRING, wp.REQUIRED),
                ('description', wp.STRING, wp.OPTIONAL),
                ('documentPath', wp.STRING, wp.OPTIONAL),
                ('documentHash', wp.STRING, wp.OPTIONAL),
                ('answersData', om.OriginalFile, wp.OPTIONAL),
                ('refStudy', Study, wp.REQUIRED),
                ('authors', wp.STRING, wp.OPTIONAL),
                ('approvingCommission', wp.STRING, wp.OPTIONAL),
                ('approvalDate', wp.TIMESTAMP, wp.OPTIONAL)]

  def __preprocess_conf__(self, conf):
    return assign_vid(conf)


class Agreement(wp.OmeroWrapper):
  
  OME_TABLE = 'Agreement'
  __fields__ = [('vid', wp.VID, wp.REQUIRED),
                ('refConsent', InformedConsent, wp.REQUIRED),
                ('enrollment', Enrollment, wp.REQUIRED),
                ('submissionDate', wp.TIMESTAMP, wp.REQUIRED),
                ('active', wp.BOOLEAN, wp.REQUIRED)]

  def __preprocess_conf__(self, conf):
    return assign_vid(conf)
