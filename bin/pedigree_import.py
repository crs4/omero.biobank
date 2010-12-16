"""
Import pedigree info from a PED file to an omero DB
"""

import logging
LOG_FILENAME = "pedigree_import.log"
logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)
import sys, optparse, time

import omero
import omero.model as om
import omero.rtypes as ort

import vl.lib.utils as vl_utils

import bl.lib.genotype.pedigree as ped


STUDY_LABEL = "pedigree_test" # FIXME: turn this into a parameter
M = "MALE"
F = "FEMALE"
GENDER_MAP = {"1": M, "m": M, "2": F, "f": F}


class individual(object):
  def __init__(self, iid, sex, father=None, mother=None, genotyped=False):
    self.id = iid
    self.sex = sex
    self.father = father
    self.mother = mother
    self.genotyped = genotyped
    self.omero_obj = None


def read_ped_file(pedfile):
  fin = open(pedfile)
  inds = {}
  for l in fin:
    l = l.strip()
    if len(l) == 0:
      continue
    fields = l.split()
    fam_label, label, father, mother, sex, genotyped = fields
    genotyped = genotyped != '0'
    inds[label] = individual(label, sex, father, mother, genotyped)
  fin.close()
  for label, ind in inds.iteritems():
    inds[label].father = inds.get(ind.father, None)
    inds[label].mother = inds.get(ind.mother, None)
    assert inds[label].father is None or inds[label].father.sex == '1'
    assert inds[label].mother is None or inds[label].mother.sex == '2'
  return inds.values()


def save_obj(ome_obj, client, user, passwd):
  session = client.createSession(user, passwd)
  try:
    us = session.getUpdateService()
    ome_obj = us.saveAndReturnObject(ome_obj)
  finally:
    client.closeSession()
  return ome_obj


def make_omero_study(study_label):
  st = om.StudyI()
  st.vid = ort.rstring(vl_utils.make_vid())
  st.label = ort.rstring(study_label)
  st.startDate = vl_utils.time2rtime(time.time())
  return st


def make_omero_ind(ind_obj):
  logger = logging.getLogger("make_omero_ind")
  ind = omero.model.IndividualI()
  ind.vid = ort.rstring(vl_utils.make_vid())
  ind.gender = make_gender(ind_obj.sex)
  if not ind_obj.father is None:
    ind.father = ind_obj.father.omero_obj
    logger.debug('IND %s: Father VID is %s' %
                 (ort.unwrap(ind.vid), ort.unwrap(ind.father.vid)))
  if not ind_obj.mother is None:
    ind.mother = ind_obj.mother.omero_obj
    logger.debug('IND %s: Mother VID is %s' %
                 (ort.unwrap(ind.vid), ort.unwrap(ind.mother.vid)))
  return ind


def make_enrollment(ind_obj, omero_ind_obj, omero_study_obj):
  enroll = om.EnrollmentI()
  enroll.vid = ort.rstring(vl_utils.make_vid())
  enroll.individual = omero_ind_obj
  enroll.study = omero_study_obj
  enroll.studyCode = ort.rstring(ind_obj.id)
  enroll.dummy = ort.rbool(False)
  enroll.stCodeUK = vl_utils.make_unique_key(ort.unwrap(omero_study_obj.id),
                                             ind_obj.id)
  enroll.stIndUK = vl_utils.make_unique_key(ort.unwrap(omero_study_obj.id), 
                                            ort.unwrap(omero_ind_obj.id))
  return enroll


def make_gender(gender_val):
  gender_obj = om.GenderI()
  gender_obj.value = ort.rstring(GENDER_MAP[gender_val])
  return gender_obj


def omero_save(to_be_saved, ome_study, ome_client, ome_user, ome_passwd):
  logger = logging.getLogger("omero_save")
  for p in to_be_saved:
    if p.omero_obj is None:
      omero_ind = make_omero_ind(p)
      omero_ind = save_obj(omero_ind, ome_client, ome_user, ome_passwd)
      enroll_data = make_enrollment(p, omero_ind, ome_study)
      # Cascade save enroll_data and individual object connected to id
      enroll_data = save_obj(enroll_data, ome_client, ome_user, ome_passwd)
      if not (p.father is None or p.father.omero_obj):
        logger.debug('p.id = %s father.id = %s, father.omero_obj %r' %
                     (p.id, p.father.id, p.father.omero_obj))
      if not (p.mother is None or p.mother.omero_obj):
        logger.debug('p.id = %s mother.id = %s, mother.omero_obj %r' %
                     (p.id, p.mother.id, p.mother.omero_obj))
      p.omero_obj = omero_ind
      logger.debug('Saved individual %s with VID %s' %
                   (p.id, ort.unwrap(p.omero_obj.vid)))


def make_parser():
  parser = optparse.OptionParser(usage="pedigree_import [OPTIONS] PED_FILE")
  parser.set_description(__doc__.lstrip())
  parser.add_option("--hostname", type="str", metavar="STRING",
                    help="omero server hostname [%default]",
                    default="localhost")
  parser.add_option("--user", type="str", metavar="STRING",
                    help="omeo server user name [%default]",
                    default="root")
  parser.add_option("--password", type="str", metavar="STRING",
                    help="omero server password [%default]",
                    default="omero")
  return parser


def main(argv):
  logger = logging.getLogger("main")

  parser = make_parser()
  opt, args = parser.parse_args()
  try:
    pedfname = args[0]
  except IndexError:
    parser.print_help()
    sys.exit(2)
  
  client  = omero.client(opt.hostname)
  ome_user, ome_passwd  = opt.user, opt.password

  study_obj = make_omero_study(STUDY_LABEL)
  study_obj = save_obj(study_obj, client, ome_user, ome_passwd)
  
  family = read_ped_file(pedfname)
  founders, non_founders, couples, children = ped.analyze(family)
  logger.debug('Total founders count: %d' % len(founders))
  logger.debug('Total non founders count: %d' % len(non_founders))
  saved = []
  to_be_saved = founders
  gen_counter = 0
  while len(to_be_saved) > 0 :
    logger.debug('Saving generation %d' % gen_counter)
    omero_save(to_be_saved, study_obj, client, ome_user, ome_passwd)
    saved.extend(to_be_saved)
    next_to_be_saved = []
    for p in saved:
      assert not p.omero_obj is None
      logger.debug('Building parental informations for individual %s' % p.id)
      for c in children.get(p, []):
        if c.mother.omero_obj and c.father.omero_obj and not c.omero_obj:
          next_to_be_saved.append(c)
    to_be_saved = list(set(next_to_be_saved))
    gen_counter += 1


if __name__ == "__main__":
  main(sys.argv)
