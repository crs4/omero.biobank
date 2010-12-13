"""
Import pedigree info from a PED file to an omero DB.

FIXME: add CLI functionalities from this script to
examples/import_ped.py, then replace the former with the latter.
"""

import logging
LOG_FILENAME = "pedigree_import.log"
logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)
import sys, optparse, time

import omero
import omero.model as om
import omero.rtypes as ort

import vl.lib.utils as vl_utils


STUDY_LABEL = "pedigree_test"  # FIXME: turn this into a parameter
M = "MALE"
F = "FEMALE"
GENDER_MAP = {"1": M, "m": M, "2": F, "f": F}


def make_gender(gender_str):
  gender = om.GenderI()
  gender.setValue(ort.rstring(GENDER_MAP[gender_str]))
  return gender


def save_object(client, user, password, obj):
  session = client.createSession(user, password)
  try:
    us = session.getUpdateService()
    result = us.saveAndReturnObject(obj)
  finally:
    client.closeSession()
  return result


def make_enrollment(ind_obj, study_obj, ind_label):
  enr_obj = om.EnrollmentI()
  enr_obj.vid = ort.rstring(vl_utils.make_vid())
  enr_obj.individual = ind_obj
  enr_obj.study = study_obj
  enr_obj.studyCode = ort.rstring(ind_label)
  enr_obj.dummy = ort.rbool(False)
  enr_obj.stCodeUK = vl_utils.make_unique_key(study_obj.id, ind_label)
  enr_obj.stIndUK = vl_utils.make_unique_key(study_obj.id, ind_obj.id)
  return enr_obj


def make_ind_data(ped_file, study_obj, client, user, password):
  logger = logging.getLogger("make_ind_data")
  ind_data = {}
  f = open(ped_file)
  for line in f:
    try:
      fam, ind, father, mother, gender = line.split(None, 5)[:5]
    except ValueError:
      continue  # blank or illegal line
    # NOTE: we assume that individual labels are unique across the whole ped
    print "saving individual %r" % ind
    ind_obj = om.IndividualI()
    ind_obj.vid = ort.rstring(vl_utils.make_vid())
    ind_obj.gender = make_gender(gender)
    ind_obj = save_object(client, user, password, ind_obj)
    logger.debug("saved ind %r with vid %r" % (ind, ort.unwrap(ind_obj.vid)))
    enrollment_obj = make_enrollment(ind_obj, study_obj, ind)
    enrollment_obj = save_object(client, user, password, enrollment_obj)
    logger.debug("saved enrollment for ind %r with vid %r" %
                 (ind, ort.unwrap(enrollment_obj.vid)))
    ind_data[ind] = [ind_obj, father, mother]
  f.close()
  return ind_data


def get_ind(client, user, password, vid):
  session = client.createSession(user, password)
  qs = session.getQueryService()
  ind = qs.findByString("Individual", "vid", vid, None)
  client.closeSession()
  return ind


def make_parser():
  parser = optparse.OptionParser(usage="pedigree_import [OPTIONS] PED_FILE]")
  parser.set_description(__doc__.lstrip())
  parser.add_option("--hostname", type="str", metavar="STRING",
                    help="omero server hostname [%default]",
                    default="localhost")
  parser.add_option("--user", type="str", metavar="STRING",
                    help="omero server user name [%default]",
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
      ped_file = args[0]
  except IndexError:
      parser.print_help()
      sys.exit(2)

  client = omero.client(opt.hostname)
  user, password = opt.user, opt.password

  study_obj = om.StudyI()
  study_obj.vid = ort.rstring(vl_utils.make_vid())
  study_obj.label = ort.rstring(STUDY_LABEL)
  study_obj.startDate = vl_utils.time2rtime(time.time())
  study_obj = save_object(client, user, password, study_obj)

  ind_data = make_ind_data(ped_file, study_obj, client, user, password)
  f = open("vid_list.txt", "w")
  for label, (ind_obj, father, mother) in ind_data.iteritems():
    print "updating father/mother for individual %r" % label
    has_father = has_mother = True
    try:
      father_obj = ind_data[father][0]
      ind_obj.father = father_obj
    except KeyError:
      has_father = False
    try:
      mother_obj = ind_data[mother][0]
      ind_obj.mother = mother_obj
    except KeyError:
      has_mother = False
    if has_mother or has_father:  # need to re-save
      ind_obj = save_object(client, user, password, ind_obj)
      # re-read obj from omero to avoid a cascade saving that could
      # break the script if grandpa is updated after daddy (daddy's
      # reference to grandpa would be out-of-sync).
      ind_data[label][0] = get_ind(client, user, password,
                                   ort.unwrap(ind_obj.vid))
      logger.debug("updated ind %r with vid %r" %
                   (label, ort.unwrap(ind_data[label][0].vid)))
    f.write("%s\t%s\n" % (label, ort.unwrap(ind_obj.vid)))
  f.close()


if __name__ == "__main__":
  main(sys.argv)
