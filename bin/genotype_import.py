"""
Import genotype data into an omero DB.
"""

import logging
LOG_FILENAME = "genotype_import.log"
logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)
import sys, os, optparse, time

import omero
import omero.model as om
import omero.rtypes as ort
import omero_sys_ParametersI as op

import vl.lib.utils as vl_utils


STUDY_LABEL = "pedigree_test"  # FIXME: turn this into a parameter


def read_merge_gt_output(merge_gt_out_dir):
  merge_gt_output = []
  for fn in os.listdir(merge_gt_out_dir):
    fn = os.path.join(merge_gt_out_dir, fn)
    if os.path.isdir(fn):
      continue
    f = open(fn)
    merge_gt_output.extend([l.split() for l in f])
    f.close()


def save_object(client, user, password, obj):
  session = client.createSession(user, password)
  try:
    us = session.getUpdateService()
    result = us.saveAndReturnObject(obj)
  finally:
    client.closeSession()
  return result


def get_study(study_label):
  pass


def get_ind(client, user, password, label, study):
  session = client.createSession(user, password)
  qs = session.getQueryService()
  query = """select ind from Individual ind where
             ind.id in (select en.individual from Enrollment en
                        join en.study as st where st.id=:st_id
                        and en.studyCode=:st_code)"""
  qp = op.ParametersI()
  qp.add("st_id", study.id)
  qp.add("st_code", ort.wrap(label))
  ind = qs.findByQuery(query, qp)
  client.closeSession()
  return ind


def make_parser():
  parser = optparse.OptionParser(usage="%prog [OPTIONS] MERGE_GT_OUTPUT_DIR]")
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
      merge_gt_out_dir = args[0]
  except IndexError:
      parser.print_help()
      sys.exit(2)

  merge_gt_output = read_merge_gt_output(merge_gt_out_dir)
  client = omero.client(opt.hostname)
  user, password = opt.user, opt.password
  #study_obj = get_study(STUDY_LABEL)
  
  for (ind_label, n_gt, n_nocall,
       ml_vid, ml_path, ml_hash,
       gt_vid, gt_path, gt_hash) in merge_gt_output:
    ind_label = filter(lambda c: c.isdigit(), ind_label)
    n_gt = int(n_gt)
    n_nocall = int(n_nocall)
    # FIXME: to be continued


if __name__ == "__main__":
  main(sys.argv)
