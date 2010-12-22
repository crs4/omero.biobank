"""
Import genotype data into an omero DB.
"""

import logging
LOG_FILENAME = "genotype_import.log"
logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)
import sys, os, optparse, time, uuid

import omero
import omero.model as om
import omero.rtypes as ort
import omero_sys_ParametersI as op

import vl.lib.utils as vl_utils
import vl.lib.utils.ome_utils as om_utils


STUDY_LABEL = "pedigree_test"  # FIXME: turn this into a parameter
ACTION_TYPES = {om.IndividualI : om.ActionOnIndividualI,
                om.BioSampleI  : om.ActionOnSampleI}
MARKERS_LIST_LABEL = "CRS4 CUSTOM" # FIXME: turn this into a parameter


def read_merge_gt_output(merge_gt_out_dir):
  logger = logging.getLogger("read_merge_gt_output")
  merge_gt_output = []
  for fn in os.listdir(merge_gt_out_dir):
    fn = os.path.join(merge_gt_out_dir, fn)
    logger.debug('Opening file %s' % fn)
    if os.path.isdir(fn):
      continue
    f = open(fn)
    merge_gt_output.extend([l.split() for l in f])
    f.close()
  return merge_gt_output

def save_object(client, user, password, obj):
  session = client.createSession(user, password)
  try:
    us = session.getUpdateService()
    result = us.saveAndReturnObject(obj)
  finally:
    client.closeSession()
  return result


def get_study(client, user, password, study_label):
  session = client.createSession(user, password)
  qs = session.getQueryService()
  study = qs.findByString("Study", "label", study_label)
  client.closeSession()
  return study


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

def get_action_type(act_type):
  action_type_obj = om.ActionTypeI()
  action_type_obj.value = ort.rstring(act_type)
  return action_type_obj

def make_bio_sample(individual, study):
  sample_obj = om.BioSampleI()
  sample_obj.vid = ort.rstring(vl_utils.make_vid())
  sample_obj.creationDate = om_utils.time2rtime(time.time())
  sample_obj.action = make_action(individual, study)
  sample_obj.barcode = ort.rstring(uuid.uuid4().hex) # Simulate a barcode, generate a random hexadecimal string
  sample_obj.initialVolume = ort.rfloat(10) # Dummy
  sample_obj.currentVolume = ort.rfloat(10) # Dummy
  sample_obj.concentration = ort.rfloat(1)  # Dummy
  return sample_obj

def make_action(target, study):
  action_obj = ACTION_TYPES[type(target)]()
  action_obj.vid = ort.rstring(vl_utils.make_vid())
  action_obj.context = study
  action_obj.target = target
  action_obj.beginTime = om_utils.time2rtime(time.time())
  action_obj.operator = ort.rstring('lianas@crs4.it')
  action_obj.actionType = get_action_type('IMPORT')
  return action_obj

def get_markers_list(fhash, client, user, password):
  session = client.createSession(user, password)
  try:
    qs = session.getQueryService()
    data_obj = qs.findByString('DataObject', 'fileHash', fhash, None)
    if not data_obj:
      client.closeSession()
      return None
    query = """
            SELECT ml FROM MarkersList ml
            WHERE ml.markers = :dobj_id
            """
    qp = ParametersI()
    qp.add('dobj_id', data_obj.id)
    ml_obj = qs.findByQuery(query, qp)
  finally:
    client.closeSession()
    return None
  return ml_obj

def make_markers_list(label, fpath, fhash, fvid = None):
  mlist_obj = om.MarkersListI()
  mlist_obj.vid = ort.rstring(vl_utils.make_vid())
  mlist_obj.label = ort.rstring(label)
  mlist_obj.markers = make_data_object(fpath, fhash)
  return mlist_obj

def make_data_object(path, checksum, vid=None):
  data_obj = om.DataObjectI()
  if vid:
    data_obj.vid = ort.rstring(vid)
  else:
    data_obj.vid = ort.rstring(vl_utils.make_vid())
  data_obj.path = ort.rstring(path)
  data_obj.fileHash = ort.rstring(checksum)
  return data_obj
  

def make_parser():
  parser = optparse.OptionParser(usage="%prog [OPTIONS] MERGE_GT_OUTPUT_DIR")
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
  parser.add_option("--treshold", type="int", metavar="INT",
                    help="minimum genotypes count for a record to be processed [%default]",
                    default=1)
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
  treshold = opt.treshold
  study_obj = get_study(client, user, password, STUDY_LABEL)
  
  for (ind_label, n_gt, n_nocall,
       ml_vid, ml_path, ml_hash,
       gt_vid, gt_path, gt_hash) in merge_gt_output:
    ind_label = filter(lambda c: c.isdigit(), ind_label)
    n_gt = int(n_gt)
    n_nocall = int(n_nocall)
    if (n_gt - n_nocall) < treshold:
      logger.debug('Genotype count under treshold value, skip record. Treshold: %d -- Genotypes: %d'
                   % (treshold, n_gt - n_nocall))
      continue
    else:
      ind_obj = get_ind(client, user, password, ind_label, study_obj)
      sample_obj = make_bio_sample(ind_obj, study_obj)
      sample_obj = save_object(client, user, password, sample_obj)
      logger.debug('Bio Sample saved with VID %s, related action\' VID %s' %
                   (ort.unwrap(sample_obj.vid), ort.unwrap(sample_obj.action.vid)))
      # Check if markers list is already saved in the DB ...
      mlist_obj = get_markers_list(ml_hash, client, user, password)
      # ... if not save and use it
      if not mlist_obj:
        logger.debug('No markers list found with hash %s. Creating a new one' % ml_hash)
        mlist_obj = make_markers_list(fvid=ml_vid, fpath=ml_path, fhash=ml_hash,
                                      label=MARKERS_LIST_LABEL)
        mlist_obj = save_object(client, user, password, mlist_obj)
      logger.debug('Markers list VID %s' % ort.unwrap(mlist_obj.vid))
    


if __name__ == "__main__":
  main(sys.argv)
