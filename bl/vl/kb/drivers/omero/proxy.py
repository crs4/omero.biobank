
# This is actually used in the meta class magic
import omero.model as om
import bl.vl.utils as vlu

from proxy_core import ProxyCore

from wrapper import ObjectFactory, MetaWrapper

import action
import vessels
import objects_collections
import data_samples
import actions_on_target
import individual

import hashlib


from genotyping import GenotypingAdapter
from modeling   import ModelingAdapter
from eav        import EAVAdapter
from ehr        import EHR

KOK = MetaWrapper.__KNOWN_OME_KLASSES__

class Proxy(ProxyCore):
  """
  An omero driver for KB.

  """

  def __init__(self, host, user, passwd, session_keep_tokens=1):
    super(Proxy, self).__init__(host, user, passwd, session_keep_tokens)
    self.factory = ObjectFactory(proxy=self)
    #-- learn
    for k in KOK:
      klass = KOK[k]
      setattr(self, klass.get_ome_table(), klass)
    #-- setup adapters
    self.gadpt = GenotypingAdapter(self)
    self.madpt = ModelingAdapter(self)
    self.eadpt = EAVAdapter(self)

  def __check_type(self, fname, ftype, val):
    if not isinstance(val, ftype):
      msg = 'bad type for %s(%s)' % (fname, val)
      raise ValueError(msg)

  # High level ops
  # ==============
  def find_all_by_query(self, query, params):
    return super(Proxy, self).find_all_by_query(query, params, self.factory)

  # MODELING related utility functions
  # ==================================

  def get_device(self, label):
    return self.madpt.get_device(label)

  def get_action_setup(self, label):
    return self.madpt.get_action_setup(label)

  def get_study(self, label):
    """
    Return the study object labeled 'label' or None if nothing matches 'label'.
    """
    return self.madpt.get_study(label)

  def get_data_collection(self, label):
    """
    Return the DataCollection object labeled 'label'
    or None if nothing matches 'label'.
    """
    return self.madpt.get_data_collection(label)

  def get_data_collection_items(self, dc):
    return self.madpt.get_data_collection_items(dc)

  def get_objects(self, klass):
    return self.madpt.get_objects(klass)

  def get_enrolled(self, study):
    return self.madpt.get_enrolled(study)

  def get_enrollment(self, study, ind_label):
    return self.madpt.get_enrollment(study, ind_label)

  def get_vessel(self, label):
    return self.madpt.get_vessel(label)

  def get_vessels(self, klass=vessels.Vessel, content=None):
    return self.madpt.get_vessels(klass, content)

  def get_containers(self, klass=objects_collections.Container):
    return self.madpt.get_containers(klass)

  # GENOTYPING related utility functions
  # ====================================


  def delete_snp_marker_defitions_table(self):
    self.delete_table(self.gadpt.SNP_MARKER_DEFINITIONS_TABLE)

  def create_snp_marker_definitions_table(self):
    self.gadpt.create_snp_marker_definitions_table()

  def delete_snp_alignments_table(self):
    self.delete_table(self.gadpt.SNP_ALIGNMENT_TABLE)

  def create_snp_alignment_table(self):
    self.gadpt.create_snp_alignment_table()

  def delete_snp_markers_set_table(self):
    self.delete_table(self.gadpt.SNP_SET_DEF_TABLE)

  def create_snp_markers_set_table(self):
    self.gadpt.create_snp_markers_set_table()

  def delete_snp_set_table(self):
    self.delete_table(self.gadpt.SNP_SET_TABLE)

  def create_snp_set_table(self):
    self.gadpt.create_snp_set_table()

  def add_snp_marker_definitions(self, stream, op_vid, batch_size=50000):
    return self.gadpt.add_snp_marker_definitions(stream, op_vid, batch_size)

  def get_snp_marker_definitions(self, selector=None, batch_size=50000):
    return self.gadpt.get_snp_marker_definitions(selector, batch_size)

  def add_snp_alignments(self, stream, op_vid, batch_size=50000):
    return self.gadpt.add_snp_alignments(stream, op_vid, batch_size)

  def snp_markers_set_exists(self, maker, model, release):
    return self.madpt.snp_markers_set_exists(maker, model, release)

  def get_snp_markers_set(self, maker, model, release):
    "returns a SNPMarkersSet object"
    return self.madpt.get_snp_markers_set(maker, model, release)

  def get_snp_markers_set_content(self, snp_markers_set, batch_size=50000):
    selector = '(vid=="%s")' % snp_markers_set.markersSetVID
    msetc = self.gadpt.get_snp_markers_set(selector, batch_size)
    selector = '|'.join(['(vid=="%s")' % mv for mv in msetc['marker_vid']])
    mdefs = self.gadpt.get_snp_marker_definitions(selector)
    return mdefs, msetc

  def add_snp_markers_set(self, maker, model, release, op_vid):
    return self.gadpt.add_snp_markers_set(maker, model, release, op_vid)

  def fill_snp_markers_set(self, set_vid, stream, op_vid, batch_size=50000):
    return self.gadpt.fill_snp_markers_set(set_vid, stream, op_vid, batch_size)

  def get_snp_alignments(self, selector=None, batch_size=50000):
    return self.gadpt.get_snp_alignments(selector, batch_size)

  def create_gdo_repository(self, set_vid, N):
    return self.gadpt.create_gdo_repository(set_vid, N)

  # Utility functions built as composition of the above
  # ====================================================

  def add_gdo_data_object(self, avid, sample, probs, confs):
    """
    FIXME
    """
    # if not isinstance(action, self.Action):
    #   raise ValueError('action should be an instance of Action')
    if not isinstance(sample, self.GenotypeDataSample):
      raise ValueError('sample should be an instance of GenotypeDataSample')
    # FIXME we delegate to gadpt checking that probs and confs have the
    #       right numpy dtype.
    mset = sample.snpMarkersSet
    tname, vid = self.gadpt.add_gdo(mset.markersSetVID, probs, confs, avid)

    size = 0
    sha1 = hashlib.sha1()
    s = probs.tostring();  size += len(s) ; sha1.update(s)
    s = confs.tostring();  size += len(s) ; sha1.update(s)

    conf = {'sample' : sample,
            'path'   : 'table:%s/vid=%s' % (tname, vid),
            'mimetype' : 'x-bl/gdo-table',
            'sha1'   : sha1.hexdigest(),
            'size'   : size,
            }
    gds = self.factory.create(self.DataObject, conf).save()
    return gds


  def get_gdo_iterator(self, mset, batch_size=100):
    """
    FIXME this is the basic object, we should have some support for
    selection.
    """
    return self.gadpt.get_gdo_iterator(mset.markersSetVID, batch_size)

  # EVA related utility functions
  # =============================

  def add_ehr_record(self, action, timestamp, archetype, rec):
    """

    FIXME multi fields rec will be exploded in a group of records all
    with the same (assuemed to be unique within KB) group id.

    :param action: action that generated this record
    :type action: ActionOnIndividual

    :param timestamp: when this record was collected, in millisecond
                      since the Epoch
    :type timestamp: long

    :param archetype: a legal archetype id, e.g.,
                      ``openEHR-EHR-EVALUATION.problem-diagnosis.v1``
    :type archetype:  str

    :param rec: key (at field code) and values for this specific archetype
                instance, e.g.::

                  {'at0002.1' :
                  'terminology://apps.who.int/classifications/apps/gE10.htm#E10'
                  }

    :type rec: dict

    """
    self.__check_type('action', self.ActionOnIndividual, action)
    self.__check_type('rec', dict, rec)
    action.reload()
    a_id = action.id
    target = action.target
    target.reload()
    i_id = target.id
    # FIXME NO CHECKS for archetypes consistency
    g_id = vlu.make_vid()
    for k in rec:
      row = {'timestamp' : timestamp,
             'i_vid' : i_id,
             'a_vid' : a_id,
             'valid' : True,
             'g_vid' : g_id,
             'archetype' : archetype,
             'field'     : k,
             'value'     : rec[k]}
      self.eadpt.add_eav_record_row(row)

  def get_ehr_records(self, selector=None):
    rows = self.eadpt.get_eav_record_rows(selector)
    if len(rows) == 0:
      return rows
    rows.sort(order='g_vid')
    # FIXME this is getting baroque
    recs = []
    g_vid = None
    x = {}
    fields = {}
    for r in rows:
      if not r[3]:
        continue
      if g_vid != r[4]:
        if g_vid:
          x['fields'] = fields
          recs.append(x)
        g_vid = r[4]
        x = {'timestamp'  : r[0],
             'i_id'      : r[1],
             'a_id'      : r[2],
             'archetype' : r[5]}
        fields = {}
      fields[r[6]] = self.eadpt.decode_field_value(r[7],
                                                   r[8], r[9], r[10], r[11])
    return recs

  def get_ehr_iterator(self, selector=None):
    "Get an iterator on all the ehr selected by selector."
    # FIXME this is a quick and dirty implementation.
    recs = self.get_ehr_records(selector)
    by_individual = {}
    for r in recs:
      by_individual.setdefault(r['i_id'], []).append(r)
    for k,v in by_individual.iteritems():
      yield (k, EHR(v))

