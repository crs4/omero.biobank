"""

FIXME

"""
# This is actually used in the meta class magic
import omero.model as om
import bl.vl.utils as vlu

from bl.vl.utils.snp import convert_to_top

from bl.vl.kb.dependency import DependencyTree

from proxy_core import ProxyCore

from wrapper import ObjectFactory, MetaWrapper

import snp_markers_set
import action
import vessels
import objects_collections
import data_samples
import actions_on_target
import individual
import location
import demographic


import hashlib, time, pwd, json, os


from genotyping import GenotypingAdapter
from modeling   import ModelingAdapter
from eav        import EAVAdapter
from ehr        import EHR
from genotyping import Marker

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
    # special case
    self.Marker = Marker
    #-- setup adapters
    self.gadpt = GenotypingAdapter(self)
    self.madpt = ModelingAdapter(self)
    self.eadpt = EAVAdapter(self)
    #-- depencency_tree service
    self.dt = None

  def __check_type(self, fname, ftype, val):
    if not isinstance(val, ftype):
      msg = 'bad type for %s(%s)' % (fname, val)
      raise ValueError(msg)

  def __resolve_action_id(self, action):
    """
    Utility function
    """
    if isinstance(action, self.Action):
      if not action.is_loaded():
        action.reload()
      avid = action.id
    else:
      avid = action
    return avid


  # High level ops
  # ==============
  def find_all_by_query(self, query, params):
    return super(Proxy, self).find_all_by_query(query, params, self.factory)

  def update_dependency_tree(self):
    self.dt = DependencyTree(self)


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

  def get_data_objects(self, sample):
    return self.madpt.get_data_objects(sample)

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

  def add_snp_marker_definitions(self, stream, action, batch_size=50000):
    """
    Save a stream of markers definitions.  For efficiency reasons,
    markers are written in batches, whose size is controlled by
    batch_size.

    .. code-block:: python

      taq_man_markers = [
        ('A0001', 'xrs122652',  'TCACTTCTTCAAAGCT[A/G]AGCTACAAGCATTATT'),
        ('A0002', 'xrs741592',  'GGAAGGAAGAAATAAA[C/G]CAGCACTATGTCTGGC'),
        ('A0003', 'xrs807079',  'CCGACCTAGTAGGCAA[A/G]TAGACACTGAGGCTGA'),
        ('A0004', 'xrs567736',  'AGGTCTATGTTAATAC[A/G]GAATCAGTTTCTCACC'),
        ('A0005', 'xrs4693427', 'AGATTACCATGCAGGA[A/T]CTGTTCTGAGATTAGC'),
        ('A0006', 'xrs4757019', 'TCTACCTCTGTGACTA[C/G]AAGTGTTCTTTTATTT'),
        ('A0007', 'xrs7958813', 'AAGGCAATACTGTTCA[C/T]ATTGTATGGAAAGAAG')
        ]
      def generator():
        for t in mark_defs:
          yield {'source' : source, 'context' :context, 'release' : release,
                 'label' : t[0], 'rs_label' : t[1],
                 'mask' : convert_to_top(t[2])
      vmap = kb.add_snp_marker_definitions(generator(), action)
      for x in vmap:
        print 'label: %s -> id: %s' % (x[0], x[1])

    :param stream: a stream of dict objects
    :type stream: generator

    :param action: a valid action, for backward compatibility reasons, it could
                   also be a VID string.
    :type action: Action

    :param batch_size: size of the batch written
    :type batch_size: positive int

    :return list: of (<label>, <vid>) tuples
    """
    op_vid = self.__resolve_action_id(action)
    return self.gadpt.add_snp_marker_definitions(stream, op_vid, batch_size)

  def get_snp_marker_definitions(self, selector=None, col_names=None,
                                 batch_size=50000):
    """
    Returns an array with the marker definitions that satisfy
    selector. If selector is None, returns all markers definitions. It
    is possible to request only specific columns of the markers
    definition by assigning to col_names a list with the names of
    the selected columns.

    .. code-block:: python

       selector = "(source == 'affymetrix') & (context == 'GW6.0')"

       col_names = ['vid', 'label']

       mrks = kb.get_snp_marker_definitions(selector, col_names)

    """
    return self.gadpt.get_snp_marker_definitions(selector, col_names,
                                                 batch_size)

  def get_snp_markers_by_source(self, source, context=None, release=None,
                                col_names=None):
    return self.gadpt.get_snp_markers_by_source(source, context=None,
                                                release=None,
                                                col_names=col_names)

  def get_snp_markers(self, labels=None, rs_labels=None, vids=None,
                      col_names=None,
                      batch_size=50000):
    return self.gadpt.get_snp_markers(labels, rs_labels, vids,
                                      col_names, batch_size)


  def add_snp_alignments(self, stream, op_vid, batch_size=50000):
    return self.gadpt.add_snp_alignments(stream, op_vid, batch_size)

  def snp_markers_set_exists(self, label=None,
                             maker=None, model=None, release=None):
    return self.madpt.snp_markers_set_exists(label, maker, model, release)

  def get_snp_markers_set(self, label=None,
                          maker=None, model=None, release=None):
    "returns a SNPMarkersSet object"
    return self.madpt.get_snp_markers_set(label, maker, model, release)

  def get_snp_markers_set_content(self, snp_markers_set, batch_size=50000):
    selector = '(vid=="%s")' % snp_markers_set.markersSetVID
    msetc = self.gadpt.get_snp_markers_set(selector, batch_size=batch_size)
    mdefs = self.get_snp_markers(vids=[mv for mv in msetc['marker_vid']],
                                 col_names=['vid', 'label'])
    return mdefs, msetc

  def add_snp_markers_set(self, maker, model, release, action):
    avid = self.__resolve_action_id(action)
    return self.gadpt.add_snp_markers_set(maker, model, release, avid)

  def fill_snp_markers_set(self, set_vid, stream, action, batch_size=50000):
    avid = self.__resolve_action_id(action)
    return self.gadpt.fill_snp_markers_set(set_vid, stream, avid, batch_size)

  def get_snp_alignments(self, selector=None, col_names=None, batch_size=50000):
    return self.gadpt.get_snp_alignments(selector, col_names, batch_size)

  def create_gdo_repository(self, set_vid, N):
    return self.gadpt.create_gdo_repository(set_vid, N)

  def get_gdo(self, set_vid, vid, indices=None):
    return self.gadpt.get_gdo(set_vid, vid, indices)

  # Syntactic sugar functions built as a composition of the above
  # =============================================================

  def create_an_action(self, study, target=None, doc='',
                       operator=None, device=None,
                       acat=None, options=None):
    """
    Syntactic sugar to simplify action creation.

    Unless explicitely provided, the action will use as its device the
    one identified by the label 'DEVICE-CREATE-AN-ACTION'.

    **Note:** this method is NOT supposed to be used in production
      code striving to be efficient. It is merely a convenience to
      simplify action creation in small scripts.
    """
    default_device_label = 'DEVICE-CREATE-AN-ACTION'
    alabel = ('auto-created-action%f' % (time.time()))
    asetup = self.factory.create(self.ActionSetup,
                                 {'label' : alabel,
                                  'conf' : json.dumps(options)})

    acat = acat if acat else self.ActionCategory.IMPORT
    if not target:
      a_klass = self.Action
    elif isinstance(target, self.Vessel):
      a_klass = self.ActionOnVessel
    elif isinstance(target, self.DataSample):
      a_klass = self.ActionOnDataSample
    elif isinstance(target, self.Individual):
      a_klass = self.ActionOnIndividual
    else:
      assert False
    operator = operator if operator else pwd.getpwuid(os.geteuid())[0]

    device = self.get_device(default_device_label)
    if not device:
      conf = {'label' : default_device_label,
              'maker' : 'CRS4',
              'model' : 'fake-device',
              'release' : 'create_an_action'}
      device = self.factory.create(self.Device, conf).save()

    conf = {'setup' : asetup,
            'device': device,
            'actionCategory' : acat,
            'operator' : operator,
            'context'  : study,
            'target' : target
            }
    action = self.factory.create(a_klass, conf).save()
    action.unload()
    return action


  def create_markers(self, source, context, release, stream, action):
    """
    Given a stream of tuples (label, rs_label, mask), will create and
    save the associated markers objets and return the label, vid
    association as a list of tuples.

    .. code-block:: python

      taq_man_markers = [
        ('A0001', 'xrs122652',  'TCACTTCTTCAAAGCT[A/G]AGCTACAAGCATTATT'),
        ('A0002', 'xrs741592',  'GGAAGGAAGAAATAAA[C/G]CAGCACTATGTCTGGC')]

      source, context, release = 'foobar', 'fooctx', 'foorel'
      lvs = kb.create_markers(source, context, release, taq_man_markers, action)
      for tmm, t in zip(taq_man_markers, lvs):
        assert (tmm[0] == t[0])
        print 'label:%s -> vid: %s' % (t[0], t[1])

    .. todo::

        add param docs.

    """
    # FIXME this is extremely inefficient,
    marker_defs = [t for t in stream]
    marker_labels = [t[0] for t in marker_defs]
    if len(marker_labels) > len(set(marker_labels)):
      raise ValueError('duplicate marker definitions in stream')

    old_markers = self.get_snp_markers(labels=marker_labels)
    if len(old_markers) > 0:
      if len(old_markers) < 10:
        msg = 'redefined markers: %s ' % [t.label for t in old_markers]
      else:
        msg = 'there are %s redefined markers' % len(old_markers)
      raise ValueError(msg)
    def generator(mdefs):
      for t in mdefs:
        yield {'source' : source,
               'context' :context,
               'release' : release,
               'label' : t[0],
               'rs_label' : t[1],
               'mask' : convert_to_top(t[2])}
    label_vid_list = self.add_snp_marker_definitions(generator(marker_defs),
                                                     action)
    return label_vid_list

  def save_snp_markers_alignments(self, ref_genome, stream, action):
    """
    Given a stream of five values tuples, will save allignment
    information of markers against a reference genome. The tuple field
    are, respectively, the marker vid, the chromosome number (with 23
    for X, 24 for Y, 25 for XY and 26 MT), a boolean that indicates if
    the marker alligns on the 5' strand (True), the allele seen on the
    reference genome and the number of times the given marker has been
    seen on the reference genome. If the latter is N larger than 1,
    there should be N records pertaining to the same marker.

    .. code-blocK:: python

        s = [('V8238983', 1, 200, True, 'A', 1),
             ('V8238983', 2, 300, True, 'B', 1),
             ('V8238983', 4, 400, True, 'A', 1),
             ('V8238983', 2, 400, True, 'A', 2)]

        kb.save_snp_markers_alignments('hg19', s, action)

    """
    # FIXME no checking....
    def generator(s):
      for x in s:
        y = {'marker_vid' : x[0], 'ref_genome' : ref_genome,
             'chromosome' : x[1], 'position' : x[2],
             'global_pos' : (x[1]*10**10 + x[2]),
             'strand' : x[3],
             'allele' : x[4],
             'copies' : x[5]}
        yield y
    self.add_snp_marker_definitions(generator(stream), action.id)

  def create_snp_markers_set(self, label, maker, model, release,
                             stream, action):
    """
    Given a stream of tuples (marker_vid, marker_indx, allele_flip),
    will build and save a new marker set.

    **NOTE:** with the current implementation, this is not an atomic
      op. FIXME so?

    .. todo::

        add param docs.

    """
    set_vid = 'V99999' # temp value
    conf = {'label': label,
            'maker' : maker, 'model' : model, 'release' : release,
            'markersSetVID' : set_vid,
            'action' : action}
    mset = self.factory.create(self.SNPMarkersSet, conf).save()
    # FIXME: the following is a brutal attempt to exception
    # containment, it should be refined.
    mlist = [t for t in stream]
    markers = self.get_snp_markers(vids=[t[0] for t in mlist])
    if len(markers) != len(mlist):
      raise ValueError('there are unknown markers in stream')
    if len(set((t[1] for t in mlist))) != len(mlist):
      raise ValueError('not unique marker_indx')

    def generator(stream):
      for t in stream:
        yield {'marker_vid' : t[0], 'marker_indx' : t[1],
               'allele_flip' : t[2]}
    try:
      set_vid = self.add_snp_markers_set(maker, model, release, action)
      N = self.fill_snp_markers_set(set_vid, generator(mlist), action)
      self.create_gdo_repository(set_vid, N)
    except Exception as e:
      self.delete(mset)
      raise e
    mset.markersSetVID = set_vid
    mset.save()
    return mset

  def update_snp_positions(self, markers, ref_genome, batch_size=50000):
    vids = [m.id for m in markers]
    res = self.gadpt.get_snp_alignment_positions(ref_genome, vids,
                                                 batch_size)
    if not res:
      raise ValueError('missing markers alignments')

    for m, r in it.izip(markers, res):
      m.position = r

  def get_individuals(self, group):
    """
    Syntactic sugar to simplify the looping on individuals contained in a group.
    The idea is that it should be possible to do the following:

    .. code-block:: python

      for i in kb.get_individuals(study):
        for d in kb.get_data_samples(i, dsample_klass_name):
          gds = filter(lambda x: x.snpMarkersSet == mset)


    :param group: a study object, we will be looping on all the
                  Individual enrolled in it.
    :type group: Study

    :type return: generator

    """
    return (e.individual for e in self.get_enrolled(group))

  def get_data_samples(self, individual, data_sample_klass_name='DataSample'):
    """
    Syntactic sugar to simplify the looping on DataSample connected to
    an individual. The idea is that it should be possible to do the
    following:

    .. code-block:: python

      for i in kb.get_individuals(study):
        for d in kb.get_data_samples(i, 'GenotypeDataSample'):
          gds = filter(lambda x: x.snpMarkersSet == mset)

    :param individual: the root individual object.
    :type group: Individual

    :param data_sample_klass_name: the name of the selected data_sample
                                   class, e.g. 'AffymetrixCel' or
                                   'GenotypeDataSample'
    :type data_sample_klass_name: str

    :type return: generator of a sequence of DataSample objects

    **Note:** in the current implementation, it has to do an expensive,
    both in memory and cpu time initialization the first time it is called.
    """
    if not self.dt:
      self.update_dependency_tree()
    klass = getattr(self, data_sample_klass_name)
    return (d for d in self.dt.get_connected(individual, aklass=klass))



  def add_gdo_data_object(self, action, sample, probs, confs):
    """
    Syntactic sugar to simplify adding genotype data objects.

    FIXME


    :param probs: a 2x<nmarkers> array with the AA and the BB
                  homozygote probs.
    :type probs: numpy.darray

    :param confs: a <nmarkers> array with the confidence on probs above.
    :type probs: numpy.darray

    """
    avid = self.__resolve_action_id(action)
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

  def get_gdo_iterator(self, mset,
                       data_samples=None,
                       indices = None,
                       batch_size=100):
    """
    FIXME this is the basic object, we should have some support for
    selection.
    """
    def get_gdo_iterator_on_list(dos):
      for do in dos:
        table, vid = do.path.split('=')
        mset_vid = table[:-3].split(':')[1]
        print 'mset_vid: %s, mset.markersSetVID: %s' % (mset_vid,
                                                        mset.markersSetVID)
        # if mset_vid != mset.markersSetVID:
        #   raise ValueError('DataObject %s map to data with a wrong SNPMarkersSet'
        #                    % (do.path))
        # yield self.get_gdo(mset.markersSetVID, vid, indices)
        yield do.path

    if not data_samples:
      return self.gadpt.get_gdo_iterator(mset.markersSetVID, indices,
                                         batch_size)
    ids = ','.join('%s' % ds.omero_id for ds in data_samples)
    query = 'from DataObject do where do.sample.id in (%s)' % ids
    dos = self.find_all_by_query(query, None)
    return get_gdo_iterator_on_list(dos)

  # EVA related utility functions
  # =============================

  def create_ehr_tables(self):
    self.eadpt.create_ehr_table()

  def delete_ehr_tables(self):
    self.delete_table(self.eadpt.EAV_EHR_TABLE)


  def add_ehr_record(self, action, timestamp, archetype, rec):
    """

    FIXME multi fields rec will be exploded in a group of records all
    with the same (assumed to be unique within a KB) group id.

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
    else:
      if g_vid:
        x['fields'] = fields
        recs.append(x)

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

  def get_ehr(self, individual):
    "Get the available ehr for an individual"
    recs = self.get_ehr_records(selector='(i_vid=="%s")' % individual.id)
    return EHR(recs)
