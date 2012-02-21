# BEGIN_COPYRIGHT
# END_COPYRIGHT

import hashlib, time, pwd, json, os
import itertools as it

# This is actually used in the metaclass magic
import omero.model as om

import bl.vl.utils as vlu
from bl.vl.utils.snp import convert_to_top
from bl.vl.kb.dependency import DependencyTree
from bl.vl.kb import mimetypes

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
from genotyping import GenotypingAdapter
from modeling import ModelingAdapter
from eav import EAVAdapter
from ehr import EHR
from genotyping import Marker
from admin import Admin


KOK = MetaWrapper.__KNOWN_OME_KLASSES__
BATCH_SIZE = 5000


class Proxy(ProxyCore):
  """
  An OMERO driver for the knowledge base.
  """
  def __init__(self, host, user, passwd, group=None, session_keep_tokens=1):
    super(Proxy, self).__init__(host, user, passwd, group, session_keep_tokens)
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
    self.admin = Admin(self)
    #-- depencency_tree service
    self.dt = None

  def __check_type(self, fname, ftype, val):
    if not isinstance(val, ftype):
      msg = 'bad type for %s(%s)' % (fname, val)
      raise ValueError(msg)

  def __resolve_action_id(self, action):
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

  def get_by_vid(self, klass, vid):
    query = "from %s o where o.vid = :vid" % klass.get_ome_table()
    params = {"vid": vid}
    res = self.find_all_by_query(query, params)
    if len(res) != 1:
      raise ValueError("%d kb objects map to %s" % (len(res), vid))
    return res[0]

  def update_dependency_tree(self):
    self.dt = DependencyTree(self)

  # Modeling-related utility functions
  # ==================================

  def get_device(self, label):
    return self.madpt.get_device(label)

  def get_action_setup(self, label):
    return self.madpt.get_action_setup(label)

  def get_study(self, label):
    return self.madpt.get_study(label)

  def get_data_collection(self, label):
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

  def get_data_sample(self, label):
    return self.madpt.get_data_sample(label)

  def get_vessels(self, klass=vessels.Vessel, content=None):
    return self.madpt.get_vessels(klass, content)

  def get_containers(self, klass=objects_collections.Container):
    return self.madpt.get_containers(klass)

  def get_container(self, label):
    return self.madpt.get_container(label)

  def get_data_objects(self, sample):
    return self.madpt.get_data_objects(sample)

  # Genotyping-related utility functions
  # ====================================

  def delete_snp_marker_defitions_table(self):
    self.delete_table(self.gadpt.SNP_MARKER_DEFINITIONS_TABLE)

  def create_snp_marker_definitions_table(self):
    self.gadpt.create_snp_marker_definitions_table()

  def add_snp_marker_definitions(self, stream, action, batch_size=BATCH_SIZE):
    """
    Save a stream of marker definitions. For efficiency reasons,
    markers are written in batches, whose size is controlled by
    batch_size.

    :param stream: a stream of dict objects
    :type stream: generator

    :param action: a valid action. For backward compatibility reasons, it can
      also be a VID.
    :type action: Action

    :param batch_size: size of the batch to write
    :type batch_size: positive int

    :return: list of (<label>, <vid>) tuples
    """
    op_vid = self.__resolve_action_id(action)
    return self.gadpt.add_snp_marker_definitions(stream, op_vid, batch_size)

  def get_snp_marker_definitions(self, selector=None, col_names=None,
                                 batch_size=BATCH_SIZE):
    """
    Return an array with the marker definitions that satisfy
    selector. If selector is None, return all marker definitions. It
    is possible to request only specific marker definition columns by
    assigning to col_names a list with the names of the selected
    columns.
    """
    return self.gadpt.get_snp_marker_definitions(selector, col_names,
                                                 batch_size)

  def get_snp_markers_by_source(self, source, context=None, release=None,
                                batch_size=BATCH_SIZE):
    return self.gadpt.get_snp_markers_by_source(
      source, context, release, batch_size
      )

  def get_snp_markers(self, labels=None, rs_labels=None, vids=None,
                      batch_size=BATCH_SIZE, col_names=None):
    return self.gadpt.get_snp_markers(
      labels, rs_labels, vids, batch_size, col_names
      )

  #--- snp_markers_set ---
  def create_snp_markers_set(self, label, maker, model, release,
                             N, stream, action):
    """
    Given a stream of (marker_vid, marker_indx, allele_flip) tuples,
    build and save a new marker set.
    """
    assert type(N) == int and N > 0
    set_vid = vlu.make_vid()
    conf = {
      'label': label,
      'maker': maker,
      'model': model,
      'release': release,
      'markersSetVID': set_vid,
      'action': action,
      }
    mset = self.factory.create(self.SNPMarkersSet, conf)
    mset.save()
    def gen(stream):
      for t in stream:
        yield {'marker_vid': t[0], 'marker_indx': t[1], 'allele_flip': t[2]}
    # TODO: add better exception handling to the following code
    try:
      self.gadpt.create_snp_markers_set_tables(mset.id, N)
      counted = self.gadpt.define_snp_markers_set(set_vid, gen(stream),
                                                  action.id)
      if counted != N:
        raise ValueError('there are %d records in stream (expected %d)' %
                         (counted, N))
    except:
      self.gadpt.delete_snp_markers_set_tables(mset.id)
      self.delete(mset)
      raise
    return mset

  def align_snp_markers_set(self, mset, ref_genome, stream, action):
    """
    Given a stream of six-element tuples, save alignment information
    of markers wrt a reference genome.

    Tuple elements are, respectively:
    
      #. the marker vid;
      #. the chromosome number (23=X, 24=Y, 25=XY, 26=MT);
      #. the position within the chromosome;
      #. a boolean that's True if the marker aligns on the 5' strand;
      #. the allele seen on the reference genome;
      #. the number of times this marker has been seen on the
         reference genome. If the latter is larger than 1, there
         should be N records for this marker.
    """
    # FIXME no checking
    global_pos = snp_markers_set.SNPMarkersSet.compute_global_position
    def gen(s):
      for x in s:
        y = {
          'marker_vid': x[0],
          'ref_genome': ref_genome,
          'chromosome': x[1],
          'pos': x[2],
          'global_pos': global_pos((x[1],x[2])),
          'strand': x[3],
          'allele': x[4],
          'copies': x[5],
          }
        yield y
    max_len = self.gadpt.SNP_ALIGNMENT_COLS[1][3]
    if len(ref_genome) > max_len:
      raise ValueError('len("%s") > %d' % (ref_genome, max_len))
    self.gadpt.add_snp_markers_set_alignments(mset.id, gen(stream), action.id)

  def make_gdo_path(self, mset, vid):
    table_name = self.gadpt.snp_markers_set_table_name('gdo', mset.id)
    return 'table:%s/vid=%s' % (table_name, vid)

  def parse_gdo_path(self, path):
    head, vid = path.split('/vid=')
    head = head[len('table:'):]
    tag, set_vid = self.gadpt.snp_markers_set_table_name_parse(head)
    return set_vid, vid

  def add_gdo_data_object(self, action, sample, probs, confs):
    """
    Syntactic sugar to simplify adding genotype data objects.

    :param probs: a 2x<nmarkers> array with the AA and the BB
      homozygous probabilities.
    :type probs: numpy.darray

    :param confs: a <nmarkers> array with the confidence on the above
      probabilities.
    :type probs: numpy.darray

    """
    avid = self.__resolve_action_id(action)
    if not isinstance(sample, self.GenotypeDataSample):
      raise ValueError('sample should be an instance of GenotypeDataSample')
    mset = sample.snpMarkersSet
    # FIXME doesn't check that probs and confs have the right dtype and size
    gdo_vid = self.gadpt.add_gdo(mset.id, probs, confs, avid)
    size = 0
    sha1 = hashlib.sha1()
    s = probs.tostring();  size += len(s) ; sha1.update(s)
    s = confs.tostring();  size += len(s) ; sha1.update(s)
    conf = {
      'sample': sample,
      'path': self.make_gdo_path(mset, gdo_vid),
      'mimetype': mimetypes.GDO_TABLE,
      'sha1': sha1.hexdigest(),
      'size': size,
      }
    gds = self.factory.create(self.DataObject, conf).save()
    return gds

  def get_gdo(self, mset, vid, indices=None):
    return self.gadpt.get_gdo(mset.id, vid, indices)


  #FIXME this is the basic object, we should have some support for selections
  def get_gdo_iterator(self, mset, data_samples=None, indices = None,
                       batch_size=100):
    def get_gdo_iterator_on_list(dos):
      seen_data_samples = set([])
      for do in dos:
        # FIXME we could, in principle, handle other mimetypes too
        if do.mimetype == mimetypes.GDO_TABLE:
          mset_vid, vid = self.parse_gdo_path(do.path)
          if mset_vid != mset.id:
            raise ValueError(
              'DataObject %s map to data with a wrong SNPMarkersSet' % do.path
              )
          yield self.get_gdo(mset, vid, indices)
        else:
          pass
    if data_samples is None:
      return self.gadpt.get_gdo_iterator(mset.id, indices, batch_size)
    for d in data_samples:
      if d.snpMarkersSet != mset:
        raise ValueError('data_sample %s snpMarkersSet != mset' % d.id)
    ids = ','.join('%s' % ds.omero_id for ds in data_samples)
    query = 'from DataObject do where do.sample.id in (%s)' % ids
    dos = self.find_all_by_query(query, None)
    return get_gdo_iterator_on_list(dos)

  def get_snp_markers_set(self, label=None,
                          maker=None, model=None, release=None):
    return self.madpt.get_snp_markers_set(label, maker, model, release)

  def snp_markers_set_exists(self, label, maker, model, release):
    """
    DEPRECATED
    """
    return not self.get_snp_markers_set(label, maker, model, release) is None


  # Syntactic sugar functions built as a composition of the above
  # =============================================================

  def create_an_action(self, study, target=None, doc='', operator=None,
                       device=None, acat=None, options=None):
    """
    Syntactic sugar to simplify action creation.

    Unless explicitely provided, the action will use as its device the
    one identified by the label 'DEVICE-CREATE-AN-ACTION'.

    **Note:** this method is NOT supposed to be used in production
    code. It is merely a convenience to simplify action creation in
    small scripts.
    """
    default_device_label = 'DEVICE-CREATE-AN-ACTION'
    alabel = ('auto-created-action%f' % (time.time()))
    asetup = self.factory.create(
      self.ActionSetup, {'label': alabel, 'conf': json.dumps(options)}
      )
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
      conf = {
        'label': default_device_label,
        'maker': 'CRS4',
        'model': 'fake-device',
        'release': 'create_an_action',
        }
      device = self.factory.create(self.Device, conf).save()
    conf = {
      'setup': asetup,
      'device': device,
      'actionCategory': acat,
      'operator': operator,
      'context': study,
      'target': target,
      }
    action = self.factory.create(a_klass, conf).save()
    action.unload()
    return action

  def create_markers(self, source, context, release,
                     ref_rs_genome, dbsnp_build, stream, action):
    """
    Given a stream of (label, rs_label, mask) tuples, create and save
    Marker objects and return the (label, vid) associations as a list
    of tuples.
    """
    # FIXME this is extremely inefficient
    marker_defs = [t for t in stream]
    marker_labels = [t[0] for t in marker_defs]
    if len(marker_labels) > len(set(marker_labels)):
      raise ValueError('duplicate marker definitions in stream')
    def generator(mdefs):
      for t in mdefs:
        yield {
          'source': source,
          'context' :context,
          'release': release,
          'ref_rs_genome': ref_rs_genome,
          'dbSNP_build': dbsnp_build,
          'label': t[0],
          'rs_label': t[1],
          'mask': convert_to_top(t[2]),
          }
    label_vid_list = self.add_snp_marker_definitions(generator(marker_defs),
                                                     action)
    return label_vid_list

  def get_individuals(self, group):
    """
    Syntactic sugar to simplify the looping on individuals contained
    in a group.

    :param group: a study object, we will be looping on all the
                  Individual(s) enrolled in it.
    :type group: Study

    :type return: generator
    """
    return (e.individual for e in self.get_enrolled(group))

  def get_data_samples(self, individual, data_sample_klass_name='DataSample'):
    """
    Syntactic sugar to simplify the looping on DataSample(s) connected
    to an individual.

    :param individual: the root individual object
    :type group: Individual

    :param data_sample_klass_name: the name of the selected data_sample
      class, e.g. 'AffymetrixCel' or 'GenotypeDataSample'
    :type data_sample_klass_name: str

    :type return: generator of a sequence of DataSample objects

    **Note:** the current implementation does an expensive initialization,
    both in memory and cpu time, when it's called for the first time.
    """
    if not self.dt:
      self.update_dependency_tree()
    klass = getattr(self, data_sample_klass_name)
    return (d for d in self.dt.get_connected(individual, aklass=klass))

  def get_vessels_by_individual(self, individual, vessel_klass_name='Vessel'):
    """
    Syntactic sugar to simplify the looping in Vessels connected to an
    individual.

    :param individual: the root individual object
    :type group: Individual

    :param vessel_klass_name: the name of the selected vessel class,
      e.g. 'Vial' or 'PlateWell'
    :type vessel_klass_name: str

    :type return: generator of a sequence of Vessel objects
    """
    klass = getattr(self, vessel_klass_name)
    if not issubclass(klass, getattr(self, 'Vessel')):
      raise ValueError('klass should be a subclass of Vessel')
    if not self.dt:
      self.update_dependency_tree()
    return (v for v in self.dt.get_connected(individual, aklass=klass))


  # EVA-related utility functions
  # =============================

  def create_ehr_tables(self):
    self.eadpt.create_ehr_table()

  def delete_ehr_tables(self):
    self.delete_table(self.eadpt.EAV_EHR_TABLE)

  def add_ehr_record(self, action, timestamp, archetype, rec):
    """
    multi-field records will be expanded to groups of records all
    with the same (assumed to be unique within a KB) group id.

    :param action: action that generated this record
    :type action: ActionOnIndividual

    :param timestamp: when this record was collected, in millisecond
      since the Epoch
    :type timestamp: long

    :param archetype: a legal archetype id, e.g.,
      ``openEHR-EHR-EVALUATION.problem-diagnosis.v1``
    :type archetype:  str

    :param rec: keys and values for this specific archetype instance,
      e.g., ``{'at0002.1':
      'terminology://apps.who.int/classifications/apps/gE10.htm#E10'}``

    :type rec: dict
    """
    self.__check_type('action', self.ActionOnIndividual, action)
    self.__check_type('rec', dict, rec)
    action.reload()
    a_id = action.id
    target = action.target
    target.reload()
    i_id = target.id
    # TODO add archetype consistency checks
    g_id = vlu.make_vid()
    for k in rec:
      row = {
        'timestamp': timestamp,
        'i_vid': i_id,
        'a_vid': a_id,
        'valid': True,
        'g_vid': g_id,
        'archetype': archetype,
        'field': k,
        'value': rec[k],
        }
      self.eadpt.add_eav_record_row(row)

  def get_ehr_records(self, selector='(valid == True)'):
    rows = self.eadpt.get_eav_record_rows(selector)
    if len(rows) == 0:
      return rows
    rows.sort(order='g_vid')
    recs = []
    g_vid = None
    x = {}
    fields = {}
    for r in rows:
      if g_vid != r[4]:
        if g_vid:
          x['fields'] = fields
          recs.append(x)
        g_vid = r[4]
        x = {
          'timestamp': r[0],
          'i_id': r[1],
          'a_id': r[2],
          'archetype': r[5],
          'valid' : r[3],
          }
        fields = {}
      fields[r[6]] = self.eadpt.decode_field_value(
        r[7], r[8], r[9], r[10], r[11]
        )
    else:
      if g_vid:
        x['fields'] = fields
        recs.append(x)
    return recs

  def __build_ehr_selector(self, individual_id, timestamp, archetype_id,
                           grouper_id, valid, archetype, field, field_value):
    selector = []
    if individual_id:
      selector.append('(i_vid == "%s")' % individual_id)
    if timestamp:
      selector.append('(timestamp == %d)' % timestamp)
    if archetype_id:
      selector.append('(a_vid == "%s")' % archetype_id)
    if grouper_id:
      selector.append('(g_vid == "%s")' % grouper_id)
    if not valid is None:
      selector.append('(valid == %s)' % valid)
    if archetype:
      selector.append('(archetype == "%s")' % archetype)
    if field :
      selector.append('(field == "%s")' % field)
    if not field_value is None:
      ftype, fval, defval = self.eadpt.FIELD_TYPE_ENCODING_TABLE[type(field_value).__name__]
      selector.append('(type == "%s")' % ftype)
      if ftype == 'str':
        selector.append('(%s == "%s")' % (fval, field_value))
      else:
        selector.append('(%s == %s)' % (fval, field_value))
    return ' & '.join(selector)

  def invalidate_ehr_records(self, individual_id, timestamp = None,
                             archetype_id = None, grouper_id = None,
                             archetype = None, field = None, field_value = None):
    selector = self.__build_ehr_selector(individual_id, timestamp, archetype_id,
                                         grouper_id, True, archetype, field,
                                         field_value)
    self.update_table_rows(self.eadpt.EAV_EHR_TABLE, selector, {'valid' : False})

  def validate_ehr_records(self, individual_id, timestamp = None,
                             archetype_id = None, grouper_id = None,
                             archetype = None, field = None, field_value = None):
    selector = self.__build_ehr_selector(individual_id, timestamp, archetype_id,
                                         grouper_id, False, archetype, field,
                                         field_value)
    self.update_table_rows(self.eadpt.EAV_EHR_TABLE, selector, {'valid' : True})

  def get_ehr_iterator(self, selector=None):
    # FIXME this is a quick and dirty implementation.
    recs = self.get_ehr_records(selector)
    by_individual = {}
    for r in recs:
      by_individual.setdefault(r['i_id'], []).append(r)
    for k,v in by_individual.iteritems():
      yield (k, EHR(v))

  def get_ehr(self, individual):
    recs = self.get_ehr_records(selector='(i_vid=="%s")' % individual.id)
    return EHR(recs)
