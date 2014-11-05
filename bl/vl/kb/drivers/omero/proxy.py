# BEGIN_COPYRIGHT
# END_COPYRIGHT

import hashlib, time, pwd, json, os
from importlib import import_module

# This is actually used in the metaclass magic
import omero.model as om

import bl.vl.utils as vlu
import bl.vl.kb.config as blconf
from bl.vl.kb.messages import get_events_sender
from bl.vl.kb.dependency import DependencyTree
from bl.vl.kb import mimetypes

from proxy_core import ProxyCore
from wrapper import ObjectFactory, MetaWrapper
import action
import vessels
import objects_collections
import data_samples
import actions_on_target
import individual
import location
import demographic
import sequencing
import snp_markers_set
import variant_call_support
import affymetrix_chips
import illumina_chips

from genomics import GenomicsAdapter
from modeling import ModelingAdapter
from context_manager import ContextManagerAdapter
from eav import EAVAdapter
from ehr import EHR

from admin import Admin


EXTRA_MODULES_ENV = 'OMERO_BIOBANK_EXTRA_MODULES'
NO_VCHECK_ENV = 'OMERO_BIOBANK_NO_VCHECK'

KOK = MetaWrapper.__KNOWN_OME_KLASSES__
BATCH_SIZE = 5000


class Proxy(ProxyCore):
  """
  An OMERO driver for the knowledge base.
  """
  def __init__(self, host, user, passwd, group=None, session_keep_tokens=1,
               check_ome_version=True, extra_modules=None):
    if os.getenv(NO_VCHECK_ENV):
      check_ome_version = False
    super(Proxy, self).__init__(host, user, passwd, group, session_keep_tokens,
                                check_ome_version)
    extra_modules = extra_modules or os.getenv(EXTRA_MODULES_ENV)
    if extra_modules:
      if isinstance(extra_modules, basestring):
        extra_modules = extra_modules.split(",")
      for name in extra_modules:
        if "." not in name:
          name = "%s.%s" % (__package__, name)
        try:
          import_module(name)
        except ImportError:
          raise ImportError('Optional module "%s" not available' % name)
    self.factory = ObjectFactory(proxy=self)
    #-- learn
    for k in KOK:
      klass = KOK[k]
      setattr(self, klass.get_ome_table(), klass)
    #-- setup adapters
    self.genomics = GenomicsAdapter(self)
    self.madpt = ModelingAdapter(self)
    self.context = ContextManagerAdapter(self)
    self.eadpt = EAVAdapter(self)
    self.admin = Admin(self)
    self.events_sender = get_events_sender(self.logger)
    self.dt = DependencyTree(self)

  def __check_type(self, fname, ftype, val):
    if not isinstance(val, ftype):
      msg = 'bad type for %s(%s)' % (fname, val)
      raise ValueError(msg)

  def resolve_action_id(self, action):
    #FIXME hack to be able to use it from Genomics
    return self.__resolve_action_id(action)
    
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

  def get_by_field(self, klass, field_name, values, batch_size=240):
    """
    FIXME returns a dictionary that map all v in values
    for which exists an object o of class klass such that o.field_name == v
    to o.
    """
    def get_by_field_helper(values):
      template = "from %s o where o.{} in (%s)".format(field_name)
      query = template % (klass.get_ome_table(),
                          ','.join(map(lambda x: "'%s'" % x, values)))
      params = {}
      res = self.find_all_by_query(query, params)
      return dict(map(lambda o: (getattr(o, field_name), o), res))
    def values_by_chunk():
      offset = 0
      while len(values[offset:]) > 0:
        yield values[offset:offset+batch_size]
        offset += batch_size

    if len(values) == 0:
      return {}
    if batch_size == 0:
      return get_by_field_helper(values)
    else:
      return reduce(lambda x, y: x.update(y) or x,
                    map(get_by_field_helper, values_by_chunk()))

  def get_by_vids(self, klass, vids, batch_size=240):
    """
    FIXME Given a list of vids, returns a dictionary that map all vid
    in vids for which exists an object o of class klass such that o.vid == vid
    to o.
    """
    return self.get_by_field(klass, 'vid', vids, batch_size)

  def get_by_labels(self, klass, labels, batch_size=240):
    """
    FIXME Given a list of labels, returns a dictionary that map all
    label in labels for which exists an object o of class klass such
    that o.label == label, to o.
    """
    return self.get_by_field(klass, 'label', labels, batch_size)

  def get_by_label(self, klass, label):
    res = self.get_by_labels(klass, [label])
    if len(res) == 0:
      return None
    if not res.has_key(label):
      raise RuntimeError('bad result for %s with label %s' %
                         (klass, label))
    return res[label]

  def create_global_tables(self, destructive=False):
    self.eadpt.create_ehr_table(destructive=destructive)

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

  def get_vessels_collection(self, label):
    return self.madpt.get_vessels_collection(label)

  def get_data_collection_items(self, dc):
    return self.madpt.get_data_collection_items(dc)

  def get_vessels_collection_items(self, vc):
    return self.madpt.get_vessels_collection_items(vc)

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

  def get_seq_data_samples_by_tube(self, tube):
      return self.madpt.get_seq_data_samples_by_tube(tube)


  # Syntactic sugar functions built as a composition of the above
  # =============================================================

  def create_an_action(self, study=None, target=None, doc='', operator=None,
                       device=None, acat=None, options=None):
    """
    Syntactic sugar to simplify action creation.

    Unless explicitely provided, the action will use as its study the
    one identified by the label 'STUDY-CREATE-AN-ACTION'.
    
    Unless explicitely provided, the action will use as its device the
    one identified by the label 'DEVICE-CREATE-AN-ACTION'.

    **Note:** this method is NOT supposed to be used in production
    code. It is merely a convenience to simplify action creation in
    small scripts.
    """
    default_study_label  = 'STUDY-CREATE-AN-ACTION'
    default_device_label = 'DEVICE-CREATE-AN-ACTION'    
    alabel = ('auto-created-action%f' % (time.time()))
    asetup = self.factory.create(
      self.ActionSetup, {'label': alabel, 'conf': json.dumps(options)}
      ).save()
    acat = acat if acat else self.ActionCategory.IMPORT
    if not target:
      a_klass = self.Action
    elif isinstance(target, self.Vessel):
      a_klass = self.ActionOnVessel
    elif isinstance(target, self.DataSample):
      a_klass = self.ActionOnDataSample
    elif isinstance(target, self.Individual):
      a_klass = self.ActionOnIndividual
    elif isinstance(target, self.VLCollection):
        a_klass = self.ActionOnCollection
    else:
      assert False

    operator = operator if operator is not None\
                        else pwd.getpwuid(os.geteuid())[0]
    study = study if study is not None\
                  else self.get_study(default_study_label)
    if study is None:
      study = self.factory.create(self.Study, 
                                  {'label': default_study_label}).save()      
    device = device if device is not None\
                    else self.get_device(default_device_label)
    if device is None:
      device = self.create_device(default_device_label, 'CRS4',
                                  'fake-device', 'create_an_action')
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

  def create_device(self, label, maker, model, release):
    conf = {
      'maker' : maker,
      'model' : model,
      'release' : release,
      'label' : label,
      }
    return self.factory.create(self.Device, conf).save()

  def get_actions(self, target):
    """
    Get all Actions that have the *target* object as target
    """
    query = "SELECT act FROM %s act JOIN act.target AS trg WHERE trg.vid = :target_id"
    # select the proper action class
    act = ''
    if isinstance(target, self.Vessel):
      act = 'ActionOnVessel'
    elif isinstance(target, self.DataSample):
      act = 'ActionOnDataSample'
    elif isinstance(target, self.Individual):
      act = 'ActionOnIndividual'
    elif isinstance(target, self.VLCollection):
      act = 'ActionOnCollection'
    else:
      raise ValueError('Target %s has no a specific Action' % type(target))
    return self.find_all_by_query(query % act, {'target_id': target.id})

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
    :type individual: Individual

    :param data_sample_klass_name: the name of the selected data_sample
      class, e.g. 'AffymetrixCel' or 'GenotypeDataSample'
    :type data_sample_klass_name: str

    :type return: generator of a sequence of DataSample objects

    **Note:** the current implementation does an expensive initialization,
    both in memory and cpu time, when it's called for the first time.
    """
    klass = getattr(self, data_sample_klass_name)
    return (d for d in self.dt.get_connected(individual, aklass=klass))


  def get_vessels_by_individual(self, individual, vessel_klass_name='Vessel'):
    """
    Syntactic sugar to simplify the looping in Vessels connected to an
    individual.

    :param individual: the root individual object
    :type individual: Individual

    :param vessel_klass_name: the name of the selected vessel class,
      e.g. 'Vial' or 'PlateWell'
    :type vessel_klass_name: str

    :type return: generator of a sequence of Vessel objects
    """
    klass = getattr(self, vessel_klass_name)
    if not issubclass(klass, getattr(self, 'Vessel')):
      raise ValueError('klass should be a subclass of Vessel')
    return (v for v in self.dt.get_connected(individual, aklass=klass))

  def get_wells_by_plate(self, plate):
    """
    Syntactic sugar to simplify PlateWell retrival using a known TiterPlate

    :param plate: a known TiterPlate
    :type plate: TiterPlate

    :type return: generator of a sequence of PlateWell objects
    """
    query = '''
    SELECT pw FROM PlateWell pw
    JOIN pw.container AS pl
    WHERE pl.vid = :pl_vid
    '''
    wells = self.find_all_by_query(query, {'pl_vid' : plate.vid})
    return (w for w in wells)

  def get_well_on_plate(self, plate, row, column):
    """
    Syntactic sugar to retrieve a specif PlateWell from a given TiterPlate.

    :param plate: a known TiterPlate
    :type plate: TiterPlate
    
    :param row: the required well row (base 0)
    :type row: int

    :param column: the required well column (base 0)
    :type column: int

    :type return: the required PlateWell object if found, None otherwise
    """
    slot = (row - 1) * plate.columns + column
    res = filter(lambda x: x.slot == slot, self.get_wells_by_plate(plate))
    return res[0] if res else None

  def get_lanes_by_flowcell(self, flowcell):
    """
    Syntactic sugar to simplify Lane retrival using a known FlowCell

    :param flowcell: a known FlowCell
    :type flowcell: FlowCell

    :type return: generator of a sequence of Lane objects
    """
    query = '''
    SELECT l FROM Lane l
    JOIN l.flowCell AS fc
    WHERE fc.vid = :fc_vid
    '''
    lanes = self.find_all_by_query(query, {'fc_vid' : flowcell.vid})
    return (l for l in lanes)

  def get_laneslots_by_lane(self, lane):
    """
    Syntactic sugar to simplify LaneSlot retrival using a known Lane

    :param lane: a known Lane
    :type lane: Lane

    :type return: generator of a sequence of LaneSlot objects
    """
    query = '''
    SELECT ls FROM LaneSlot ls
    JOIN ls.lane AS l
    WHERE l.vid = :l_vid
    '''
    laneslots = self.find_all_by_query(query, {'l_vid' : lane.vid})
    return (ls for ls in laneslots)

  # EVA-related utility functions
  # =============================

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
          'g_id' : r[4],
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

  def get_birth_data(self, individual=None):
    selector = '(valid == True) & (archetype == "openEHR-DEMOGRAPHIC-CLUSTER.person_birth_data_iso.v1")'
    if individual:
      selector += ' & (i_vid == "%s")' % individual.id
    rows = self.get_ehr_records(selector)
    return rows

  def __build_ehr_selector(self, individual_id, timestamp, action_id,
                           grouper_id, valid, archetype, field, field_value):
    selector = []
    if individual_id:
      selector.append('(i_vid == "%s")' % individual_id)
    if timestamp:
      selector.append('(timestamp == %d)' % timestamp)
    if action_id:
      selector.append('(a_vid == "%s")' % action_id)
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
                             action_id = None, grouper_id = None,
                             archetype = None, field = None, field_value = None):
    selector = self.__build_ehr_selector(individual_id, timestamp, action_id,
                                         grouper_id, True, archetype, field,
                                         field_value)
    self.update_table_rows(self.eadpt.EAV_EHR_TABLE, selector, {'valid' : False})

  def validate_ehr_records(self, individual_id, timestamp = None,
                             action_id = None, grouper_id = None,
                             archetype = None, field = None, field_value = None):
    selector = self.__build_ehr_selector(individual_id, timestamp, action_id,
                                         grouper_id, False, archetype, field,
                                         field_value)
    self.update_table_rows(self.eadpt.EAV_EHR_TABLE, selector, {'valid' : True})

  def get_ehr_iterator(self, selector='(valid == True)'):
    # FIXME this is a quick and dirty implementation.
    recs = self.get_ehr_records(selector)
    by_individual = {}
    for r in recs:
      by_individual.setdefault(r['i_id'], []).append(r)
    for k,v in by_individual.iteritems():
      yield (k, EHR(v))

  def get_ehr(self, individual, get_invalid = False):
    if not get_invalid:
      recs = self.get_ehr_records(selector='(i_vid=="%s") & (valid == True)' % individual.id)
    else:
      recs = self.get_ehr_records(selector='(i_vid=="%s")' % individual.id)
    return EHR(recs)
