# BEGIN_COPYRIGHT
# END_COPYRIGHT

import json, logging

from bl.vl.kb import KnowledgeBase as KB


class Core(object):

  def __init__(self, host=None, user=None, passwd=None, group=None,
               keep_tokens=1, study_label=None, logger=None):
    self.kb = KB(driver='omero')(host, user, passwd, group, keep_tokens)
    self.logger = logger if logger else logging.getLogger()
    self.record_counter = 0
    self.default_study = None
    if study_label:
      s = self.kb.get_study(study_label)
      if not s:
        raise ValueError('No known study with label %s' % study_label)
      self.logger.info('Selecting %s[%d,%s] as default study' %
                       (s.label, s.omero_id, s.id))
      self.default_study = s

  @classmethod
  def find_action_setup_conf(klass, args):
    action_setup_conf = {}
    for x in dir(args):
      if not (x.startswith('_') or x.startswith('func')):
        action_setup_conf[x] = getattr(args, x)
    # HACKS
    action_setup_conf['ifile'] = action_setup_conf['ifile'].name
    action_setup_conf['ofile'] = action_setup_conf['ofile'].name
    return action_setup_conf

  def get_device(self, label, maker, model, release):
    device = self.kb.get_device(label)
    if not device:
      self.logger.debug('creating a device')
      conf = {
        'maker': maker,
        'model': model,
        'release': release,
        'label': label,
        }
      device = self.kb.factory.create(self.kb.Device, conf).save()
    return device

  def get_action_setup(self, label, conf):
    """
    Return the ActionSetup corresponding to label if there is one,
    else create a new one using conf.
    """
    asetup = self.kb.get_action_setup(label)
    if not asetup:
      kb_conf = {
        'label': label,
        'conf': json.dumps(conf),
        }
      asetup = self.kb.factory.create(self.kb.ActionSetup, kb_conf).save()
    return asetup

  def get_study(self, label):
    if self.default_study:
      return self.default_study
    study = self.kb.get_study(label)
    if not study:
      study = self.kb.factory.create(self.kb.Study, {'label': label}).save()
    return study

  def find_study(self, records):
    study_label = records[0]['study']
    for r in records:
      if r['study'] != study_label:
        m = 'all records should have the same study label'
        self.logger.critical(m)
        raise ValueError(m)
    return self.get_study(study_label)

  def find_klass(self, col_name, records):
    o_type = records[0][col_name]
    for r in records:
      if r[col_name] != o_type:
        m = 'all records should have the same %s' % col_name
        self.logger.critical(m)
        raise ValueError(m)
    return getattr(self.kb, o_type)

  def preload_by_type(self, name, klass, preloaded):
    self.logger.info('start preloading %s' % name)
    objs = self.kb.get_objects(klass)
    for o in objs:
      assert not o.id in preloaded
      preloaded[o.id] = o
    self.logger.info('done preloading %s' % name)

  def missing_fields(self, fields, r):
    for f in fields:
      if f not in r:
        return f
    return False


class RecordCanonizer(object):

  def __init__(self, fields, args):
    overrides = {}
    for f in fields:
      v = getattr(args, f, None)
      if v is not None:
        overrides[f] = v
    self.overrides = overrides
  
  def canonize(self, r):
    r.update(self.overrides)

  def canonize_list(self, l):
    for r in l:
      self.canonize(r)
