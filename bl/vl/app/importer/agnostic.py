# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
Agnostic loader
===============

FIXME
This is a know-nothing, do what you are told, record loader.  It
basically expects to be told what is the class of objects that should
be loaded, the attributes that should be assigned and if the objects
are obtained from an action defined on another object.

It expects an input file with a structure similar to the following::

importer agnostic --source PlateWell --object-type AffymetrixArray \
--object-fields="assayType,status"

  source   label assayType       status
  V9898989 cccc  GENOMEWIDESNP_6 CONTENTUSABLE


importer agnostic --source PlateWell --object-type IlluminaBeadChipArray \
--object-fields="assayType,container,status"

  source   label   assayType              container  status
  V9898989 R01C01  HumanOmni1_Quad_v1_0_B V9898989   CONTENTUSABLE

"""

import os, time, json, csv, copy, sys
import itertools as it

import core
from version import version


# FIXME this should not be here....
REQUIRED = 'required'
OPTIONAL = 'optional'
VID = 'vid'
STRING = 'string'
BOOLEAN = 'boolean'
INT = 'int'
LONG = 'long'
FLOAT = 'float'
TEXT = 'text'
TIMESTAMP = 'timestamp'
SELF_TYPE = 'self-type'
DEWRAPPING = {
  STRING: str,
  TEXT: str,
  FLOAT: float,
  INT: int,
  LONG: int,
  BOOLEAN: bool,
  }


class Recorder(core.Core):

  def __init__(self, out_stream=None, study_label=None,
               host=None, user=None, passwd=None,
               keep_tokens=1, batch_size=200, operator='Alfred E. Neumann',
               action_setup_conf=None, logger=None,
               source_type=None,
               object_type=None,
               object_fields=None,
               object_defaults=None):

    def get_types():
      try:
        if source_type == 'NO_SOURCE':
          stype = None
        else:
          stype =  getattr(self.kb, source_type)
        otype = getattr(self.kb, object_type)
      except AttributeError as e:
        self.logger.fatal('Type {} is unkown.'.format(e.message.split()[-1]))
        sys.exit(1)
      return stype, otype

    def get_object_fields(field_pairs):
      ofields = {}
      o = self.object_type()
      for (f, d) in field_pairs:
        if not hasattr(o, f):
          self.logger.fatal('Attribute {} is unkown.'.format(f))
          sys.exit(1)
        K = self.object_type
        while hasattr(K, '__fields__'):
          if f in K.__fields__:
            ofields[f] = (K.__fields__[f], d)
            break
          else:
            K = K.__base__
        else:
          self.logger.fatal('Internal error on {} search.'.format(f))
          sys.exit(1)
      return ofields

    super(Recorder, self).__init__(host, user, passwd, keep_tokens=keep_tokens,
                                   study_label=study_label, logger=logger)
    self.source_type, self.object_type = get_types()
    # stupid parser
    no_default   = map(lambda x: (x, None), object_fields.split(','))
    if object_defaults:
      with_default = map(lambda x: x.split('='), object_defaults.split(','))
    else:
      with_default = []
    self.object_fields = get_object_fields(no_default + with_default)
    self.operator = operator
    self.action_setup_conf = action_setup_conf
    self.out_stream = out_stream
    self.batch_size = batch_size

  def process_chunk(self, otsv, chunk, asetup, device, acat):
    def build_conf(r):
      conf = {}
      for k in self.object_fields:
        T, v = self.object_fields[k]
        v = r[k] if v is None else v
        if (hasattr(T[0], 'is_enum') and T[0].is_enum()):
          conf[k] = getattr(T[0], v)
        else:
          conf[k] = DEWRAPPING[T[0]](v)
      return conf

    def check_if_objects_are_unknown():
      labels = map(lambda r: r['label'], chunk)
      objs = self.kb.get_by_labels(self.object_type, labels)
      if len(objs) > 0:
        for k in objs:
          self.logger.fatal('a %s with label %s is already in kb'
                            % (self.object_type, k))
        sys.exit(1)

    def find_targets():
      if self.source_type is None:
        targets = [None] * len(chunk)
      else:
        vids = map(lambda x: x['source'], chunk)
        tdict = self.kb.get_by_vids(self.source_type, vids)
        try:
          targets = map(tdict.__getitem__, vids)
        except KeyError, e:
          self.logger.fatal('No target for vid {}.'.format(e.message))
          sys.exit(1)
      print 'targets: {}'.format(targets)
      return targets

    def build_actions():
      actions = []
      for r, t in it.izip(chunk, find_targets()):
        conf = {'setup' :
                asetup[self.get_action_setup_options(r,
                                                     self.action_setup_conf)],
                'device' : device, 'actionCategory' : acat,
                'operator' : self.operator, 'context' : self.default_study}
        if t is None:
          aclass = self.kb.Action
        else:
          aclass = self.get_action_class_by_target(t)
          conf['target'] = t
        actions.append(self.kb.factory.create(aclass, conf))
      return actions

    def build_objects(actions):
      objects = []
      for a, r in it.izip(actions, chunk):
        a.unload()  # FIXME we need to do this, or the next save will choke
        conf = build_conf(r)
        conf['action'] = a
        objects.append(self.kb.factory.create(self.object_type, conf))
      return objects

    def document_objects(objects):
      for o in objects:
        otsv.writerow({'study' : self.default_study.label,
                       'label' : o.label,
                       'type'  : o.get_ome_table(),
                       'vid'   : o.id })

    check_if_objects_are_unknown()
    actions = build_actions()
    self.kb.save_array(actions)
    objects = build_objects(actions)
    self.kb.save_array(objects)
    document_objects(objects)

  def record(self, records, otsv):
    def records_by_chunk(batch_size, records):
      offset = 0
      while len(records[offset:]) > 0:
        yield records[offset:offset+batch_size]
        offset += batch_size
    if not records:
      msg = 'No records are going to be imported'
      self.logger.critical(msg)
      raise core.ImporterValidationError(msg)
    device = self.get_device('importer-%s' % version,
                             'CRS4', 'IMPORT', version)
    asetup = {}
    act_setups = set(self.get_action_setup_options(r,
                                                   self.action_setup_conf)
                     for r in records)

    for acts in act_setups:
      setup_conf = {'label' : 'import-prog-%f' % time.time(),
                    'conf' : acts}
      setup = self.kb.factory.create(self.kb.ActionSetup,
                                     setup_conf)
      asetup[acts] = self.kb.save(setup)
    acat = getattr(self.kb.ActionCategory, 'IMPORT')
    for i, c in enumerate(records_by_chunk(self.batch_size, records)):
      self.logger.info('start processing chunk %d' % i)
      self.process_chunk(otsv, c, asetup, device, acat)
      self.logger.info('done processing chunk %d' % i)

help_doc = """
FIXME.
"""

def make_parser(parser):
  parser.add_argument('--study', metavar="STRING",
                      help="assign study")
  parser.add_argument('--source-type', metavar="STRING",
                      default='NO_SOURCE',
                      help="""Type of the source object.
  It should be a legal KB object type. NO_SOURCE if none""")
  parser.add_argument('--object-type', metavar="STRING",
                      help="""Type of the object that should be registered.
  It should be a legal KB object type.""")
  parser.add_argument('--object-fields', metavar="STRING",
                      help="""fields that should be filled.""")
  parser.add_argument('--object-defaults', metavar="STRING",
                      help="""fields that should be filled
  with default values.""")


def implementation(logger, host, user, passwd, args, close_handles):
  action_setup_conf = Recorder.find_action_setup_conf(args)
  recorder = Recorder(host=host, user=user, passwd=passwd,
                      study_label=args.study,
                      keep_tokens=args.keep_tokens,
                      batch_size=200, operator=args.operator,
                      action_setup_conf=action_setup_conf, logger=logger,
                      source_type=args.source_type,
                      object_type=args.object_type,
                      object_fields=args.object_fields,
                      object_defaults=args.object_defaults)

  f = csv.DictReader(args.ifile, delimiter='\t')
  recorder.logger.info('start processing file %s' % args.ifile.name)
  records = [r for r in f]
  o = csv.DictWriter(args.ofile,
                     fieldnames=['study', 'label', 'type', 'vid'],
                     delimiter='\t', lineterminator=os.linesep)
  o.writeheader()
  report_fnames = copy.deepcopy(f.fieldnames)
  report_fnames.append('error')
  report = csv.DictWriter(args.report_file, report_fnames,
                          delimiter='\t', lineterminator=os.linesep,
                          extrasaction='ignore')
  report.writeheader()
  try:
    recorder.record(records, o)
  except core.ImporterValidationError as ve:
    recorder.logger.critical(ve.message)
    raise
  finally:
    close_handles(args)
  recorder.logger.info('done processing file %s' % args.ifile.name)

def do_register(registration_list):
  registration_list.append(('agnostic', help_doc, make_parser,
                            implementation))
