"""
Generic Query support
=====================

FIXME


"""

from bl.vl.kb.dependency import DependencyTree

from bl.vl.app.importer.core import Core
from version import version

import random

import csv, json
import time, sys
import itertools as it


import logging

class Selector(Core):
  """
.. code-block:: python

   writeheader('dc_id', 'gender', 'data_sample',
               'path', 'mimetype', 'size', 'sha1')
   for i in Individuals(group):
      for d in DataSamples(i, AffymetrixCel):
         for o in DataObjects(d):
            writerow(group.id, i.gender.enum_label(), d.id,
                     o.path, o.mimetype, o.size, o.sha1)

  """
  def __init__(self, host=None, user=None, passwd=None,
               operator='Alfred E. Neumann', logger=None):
    """
    FIXME
    """
    super(Selector, self).__init__(host, user, passwd, logger=logger)

    #FIXME we need to do this to sync with the DB idea of the enums.
    self.kb.Gender.map_enums_values(self.kb)

  def dump(self, args):
    otsv = None
    _field_names = []

    self.logger.info('start loading dependency tree')
    dt = DependencyTree(self.kb)
    self.logger.info('done loading dependency tree')

    def writeheader(*field_names):
      _field_names = field_names
      ots = csv.DictWriter(args.ofile, _field_names, delimiter='\t')
      ots.writeheader()

    def writerow(*field_values):
      d = dict(zip(_field_names, field_values))
      ots.writerow(d)

    def Individuals(group):
      return [e.individual for e in self.kb.get_enrolled(group)]

    def DataSamples(individual, data_sample_klass_name=DataSample):
      klass = getattr(self.kb, data_sample_klass_name)
      return dt.get_connected(individual, aklass=klass)

    def DataObjects(data_sample):
      q = """select o from DataObject as o join fetch o.sample as s
             where s.id = :sid"""
      return self.kb.find_all_by_query(q, {'sid' : data_sample.omero_id})

    code  = args.code_file.read()
    group = self.kb.get_study(args.group)
    ccode = compile(code, '<string>', 'exec')
    exec ccode in {}

#-------------------------------------------------------------------------
help_doc = """
Select a group of individuals
"""

def make_parser_query(parser):
  parser.add_argument('--group', type=str,
                      required=True,
                      help="the group label")
  parser.add_argument('--code-file', type=argparse.FileType('w'),
                      required=True,
                      help="path to the query code file")

def import_query_implementation(logger, args):
  #--
  selector = Selector(host=args.host, user=args.user, passwd=args.passwd,
                      logger=logger)
  selector.dump(args)

def do_register(registration_list):
  registration_list.append(('selector', help_doc,
                            make_parser_selector,
                            import_selector_implementation))


