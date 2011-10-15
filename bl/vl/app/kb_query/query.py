"""
Generic Query support
=====================

The goal of this module is to provide a simplified environment to
perform complex queries in VL.

The basic command is the following::

  usage: kb_query query [-h] --group GROUP --code-file CODE_FILE

  optional arguments:
    -h, --help            show this help message and exit
    --group GROUP         the group label
    --code-file CODE_FILE
                          path to the query code file


The idea is that one invokes

.. code-block:: bash

   bash> ${KB_QUERY} --ofile foo_junk.tsv -P romeo \
                     --operator aen query --code-file foo.py \
                     --group BSTUDY

where the contents of the '''--code-file''' are something like the following.

.. code-block:: python

   writeheader('dc_id', 'gender', 'data_sample',
               'path', 'mimetype', 'size', 'sha1')
   for i in Individuals(group):
      for d in DataSamples(i, 'AffymetrixCel'):
         for o in DataObjects(d):
            writerow(group.id, enum_label(i.gender), d.id,
                     o.path, o.mimetype, o.size, o.sha1)


Where '''group''' (actually a study) corresponds to the group whose
label is assigned by the '''--group''' flag.

**Note** This is clearly an extremely dangerous tool.

"""

from bl.vl.kb.dependency import DependencyTree

from bl.vl.app.importer.core import Core
from version import version

import random

import csv, json
import time, sys
import itertools as it

import argparse

import logging

class Selector(Core):
  """
.. code-block:: python

   writeheader('dc_id', 'gender', 'data_sample',
               'path', 'mimetype', 'size', 'sha1')
   for i in Individuals(group):
      for d in DataSamples(i, AffymetrixCel):
         for o in DataObjects(d):
            writerow(group.id, enum_label(i.gender), d.id,
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
    self.ots = None
    self._field_names = []

    self.logger.info('start loading dependency tree')
    dt = DependencyTree(self.kb)
    self.logger.info('done loading dependency tree')

    def writeheader(*field_names):
      self._field_names = field_names
      self.ots = csv.DictWriter(args.ofile, self._field_names, delimiter='\t')
      self.ots.writeheader()

    def writerow(*field_values):
      d = dict(zip(self._field_names, field_values))
      self.ots.writerow(d)

    def Individuals(group):
      return self.kb.get_individuals(group)

    def DataSamples(individual, data_sample_klass_name='DataSample'):
      klass = getattr(self.kb, data_sample_klass_name)
      return dt.get_connected(individual, aklass=klass)

    def DataObjects(data_sample):
      q = """select o from DataObject as o join fetch o.sample as s
             where s.id = :sid"""
      return self.kb.find_all_by_query(q, {'sid' : data_sample.omero_id})

    def enum_label(x):
      if isinstance(x, self.kb.Gender):
        if x == self.kb.Gender.MALE:
          return 'MALE'
        if x == self.kb.Gender.FEMALE:
          return 'FEMALE'

    code  = args.code_file.read()
    group = self.kb.get_study(args.group)
    ccode = compile(code, '<string>', 'exec')
    exec ccode in locals()

#-------------------------------------------------------------------------
help_doc = """
Perform (almost) arbitrary queries against BIOBANK
"""

def make_parser(parser):
  parser.add_argument('--group', type=str,
                      required=True,
                      help="the group label")
  parser.add_argument('--code-file', type=argparse.FileType('r'),
                      required=True,
                      help="path to the query code file")

def implementation(logger, args):
  #--
  selector = Selector(host=args.host, user=args.user, passwd=args.passwd,
                      logger=logger)
  selector.dump(args)

def do_register(registration_list):
  registration_list.append(('query', help_doc,
                            make_parser,
                            implementation))


