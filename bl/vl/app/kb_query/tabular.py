"""
Extract data in tabular form from KB
====================================

FIXME

"""

from bl.vl.app.importer.core import Core, BadRecord
from version import version

from bl.vl.kb import DependencyTree

import csv, json
import time, sys
import itertools as it

import logging

class Tabular(Core):
  """
  An utility class that handles the dumping of tabular
  specification data from the KB.
  """

  SUPPORTED_DATA_PROTOCOLS = ['file', 'hdfs']
  SUPPORTED_FIELDS_SETS    = ['gender_check', 'call_gt']


  def __init__(self, host=None, user=None, passwd=None, keep_tokens=1,
               data_collection_label=None,
               preferred_data_protocol=None,
               operator='Alfred E. Neumann'):
    """
    FIXME
    """
    self.logger = logging.getLogger()
    super(Tabular, self).__init__(host, user, passwd,
                                  keep_tokens=keep_tokens,
                                  study_label='TEST01')
    if data_collection_label:
      self.data_collection = self.kb.get_data_collection(data_collection_label)
    else:
      self.data_collection = None

    assert  preferred_data_protocol in self.SUPPORTED_DATA_PROTOCOLS

    self.preferred_data_protocol = preferred_data_protocol

  def pre_load_data(self):
    self.logger.info('start prefetching DataSample')
    if self.data_collection:
      data_samples = [dci.dataSample
                      for dci
                      in self.kb.get_data_collection_items(self.data_collection)]
    else:
      data_samples = self.kb.get_bio_samples(self.skb.AffymetrixCel)
    self.logger.info('done prefetching DataSample')

    self.logger.info('start prefetching DataObject')
    q = "select o from DataObject as o join fetch o.sample as s"
    factory = lambda x, proxy : self.skb.DataObject(x, proxy=proxy)
    objs = self.skb.find_all_by_query(q, {}, factory)
    ds_to_do = {}
    for o in objs:
      ds_to_do.setdefault(o.sample.id, []).append(o)
    self.logger.info('done prefetching DataObject')

    return data_samples, ds_to_do

  def dump_call_gt(self, ofile):
    if not self.data_collection:
      raise ValueError('data_collection %s is not known to KB' % self.data_collection)
    #--
    dt = DependencyTree(self.skb, self.ikb, [self.ikb.Individual,
                                             self.skb.BioSample,
                                             self.skb.PlateWell,
                                             self.skb.DataSample])
    data_samples, ds_to_do = self.pre_load_data()
    #--
    fnames = 'dc_id item_id data_sample_label path gender mimetype size sha1'.split()
    tsv = csv.DictWriter(ofile, fnames, delimiter='\t')
    tsv.writeheader()
    #--
    dc_id = self.data_collection.id
    for ds in data_samples:
      v = dt.get_connected(ds, self.ikb.Individual)
      assert len(v) == 1
      i = v[0]
      gender = self.gm_by_object[i.gender.id]
      if ds_to_do.has_key(ds.id):
        for do in ds_to_do[ds.id]:
          r = {'dc_id' : dc_id,
               'data_sample_label' : do.sample.label,
               'item_id' : do.sample.id,
               'gender' : gender,
               'path' : do.path,
               'mimetype' : do.mimetype,
               'size' : do.size,
               'sha1' : do.sha1}
          tsv.writerow(r)
      else:
        self.logger.warn('there is no DataObject for %s[%s]' % (ds.label, ds.id))


  def dump_gender_check(self, ofile):
    dt = DependencyTree(self.kb, [self.kb.Individual,
                                  self.kb.BioSample,
                                  self.kb.PlateWell,
                                             self.skb.DataSample])

    data_samples, ds_to_do = self.pre_load_data()

    fnames = 'individual_id gender path mimetype size sha1'.split()
    tsv = csv.DictWriter(ofile, fnames, delimiter='\t')
    tsv.writeheader()
    #--
    for ds in data_samples:
      v = dt.get_connected(ds)
      i = filter(lambda x: type(x) == self.ikb.Individual, v)[0]
      gender = self.gm_by_object[i.gender.id]
      if ds_to_do.has_key(ds.id):
        for do in ds_to_do[ds.id]:
          r = {'individual_id' : i.id,
               'gender' : gender,
               'path' : do.path,
               'mimetype' : do.mimetype,
               'size' : do.size,
               'sha1' : do.sha1}
          tsv.writerow(r)
      else:
        self.logger.warn('there is no DataObject for %s[%s]' % (ds.label, ds.id))

  def dump(self, fields_set, ofile):
    assert fields_set in self.SUPPORTED_FIELDS_SETS
    if fields_set == 'gender_check':
      self.dump_gender_check(ofile)
    elif fields_set == 'call_gt':
      self.dump_call_gt(ofile)


#-------------------------------------------------------------------------
help_doc = """
Extract data from the KB in tabular form.
"""

def make_parser_tabular(parser):
  parser.add_argument('--data-collection', type=str,
                      help="data collection label")
  parser.add_argument('--study', type=str,
                      help="study label")
  parser.add_argument('--preferred-data-protocol', type=str,
                      choices=Tabular.SUPPORTED_DATA_PROTOCOLS,
                      default='file',
                      help="""try, if possible, to provide
                      data object paths that use this protocol""")
  parser.add_argument('--fields-set', type=str,
                      choices=Tabular.SUPPORTED_FIELDS_SETS,
                      help="""choose all the fields listed in this set""")

def import_tabular_implementation(args):
  #--
  tabular = Tabular(host=args.host, user=args.user, passwd=args.passwd,
                    keep_tokens=args.keep_tokens,
                    data_collection_label=args.data_collection,
                    preferred_data_protocol=args.preferred_data_protocol)
  tabular.dump(args.fields_set, args.ofile)

def do_register(registration_list):
  registration_list.append(('tabular', help_doc,
                            make_parser_tabular,
                            import_tabular_implementation))


