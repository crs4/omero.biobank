"""

Load Genotypes
==============

This example shows how one can load genotype data in Omero/VL.
Specifically, we will be importing a collection of data obtained by
`Taqman <http://en.wikipedia.org/wiki/TaqMan>`_. The data is expressed
as lines in a ped file, with the list of relevant markers contained in
related dat file. We are assuming the following:

 #. for each row of the ped file, the first column is taken as the
    label of the specific DataSample, the second column is taken as
    the dna sample label (plate wells are labeled as
    plate_label:plate_well_label) columns three to five
    are ignored, the dna_sample labels should be known to OMERO/BL;

 #. all the markers listed in the dat file should be known to OMERO/BL
    as SNP markers and the marker name will be interpreted as a 'rs_label';

 #. we assume that all the measures have been performed at the same
    time and by the same device.

Usage
-----

Basic usage example::

 bash$ load_genotypes --pedfile foo.ped --datfile foo.dat \
                      --maker CRS4 --model TaqMan --release foo \
                      --study-label ASTUDY

"""

import logging
logging.basicConfig(level=logging.INFO)

import argparse

import bl.vl.genotype.io as io
from bl.vl.kb import KnowledgeBase as KB

import os, sys, time
import itertools as it

import numpy as np

#------------------------------------------------------------------------------
def make_parser():
  parser = argparse.ArgumentParser(description="Load genotype data")
  parser.add_argument('--pedfile', type=argparse.FileType('r'),
                      help='the input pedfile')
  parser.add_argument('--datfile', type=argparse.FileType('r'),
                      help='the input datfile')
  parser.add_argument('-S', '--study', type=str,
                      help='the context study label')
  parser.add_argument('--maker', type=str,
                      help='the SNPMarkersSet maker')
  parser.add_argument('--model', type=str,
                      help='the SNPMarkersSet model')
  parser.add_argument('--release', type=str,
                      help='the SNPMarkersSet release')
  parser.add_argument('-H', '--host', type=str,
                      help='omero host system',
                      default='localhost')
  parser.add_argument('-U', '--user', type=str,
                      help='omero user',
                      default='root')
  parser.add_argument('-P', '--passwd', type=str,
                      help='omero user passwd')
  return parser

#----------------------------------------------------------------------------
class PedReader(object):
  def __init__(self, pedfile, datfile, conf_value=1.0):
    self.ped_file = pedfile
    self.dat_file = datfile
    self.conf_value = conf_value
    self.marker_types = []
    self.marker_names = []
    dr = io.DatReader(self.dat_file)
    for t, name in dr:
      self.marker_types.append(t)
      self.marker_names.append(name)
    self.plp = io.PedLineParser(''.join(self.marker_types), m_only=True)

  def __iter__(self):
    for l in self.ped_file:
      yield self.canonize(self.plp.parse(l))

  def get_marker_names(self):
    _ = []
    for t, n in it.izip(self.marker_types, self.marker_names):
      if t == 'M':
        _.append(n)
    return _

  def canonize(self, record):
    TO_PROB = {('1', '1') : (1.0, 0.0), ('2', '2') : (0.0, 1.0),
               ('1', '2') : (0,0),      ('2', '1') : (0,0),
               ('0', '0') : (0.5,0.5), # No call
               }
    fam_id, p_id, f_id, m_id, gender = record[0:5]
    nd = np.transpose(np.array([TO_PROB[tuple(x)] for x in record[5:]],
                               dtype=np.float32))
    c  = np.zeros((nd.shape[1],), dtype=np.float32)
    c[:] = self.conf_value
    return {'label' : fam_id, 'sample_label' : p_id, 'probs' : nd, 'confs' : c}
#----------------------------------------------------------------------------

class App(object):

  def __init__(self, host, user, passwd,
               study_label,
               maker, model, release):
    self.kb = KB(driver='omero')(host, user, passwd)
    self.mset = self.kb.get_snp_markers_set(maker, model, release)
    self.logger = logging.getLogger()
    if not self.mset:
      raise ValueError('SNPMarkersSet[%s,%s,%s] has not been defined.'
                       % (maker, model, release))
    #--
    alabel = 'load_genotypes-setup-%s' % time.time()
    self.asetup = self.kb.factory.create(self.kb.ActionSetup,
                                         {'label' : alabel,
                                          'conf'  : ''}).save()
    #--
    dmaker, dmodel, drelease = 'CRS4', 'load_genotypes', '0.1'
    dlabel = '%s-%s-%s' % (dmaker, dmodel, drelease)
    device = self.kb.get_device(dlabel)
    if not device:
      device = self.kb.factory.create(self.kb.Device,
                                      {'label' : dlabel,
                                       'maker' : dmaker,
                                       'model' : dmodel,
                                       'release' : drelease}).save()
    self.device = device
    #-- FIXME this will break if study is not defined.
    self.study = self.kb.get_study(study_label)

  def check_snp_markers_set(self, marker_types, marker_names):
    self.logger.info('start checking snp_markers_set')
    mdefs, msetc = self.kb.get_snp_markers_set_content(self.mset)
    rs_labels = mdefs['rs_label']
    for t, n in it.izip(marker_types, marker_names):
      if t == 'M':
        if not n in rs_labels:
          msg = 'marker %s is not in the specified SNPMarkersSet' % n
          self.logger.critical(msg)
          raise ValueError(msg)
    self.logger.info('done checking snp_markers_set')

  def create_action(self, target):
    conf = {'setup' : self.asetup,
            'device' : self.device,
            'actionCategory' : self.kb.ActionCategory.MEASUREMENT,
            'operator' : 'Alfred E. Neumann',
            'context'  : self.study,
            'target'   : target,
            }
    action = self.kb.factory.create(self.kb.ActionOnVessel, conf).save()
    return action

  def create_data_sample(self, action, label):
    conf = {'snpMarkersSet' : self.mset,
            'label' : label,
            'status' : self.kb.DataSampleStatus.USABLE,
            'action' : action}
    return self.kb.factory.create(self.kb.GenotypeDataSample, conf).save()

  def load(self, pedfile, datfile, conf_value=1.0):
    pr = PedReader(pedfile, datfile, conf_value)
    self.check_snp_markers_set(pr.marker_types, pr.marker_names)
    #--
    self.logger.info('start loading from pedfile %s' % pedfile.name)
    for x in pr:
      sample = self.kb.get_vessel(x['sample_label'])
      if not sample:
        self.logger.error('No sample with label %s in VL' % x['sample_label'])
        continue
      action = self.create_action(sample)
      avid = action.id
      action.unload()
      data_sample = self.create_data_sample(action, x['label'])
      data_object = self.kb.add_gdo_data_object(avid, data_sample,
                                                x['probs'], x['confs'])
      self.logger.info('-- loaded %s' % x['label'])
    self.logger.info('done loading from pedfile %s' % pedfile.name)


def main():
  parser = make_parser()
  args = parser.parse_args()
  if not (args.passwd
          and args.maker and args.model and args.release and args.study):
    parser.print_help()
    sys.exit(1)

  app = App(args.host, args.user, args.passwd,
            args.study,
            args.maker, args.model, args.release)
  app.load(args.pedfile, args.datfile)

if __name__ == "__main__":
    main()


# Local Variables: **
# mode: python **
# End: **
