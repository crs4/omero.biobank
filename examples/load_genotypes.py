"""

Load Genotypes
==============

This example shows how one can load genotype data in Omero.
Specifically, we will be importing a collection of data obtained by
`Taqman <http://en.wikipedia.org/wiki/TaqMan>`_. The data is expressed
as lines in a ped file, with the list of relevant markers, each
corresponding to one of the 5: columns, contained in xxx.  The first
column of each line, the family column, is taken as the the id of the
operation that produced that specific dataset.

We are working under the following assumptions:

  #. this is the first time we save data for this specific genotyping
     apparatus;

  #. all the SNP markers that characterize this technology are already
     known to the KnowledgeBase;

"""

import bl.lib.pedal.io as io
from bl.lib.genotype.kb import KnowledgeBase
import numpy as np

import os
import itertools as it
import logging
logging.basicConfig(level=logging.DEBUG)


#-------------------------------------------------------------------------------------
class PedReader(object):
  def __init__(self, pedfile, datfile, conf_value=1.0):
    self.ped_file = pedfile
    self.dat_file = datfile
    self.conf_value = conf_value
    self.marker_types = []
    self.marker_names = []
    dr = io.DatReader(open(self.dat_file))
    for t, name in dr:
      self.marker_types.append(t)
      self.marker_names.append(name)
    self.plp = io.PedLineParser(''.join(self.marker_types), m_only=True)

  def __iter__(self):
    with open(self.ped_file) as f:
      for l in f:
        yield self.canonize(self.plp.parse(l))

  def get_marker_names(self):
    _ = []
    for t, n in it.izip(self.marker_types, self.marker_names):
      if t == 'M':
        _.append(n)
    return _

  def canonize(self, record):
    TO_PROB = {('1', '1') : (1.0, 0.0), ('2', '2') : (0.0, 1.0),
               ('1', '2') : (0,0),
               ('2', '1') : (0,0),
               ('0', '0') : (0.5,0.5),
               }
    fam_id, p_id, f_id, m_id, aff = record[0:5]
    nd = np.transpose(np.array([TO_PROB[tuple(x)] for x in record[5:]],
                               dtype=np.float32))
    print 'nd.shape:',  nd.shape
    c  = np.zeros((nd.shape[1],), dtype=np.float32)
    c[:] = self.conf_value
    return {'op_vid' : fam_id[1:], 'probs' : nd, 'confs' : c}

#------------------------------------------------------------------------------------------
def create_new_snp_markers_set(kb, maker, model, ped_reader):
  marker_vids = kb.get_snp_vids(rs_labels=ped_reader.get_marker_names())
  def snp_set_item(vids):
    for i, v in enumerate(vids):
      r = {'marker_vid' : v, 'marker_indx' : i, 'allele_flip' : False}
      yield r
  op_vid = kb.make_vid()
  set_vid = kb.create_new_snp_markers_set(maker, model, snp_set_item(marker_vids), op_vid)
  kb.create_new_gdo_repository(set_vid)
  return set_vid

def main():
  pedfile = '../tests/bl/lib/genotype/data/VALIDAZ_noCBLB.ped'
  datfile = '../tests/bl/lib/genotype/data/validazione.dat'
  OME_HOST = os.getenv("OME_HOST", "localhost")
  OME_USER = os.getenv("OME_USER", "root")
  OME_PASS = os.getenv("OME_PASS", "romeo")

  kb = KnowledgeBase(driver='omero')

  kb.open(OME_HOST, OME_USER, OME_PASS)
  #--
  maker, model = 'crs4-bl', 'taqman-foo'
  pr = PedReader(pedfile, datfile, conf_value=0.8)
  set_vid = create_new_snp_markers_set(kb, maker, model, pr)
  #--
  for x in pr:
    vid = kb.append_gdo(set_vid, x['probs'], x['confs'], x['op_vid'])
  #--
  kb.close()

main()
