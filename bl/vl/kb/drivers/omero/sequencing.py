import wrapper as wp
from data_samples import DataSample
from utils import make_unique_key

from vessels import Tube
from objects_collections import Lane


class SequencerOutput(DataSample):
  OME_TABLE = 'SequencerOutput'
  __fields__ = []


class RawSeqDataSample(DataSample):  
  OME_TABLE = 'RawSeqDataSample'
  __fields__ = [('lane', Lane, wp.OPTIONAL)]


class SeqDataSample(DataSample):  
  OME_TABLE = 'SeqDataSample'
  __fields__ = [('sample', Tube, wp.OPTIONAL)]

class ReferenceGenome(DataSample):
  OME_TABLE = 'ReferenceGenome'
  __fields__ = [('nChroms', wp.INT, wp.REQUIRED),
                ('maker', wp.STRING, wp.REQUIRED),
                ('model', wp.STRING, wp.REQUIRED),
                ('release', wp.STRING, wp.REQUIRED),
                ('referenceGenomeUK', wp.STRING, wp.REQUIRED)]

  def __preprocess_conf__(self, conf):
    if not 'referenceGenomeUK' in conf:
      conf['referenceGenomeUK'] = make_unique_key(conf['maker'], conf['model'],
                                                  conf['release'])
    return super(ReferenceGenome, self).__preprocess_conf__(conf)

  def __update_constraints__(self):
    key = make_unique_key(self.maker, self.model, self.release)
    setattr(self.ome_obj, 'referenceGenomeUK',
            self.to_omero(self.__fields__['referenceGenomeUK'][0], key))

class AlignedSeqDataSample(SeqDataSample):  
  OME_TABLE = 'AlignedSeqDataSample'
  __fields__ = [('referenceGenome', ReferenceGenome, wp.REQUIRED)]

class GenomicAssemblyDataSample(SeqDataSample):  
  OME_TABLE = 'GenomicAssemblyDataSample'
  __fields__ = []
  
class GenomeVariationsDataSample(DataSample):  
  OME_TABLE = 'GenomeVariationsDataSample'
  __fields__ = [('referenceGenome', ReferenceGenome, wp.REQUIRED)]  

class GeneExpressionLevelsDataSample(DataSample):  
  OME_TABLE = 'GeneExpressionLevelsDataSample'
  __fields__ = []

class TaxonomicProfileDataSample(DataSample):  
  OME_TABLE = 'TaxonomicProfileDataSample'
  __fields__ = []
  
  
