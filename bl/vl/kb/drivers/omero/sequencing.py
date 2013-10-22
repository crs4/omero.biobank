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
                ]
  def __preprocess_conf__(self, conf):
    if not 'referenceGenomeUK' in conf:
      conf['referenceGenomeUK'] = make_unique_key(conf['maker'], conf['model'],
                                                  conf['release'])
    return super(ReferenceGenome, self).__preprocess_conf__(conf)

class AlignedSeqDataSample(SeqDataSample):  
  OME_TABLE = 'AlignedSeqDataSample'
  __fields__ = [('referenceGenome', ReferenceGenome, wp.REQUIRED)]

class GenomicAssemblyData(SeqDataSample):  
  OME_TABLE = 'GenomicAssemblyData'
  __fields__ = []
  
class GenomeVariationsData(DataSample):  
  OME_TABLE = 'GenomeVariationsData'
  __fields__ = [('referenceGenome', ReferenceGenome, wp.REQUIRED)]  

class GeneExpressionLevels(DataSample):  
  OME_TABLE = 'GeneExpressionLevels'
  __fields__ = []

class TaxonomicProfile(DataSample):  
  OME_TABLE = 'TaxonomicProfile'
  __fields__ = []
  
  
