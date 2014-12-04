import wrapper as wp
from data_samples import DataSample
from utils import make_unique_key

from vessels import Tube
from objects_collections import Lane


class SequencerOutput(DataSample):
  OME_TABLE = 'SequencerOutput'
  __fields__ = []

  def __update_constraints__(self):
    self.__fields__['labelUK'] = super(SequencerOutput, self).__fields__['labelUK']


class RawSeqDataSample(DataSample):  
  OME_TABLE = 'RawSeqDataSample'
  __fields__ = [('lane', Lane, wp.OPTIONAL)]

  def __update_constraints__(self):
    self.__fields__['labelUK'] = super(RawSeqDataSample, self).__fields__['labelUK']


class SeqDataSample(DataSample):  
  OME_TABLE = 'SeqDataSample'
  __fields__ = [('sample', Tube, wp.OPTIONAL)]

  def __update_constraints__(self):
    self.__fields__['labelUK'] = super(SeqDataSample, self).__fields__['labelUK']


class ReferenceGenome(DataSample):
  OME_TABLE = 'ReferenceGenome'
  __fields__ = [('nChroms', wp.INT, wp.REQUIRED),
                ('maker', wp.STRING, wp.REQUIRED),
                ('model', wp.STRING, wp.REQUIRED),
                ('release', wp.STRING, wp.REQUIRED),
                ('referenceGenomeUK', wp.STRING, wp.REQUIRED)]
  __do_not_serialize__ = ['referenceGenomeUK'] + DataSample.__do_not_serialize__

  def __preprocess_conf__(self, conf):
    if not 'referenceGenomeUK' in conf:
      conf['referenceGenomeUK'] = make_unique_key(conf['maker'], conf['model'],
                                                  conf['release'])
    return super(ReferenceGenome, self).__preprocess_conf__(conf)

  def __update_constraints__(self):
    self.__fields__['labelUK'] = super(ReferenceGenome, self).__fields__['labelUK']
    key = make_unique_key(self.maker, self.model, self.release)
    setattr(self.ome_obj, 'referenceGenomeUK',
            self.to_omero(self.__fields__['referenceGenomeUK'][0], key))
    super(ReferenceGenome, self).__update_constraints__()


class AlignedSeqDataSample(SeqDataSample):  
  OME_TABLE = 'AlignedSeqDataSample'
  __fields__ = [('referenceGenome', ReferenceGenome, wp.REQUIRED)]


class GenomicAssemblyDataSample(SeqDataSample):  
  OME_TABLE = 'GenomicAssemblyDataSample'
  __fields__ = []


class GenomeVariationsDataSample(DataSample):  
  OME_TABLE = 'GenomeVariationsDataSample'
  __fields__ = [('referenceGenome', ReferenceGenome, wp.REQUIRED)]

  def __update_constraints__(self):
    self.__fields__['labelUK'] = super(GenomeVariationsDataSample, self).__fields__['labelUK']
    super(GenomeVariationsDataSample, self).__update_constraints__()


class GeneExpressionLevelsDataSample(DataSample):  
  OME_TABLE = 'GeneExpressionLevelsDataSample'
  __fields__ = []

  def __update_constraints__(self):
    self.__fields__['labelUK'] = super(GeneExpressionLevelsDataSample, self).__fields__['labelUK']
    super(GeneExpressionLevelsDataSample, self).__update_constraints__()


class TaxonomicProfileDataSample(DataSample):  
  OME_TABLE = 'TaxonomicProfileDataSample'
  __fields__ = []
  
  def __update_constraints__(self):
    self.__fields__['labelUK'] = super(TaxonomicProfileDataSample, self).__fields__['labelUK']
    super(TaxonomicProfileDataSample, self).__update_constraints__()
