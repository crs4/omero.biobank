# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
Genotyping-related I/O
======================
"""

import array, struct, logging
logger = logging.getLogger('bl.vl.genotype.io')
import numpy as np
import itertools as it
import datetime

from collections import Counter

from bl.core.seq.utils import reverse_complement as rev_compl
from bl.vl.utils.snp import split_mask
from bl.core.io import MessageStreamReader
from bl.vl.genotype.algo import project_to_discrete_genotype


class Error(Exception): pass
class InvalidRecordError(Error): pass
class MismatchError(Error): pass


# merlin-1.1.2/libsrc/PedigreeDescription.cpp
def DatReader(datfile):
  for line in datfile:
    try:
      t, name = line.split(None, 1)
    except ValueError:
      if line.strip():  # not ws-only
        raise InvalidRecordError("%r is not a valid DAT line" % line)
      else:
        continue
    if t[0] == 'E':  # end of data
      raise StopIteration
    if t[0] == 'S':  # skip n items
      n_items = t[1:] or "1"
      try:
        n_items = int(n_items)
        if n_items < 1:
          raise ValueError
      except ValueError:
        raise InvalidRecordError("Invalid data type %r in line %r" % (t, line))
    else:
      n_items = 1
    for _ in xrange(n_items):
      yield t[0], name.rstrip()


def CompiledDatReader(datfile, unpack_indices=True):
  while 1:
    t = datfile.read(1)
    if t == "":
      raise StopIteration
    if t != "M":
      yield t, None
    else:
      idx = datfile.read(4)
      if unpack_indices:
        idx = struct.unpack(">I", idx)[0]
      yield t, idx


def MapReader(mapfile):
  n_skipped = 0
  for i, line in enumerate(mapfile):
    record = line.strip()
    if record == "":
      n_skipped += 1
      continue
    record = record.split()
    try:
      record[0] = int(record[0])
      record[2:] = map(float, record[2:])
    except ValueError:
      if i == n_skipped:
        continue  # header
      else:
        raise InvalidRecordError("Invalid map record: %r" % line)
    yield record


def get_dat_types(datfile):
  if not hasattr(datfile, "next"):
    datfile = open(datfile)
  dat_types = array.array('c')
  for t, _ in DatReader(datfile):
    dat_types.append(t)
  if hasattr(datfile, "close"):
    datfile.close()
  return dat_types.tostring()


def get_dat_data(datfile):
  if not hasattr(datfile, "next"):
    datfile = open(datfile)
  dat_data = list(DatReader(datfile))
  if hasattr(datfile, "close"):
    datfile.close()
  return dat_data


def get_map_data(mapfile):
  map_data = {}
  if not hasattr(mapfile, "next"):
    mapfile = open(mapfile)
  for chr_, marker, pos in MapReader(mapfile):
    map_data[marker] = [chr_, pos]
  if hasattr(mapfile, "close"):
    mapfile.close()
  return map_data


class PedLineParser(object):

  HDR_COLS = 5

  def __init__(self, dat_types, skip=False, m_only=False):
    self.dat_types = dat_types
    self.skip = skip
    indexing = [0, 1, 2, 3, 4]
    k = self.HDR_COLS
    for t in dat_types:
      if t == 'M':
        indexing.append(slice(k,k+2))
        k += 2
      else:
        if (t == 'S' and self.skip) or m_only:
          # FIXME: this does not correctly skip marker columns
          k += 1
          continue
        indexing.append(k)
        k += 1
    self.indexing = indexing

  def parse(self, ped_line):
    if ped_line.find('/') >= 0:
      ped_line = ped_line.replace("/", " ")
    data = ped_line.split()
    try:
      return map(data.__getitem__, self.indexing)
    except IndexError:
      if len(data) < 5:
        raise InvalidRecordError("%r is not a valid PED line" % ped_line)
      else:
        preview = " ".join(data[:5]) + " [...]"
        raise MismatchError("%r is not consistent with DAT types" % preview)

class VCFWriter(object):
  """
  Writes a `VCF 4.1 <http://www.1000genomes.org/wiki/Analysis/Variant%20Call%20Format/vcf-variant-call-format-version-41>'_ formatted file

  Current version is a minimalistic implementation, but should produce
  something that, at least, verifyBAMid could read.

  Example

  ##fileformat=VCFv4.1
  ##fileDate=20090805
  ##source=myImputationProgramV3.1
  ##reference=file:///seq/references/1000GenomesPilot-NCBI36.fasta
  ##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
  #CHROM POS     ID        REF    ALT     QUAL FILTER FORMAT NA01 NA02 NA03
  20     14370   rs6054257 G      A       .    PASS   GT     0/0  1/0  1/1
  20     17330   .         T      A       .    PASS   GT     0/0  0/1  0/0
  20     1110696 rs6040355 A      G       .    PASS   GT     1/2  2/1  2/2

  The REF column corresponds to the allele found on the reference for
  that SNP, while ALT is the alternative allele. The last three
  columns in this example are what has been measured for,
  respectively, samples NA01, NA02, NA03.

  """
  def __init__(self, mset, ref_genome, marker_selector=None):
    self.mset = mset
    self.ref_genome = ref_genome
    self.marker_selector = marker_selector
    self.mset.load_markers(additional_fields=['mask', 'rs_label'])
    self.mset.load_alignments(self.ref_genome)

  def __initialize_marker_selector(self, d):
    if self.marker_selector is None:
      probs, _ = d.resolve_to_data()
      self.marker_selector = np.arange(probs.shape[1], dtype=np.uint32)

  def __load_data(self, data_samples):
    self.__initialize_marker_selector(data_samples[0])
    N = len(data_samples)
    M = len(self.marker_selector)
    data = np.zeros((N, M), dtype=np.uint8)
    labels = []
    for i, d in enumerate(data_samples):
      labels.append(d.label)
      # FIXME: it would be better if we could directly tell resolve_to_data
      # to fetch only as selected by marker_selector
      probs, _ = d.resolve_to_data()
      data[i, :] = project_to_discrete_genotype(probs[:, self.marker_selector])
    return labels, data

  def __write_header(self, fobj, labels):
    d = datetime.date.today()
    fobj.write('##fileformat=VCFv4.1\n')
    fobj.write('##fileDate=%d%02d%02d\n' % (d.year, d.month, d.day))
    fobj.write('##source=biobank.export.v0\n')
    fobj.write('##reference=%s\n' % self.ref_genome) # FIXME
    fobj.write('##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">\n')
    fobj.write('#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tFORMAT')
    fobj.write('\t' + '\t'.join(labels))
    fobj.write('\n')

  def __write_snp(self, fobj, m, label, dat):
    allele_patterns = np.array(['0/0','1/1','0/1', './.'])
    _, alleles, _ = split_mask(m.mask)
    if m.on_reference_strand:
      ref_allele = alleles[m.allele_on_reference == 'B']
      alt_allele = alleles[m.allele_on_reference == 'A']
    else:
      ref_allele = rev_compl(alleles[m.allele_on_reference == 'B'])
      alt_allele = rev_compl(alleles[m.allele_on_reference == 'A'])
    fobj.write('%s\t%s' % m.position)
    fobj.write('\t%s\t%s\t%s' % (m.rs_label, ref_allele, alt_allele))
    fobj.write('\t.\tPASS\tGT')
    fobj.write('\t' + '\t'.join(allele_patterns[dat]))
    fobj.write('\n')

  def write(self, file_object, data_samples):
    labels, data = self.__load_data(data_samples)
    self.__write_header(file_object, labels)
    for i, l in enumerate(labels):
      self.__write_snp(file_object, self.mset[i], l, data[i, :])

class PedWriter(object):
  """
  Writes a `PLINK <http://pngu.mgh.harvard.edu/~purcell/plink>`_
  (ped, map) pair for a given marker set and collection of families.

  Use a subset of the markers if ``selected_markers`` is set. It is
  possible to request that the map file contain marker positions wrt
  a reference genome: if the latter is not provided, the map file
  will contain default values, i.e., (0, 0). If the user does
  provide a reference genome and there is no alignment information
  for the markers in mset wrt to that genome, an error is generated.

  :param mset: a reference markers set that will be used to generate
    the map file
  :type mset: SNPMarkersSet

  :param base_path: optional base_path that will be used to create the
    .ped and .map files
  :type base_path: str

  :param ref_genome: optional reference genome against which the
    markers are aligned in the map file
  :type ref_genome: str

  :param selected_markers: an array with the indices of the selected
    markers.
  :type selected_markers: numpy.ndarray of numpy.int32
  """
  def __init__(self, mset, base_path="bl_vl_ped",
               ref_genome=None, selected_markers=None):
    self.mset = mset
    self.base_path = base_path
    self.selected_markers = selected_markers
    self.ref_genome = ref_genome
    self.ped_file = None
    try:
      N = len(self.mset)
    except ValueError:
      self.mset.load_markers()
      N = len(self.mset)
    self.null_probs = np.empty((2, N), dtype=np.float32)
    self.null_probs.fill(1/3.)
    if self.ref_genome:
      self.mset.load_alignments(self.ref_genome)
    kb = self.mset.proxy
    kb.Gender.map_enums_values(kb)
    self.gender_map = lambda x: 2 if x == kb.Gender.FEMALE else 1

  def write_map(self):
    """
    Write out the map file.

    **NOTE:** we currently do not have a way to estimate the genetic
    distance, so we force it to 0.
    """
    def chrom_label(x):
      # FIXME this function is not used
      if x < 23:
        return x
      return { 23 : 'X', 24 : 'Y', 25 : 'XY', 26 : 'MT'}[x]
    def dump_markers(fo, marker_indx):
      for i in marker_indx:
        m = self.mset.markers[i]
        chrom, pos = m.position
        fo.write('%s\t%s\t%s\t%s\n' % (chrom, m.label, 0, pos))
    with open(self.base_path + '.map', 'w') as fo:
      fo.write('# map based on mset %s aligned on %s\n' %
               (self.mset.id, self.ref_genome))
      s = self.selected_markers or xrange(len(self.mset))
      dump_markers(fo, s)

  def write_family(self, family_label, family_members,
                   data_sample_by_id=None, phenotype_by_id=None):
    """
    Write out ped file lines corresponding to individuals in a given
    list, together with genotypes and, optionally, phenotypes.

    :param family_label: what to write as the family id
    :type family_label: str

    :param family_members: relevant elements of the family
    :type family_members: iterator on Individual

    :param data_sample_by_id: an optional dict-like object that maps
      individual ids to GenotypeDataSample objects
    :type data_sample_by_id: dict

    :param phenotype_by_id: an optional dict-like object that maps
      individual ids to values that can be put in column 6 (phenotype)
      of a PLINK ped file
    :type phenotype_by_id: dict
    """
    if not phenotype_by_id:
      phenotype_by_id = {None: 0}
    allele_patterns = {0: 'A A', 1: 'B B', 2: 'A B', 3: '0 0'}
    def dump_genotype(fo, data_sample):
      if data_sample is None:
        probs = self.null_probs
      else:
        probs, _ = data_sample.resolve_to_data()
      if self.selected_markers:
        probs = probs[:, self.selected_markers]
      fo.write('\t'.join([allele_patterns[x]
                          for x in project_to_discrete_genotype(probs)]))
      fo.write('\n')
    if self.ped_file is None:
      self.ped_file = open(self.base_path+'.ped', 'w')
    for i in family_members:
      # Family ID, IndividualID, paternalID, maternalID, sex, phenotype
      fat_id = 0 if not i.father else i.father.id
      mot_id = 0 if not i.mother else i.mother.id
      gender = self.gender_map(i.gender)
      pheno = phenotype_by_id.get(i.id, 0)
      self.ped_file.write('%s\t%s\t%s\t%s\t%s\t%s\t' %
                          (family_label, i.id, fat_id, mot_id, gender, pheno))
      if data_sample_by_id:
        dump_genotype(self.ped_file, data_sample_by_id.get(i.id))

  def close(self):
    if self.ped_file:
      self.ped_file.close()
    self.ped_file = None


def read_ssc(fn, mset):
  """
  Read a file with mimetypes.SSC_FILE mimetype and return the prob and
  conf arrays for a given SNPMarkersSet mset.

  :param fn: ssc file name
  :type fn: str

  :param mset: a reference markers set
  :type mset: SNPMarkersSet
  """
  ct = Counter()
  if (not mset.has_markers()
      or 'label' not in mset.get_add_marker_info_fields()):
    mset.load_markers(additional_fields=['label'])
  n_markers = len(mset)
  probs = np.empty((2, n_markers), dtype=np.float32)
  probs.fill(1/3.)
  confs = np.zeros((n_markers,), dtype=np.float32)
  markers = mset.markers
  add_marker_info = mset.add_marker_info
  labels = add_marker_info['label']
  flips = markers['allele_flip']
  indx  = markers['marker_indx']
  l2m = dict((l, (f, i)) for (l, f, i) in it.izip(labels, flips, indx))
  reader = MessageStreamReader(fn)
  for _ in xrange(n_markers):
    _, snp_label, _, conf, _, _, w_AA, w_AB, w_BB = reader.read()
    flip, idx = l2m[snp_label]
    S = w_AA + w_AB + w_BB
    try:
      p_AA, p_BB = w_AA / S, w_BB / S
      if flip:
        p_AA, p_BB = p_BB, p_AA
      probs[0,idx] = p_AA
      probs[1,idx] = p_BB
    except ZeroDivisionError:
      logger.warning(
        'read_ssc:\tZeroDivisionError raised while parsing file %s' % fn
        )
      logger.debug('read_ssc:\tsnp_label = %s -- w_AA, w_AB, w_BB = %r' % (
        snp_label, (w_AA, w_AB, w_BB)
        ))
      logger.debug('read_ssc:\tusing default probs %r' % (
        (probs[0,idx], probs[1,idx]),
        ))
      ct['outliers'] += 1
    confs[idx] = conf
  logger.info('read_scc:\tfound %d suspected outliers in %s' % (
    ct['outliers'], fn
    ))
  return probs, confs
