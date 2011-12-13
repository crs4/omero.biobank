import array, struct
import numpy as np
from bl.core.io import MessageStreamReader
from bl.vl.genotype.algo import project_to_discrete_genotype


class Error(Exception):

  def __init__(self, msg):
    self.msg = msg

  def __str__(self):
    return str(self.msg)


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
    for i in xrange(n_items):
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
  for t, name in DatReader(datfile):
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
  for chr, marker, pos in MapReader(mapfile):
    map_data[marker] = [chr, pos]
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


class PedWriter(object):
  """
  A ped file writer.

  It will output a plink formatted ped and map pair for a collection
  of families and related genotype and phenotype information.

  Expected usage:

  .. code-block:: python

    from bl.vl.genotype.io import PedWriter

    mset = kb.get_snp_markers_set(label='FakeTaqMan01')
    pw = PedWriter(mset, base_path="./foo")
    pw.write_map()
    pw.write_family(family_label1, family1, data_sample_by_id)
    pw.write_family(family_label2, family2, data_sample_by_id)
    pw.close()

  will generate in the working directory two files, './foo.ped' and
  './foo.map', in the format described in `ped link`_.

  .. _ped link: http://pngu.mgh.harvard.edu/~purcell/plink/data.shtml#ped
  """
  def __init__(self, mset, base_path="bl_vl_ped",
               ref_genome=None, selected_markers=None):
    """
    Instantiate a PedWriter object for SNPMarkersSet mset. Will use a
    subset of the markers if selected_markers is set.  It is possible
    to request that the map file contains genomic markers positions
    against a reference genome. If the latter is not provided, the map
    file will contain default values, i. e., (0, 0). It will raise a
    ValueError if there is no alignment information for the markers
    in mset on ref_genome.

    :param mset: a reference markers set that will be used to generate
                 the map file
    :type mset: SNPMarkersSet

    :param base_path: optional base_path that will be used to create the
                      .ped and .map files. Defaults to 'bl_vl_ped'
    :type base_path: str

    :param ref_genome: optional reference genome against which the
                       markers are aligned in the map file.
    :type ref_genome: str

    :param selected_markers: an array with the indices of the selected
                             markers.
    :type selected_markers: numpy.array of numpy.int32
    """

    self.mset = mset
    self.base_path = base_path
    self.selected_markers = selected_markers
    self.ref_genome = ref_genome
    self.ped_file = None

    try:
      N = len(self.mset)
    except ValueError as e:
      self.mset.load_markers()
      N = len(self.mset)

    self.null_probs = np.empty((2, N), dtype=np.float32)
    self.null_probs.fill(1/3.)

    if self.ref_genome:
      self.mset.load_alignments(self.ref_genome)

    # FIXME this starts to be nasty...
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
      if x < 23:
        return x
      return { 23 : 'X', 24 : 'Y', 25 : 'XY', 26 : 'MT'}[x]
    def dump_markers(fo, marker_indx):
      for i in marker_indx:
        m = self.mset.markers[i]
        chrom, pos = m.position
        # FIXME: we currently do not have a way to estimate the
        # genetic distance, so we force it to 0
        fo.write('%s\t%s\t%s\t%s\n' % (chrom, m.label, 0, pos))
    with open(self.base_path + '.map', 'w') as fo:
      fo.write('# map based on mset %s aligned on %s\n' %
               (self.mset.id, self.ref_genome))
      s = self.selected_markers if self.selected_markers \
                                else xrange(len(self.mset))
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
                            individual ids to values that can be put in
                            column 6 (phenotype) of a ped file
    :type phenotype_by_id: dict
    """
    if not phenotype_by_id:
      phenotype_by_id = {None: 0}
    allele_patterns = { 0 : 'A A', 1 : 'B B', 2 : 'A B', 3 : '0 0'}
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
      self.ped_file = open(self.base_path + '.ped', 'w')
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
        # write \n !

  def close(self):
    if self.ped_file:
      self.ped_file.close()
    self.ped_file = None


def read_ssc(fn, mset):
  """
  Read a file with mimetypes.SSC_FILE mimetype and return the prob and
  conf arrays for a given SNPMarkersSet mset.

  :param mset: a reference markers set
  :type mset: SNPMarkersSet

  :param fn: ssc file name
  :type fn: str
  """
  if not mset.has_markers():
    mset.load_markers()
  n_markers = len(mset)
  probs = np.empty((2, n_markers), dtype=np.float32)
  probs[:] = 1/3.
  confs = np.zeros((n_markers,), dtype=np.float32)
  vid_to_marker = dict((m.id, (m, i)) for (i, m) in enumerate(mset.markers))
  reader = MessageStreamReader(fn)
  for i in xrange(n_markers):
    _, snp_id, _, conf, _, _, w_AA, w_AB, w_BB = reader.read()
    m, idx = vid_to_marker[snp_id]
    S = w_AA + w_AB + w_BB
    p_AA, p_BB = w_AA / S, w_BB / S
    if m.flip:
      p_AA, p_BB = p_BB, p_AA
    probs[0,idx] = p_AA
    probs[1,idx] = p_BB
    confs[idx] = conf
  return probs, confs
