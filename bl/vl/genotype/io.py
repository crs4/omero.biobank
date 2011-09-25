import array, struct


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


from bl.vl.genotype.algo import project_to_genotype

class PedWriter(object):
  def __init__(self, mset, base_path, ref_genome=None, selected_markers=None):
    self.mset = mset
    self.base_path = base_path
    self.selected_markers = selected_markers
    self.ref_genome = ref_genome
    self.ped_file = None

  def write_map(self):
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
        fo.write('%s\t%s\t%s\n' % (chrom, m.label, 0, pos))

    if self.ref_genome:
      self.mset.load_alignments(self.ref_genome)
    with open(self.base_path + '.map', 'w') as fo:
      fo.write('# map based on mset %s aligned on %s\n' %
               (self.mset.id, self.ref_genome))
      s = self.selected_markers if self.selected_markers \
                                else xrange(len(self.mset))
      dump_markers(fo, s)

  def write_family(self, group, family_members, data_sample_by_id,
                   phenotype_by_id=None):
    allele_patterns = { 0 : 'A A', 1 : 'B B', 2 : 'A B', 3 : '0 0'}
    def dump_genotype(fo, data_sample):
      probs, conf = data_sample.resolve_to_data()
      probs = probs[self.selected_markers] if self.selected_markers \
                                           else probs
      fo.write('\t'.join([allele_patterns[x]
                          for x in project_to_genotype(probs)]))

    if self.ped_file is None:
      self.ped_file = open(self.base_path + '.ped', 'w')
    for i in family_members:
      # Family ID, IndividualID, paternalID, maternalID, sex, phenotype
      fam_id, ind_id = group.id, i.id
      fat_id = 0 if not i.father else i.father.id
      mot_id = 0 if not i.mother else i.mother.id
      gender = {'MALE' : 1, 'FEMALE' : 2}[i.gender.enum_label()]
      pheno  = 0 if not phenotype_by_id else phenotype_by_id[i.id]
      self.ped_file.write('%s\t%s\t%s\t%s\t%s\t%s\t' %
                          (group.id, i.id, fat_id, mot_id, gender, pheno))
      dump_genotype(self.ped_file, data_sample_by_id[i.id])

  def close(self):
    if self.ped_file:
      self.ped_file.close()
    self.ped_file = None







