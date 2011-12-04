"""
A quick and dirty collection of pedigree manipulation functions.

"""
import itertools as it

INDIVIDUAL_DEFINITION_DOC = """
  An individual I is, as far this function is concerned, an object
  with the following attributes:

   - I.id       an unique string identifying this individual
   - I.father   the individual mother or None if not known
   - I.mother   the individual father or None if not known
   - I.gender      FIXME (an integer? a constant?)
"""

MAX_COMPLEXITY=19


#-----------------------------------------------------------------------------

def import_pedigree(recorder, istream):
  """
  Given a stream of individuals it will manage the flow so that it is
  guaranteed that parents are recorded before their children.

  :param recorder: a recorder object
  :type recorder: an object that presents a method with signature
                  .record(label, gender, father, mother) FIXME also a method
                  .retrieve(label)

  :param istream: the stream of individuals that should be imported
  :type istream:  an iterator of individuals I.

  %s

  """ % INDIVIDUAL_DEFINITION_DOC

  family = []
  by_id = {}
  for i in istream:
    family.append(i)
    by_id[i.id] = (i, None)

  def register(x):
    i = recorder.retrieve(x.id)
    if not i:
      father = None if x.father is None else by_id[x.father][1]
      mother = None if x.mother is None else by_id[x.mother][1]
      i = recorder.record(x.id, x.gender, father, mother)
    by_id[x.id] = (x, i)

  founders, non_founders, dangling, couples, children = analyze(family)
  if dangling:
    raise ValueError('there are %d dangling individual IDs: %s' %
                     (len(dangling), dangling))
  visited = {}
  kids = {}
  couples_by_partner = {}

  for c in couples:
    visited[c] = False
    kids[c] = children[c[0]].intersection(children[c[1]])
    couples_by_partner.setdefault(c[0], set()).add(c)
    couples_by_partner.setdefault(c[1], set()).add(c)

  wave = []
  for f in founders:
    register(f)
    for c1 in couples_by_partner.get(f.id, []):
      if visited[c1]:
        wave.append(c1)
      else:
        visited[c1] = True

  while wave:
    # FIXME this is a dirty trick...
    recorder.dump_out()
    new_wave = []
    for c in  wave:
      for k in kids[c]:
        register(k)
        for c1 in couples_by_partner.get(k.id, []):
          if visited[c1]:
            new_wave.append(c1)
          else:
            visited[c1] = True
    wave = new_wave


def analyze(family):
  """
  Analyze pedigree to extract:

     - F  the list of founders

     - NF the list of non-founders

     - D the list of dangling individuals ids, that is individuals
       that are mentioned as a parent but that do not appear in
       family as members

     - C  the list of couples

     - CH a dictionary of children set with individual id as key.

  :param family: a list of individuals
  :type  family: list
  :rtype: tuple(F, NF, D, C, CH)

  %s

  """ % INDIVIDUAL_DEFINITION_DOC
  if len(family) == 0:
    return ([], [], [], [], {})

  founders     = []
  non_founders = []
  children = {}
  couples = set([])
  by_id = {}
  # HACK in special case for omero objects (see ticket:101)
  #
  if hasattr(family[0], 'omero_id'):
    by_omero_id = {}
    for x in family:
      by_omero_id[x.omero_id] = x
    def resolve(x):
      if x is None:
        return None
      return by_omero_id[x.omero_id]
  else:
    def resolve(x):
      return x
  for i in family:
    by_id[i.id] = i
    if i.father is None and i.mother is None:
      founders.append(i)
    else:
      non_founders.append(i)
      children.setdefault(resolve(i.father), set()).add(i)
      children.setdefault(resolve(i.mother), set()).add(i)
      couples.add((resolve(i.father), resolve(i.mother)))
  if len(children) > 0:
    insiders = founders + non_founders
    parents = children.keys()
    # HACK in special case when father and mother are given as labels,
    # HACK this is needed because of a legacy bug in import.individual
    if isinstance(parents[0], str):
      insiders = [x.id for x in insiders]
      dangling = [by_id[x.id] for x in
                  filter(lambda x: x not in insiders, parents)]
    else:
      dangling = filter(lambda x: x not in insiders, parents)
      tmp_children = {}
      for k in children:
        tmp_children[k.id] = children[k]
      children = tmp_children
  else:
    dangling = []
  return (founders, non_founders, dangling, list(couples), children)

def compute_bit_complexity(family, genotyped):
  """
  Bit complexity defined as:
  .. math::
    b = 2n - (f + c)

  Where:
     - n = number of non-founders
     - f = number of founders (any individual whose parents are not known, genotyped or not)
     - c = number of couples of non-genotyped founders

  :param family: a list of individuals
  :type  family: list

  :param genotyped: the genotyped status of all the individuals in family
  :type genotyped: a dict like object that returns the genotyped
                   status (bool) when indexed by individual.id

  :rtype: integer

  %s

  """ % INDIVIDUAL_DEFINITION_DOC
  founders, non_founders, couples, children = analyze(family)
  not_gt_couples = filter(lambda c : not (genotyped[c[0].id]
                                          or genotyped[c[1].id]),
                          couples)
  return 2*len(non_founders) - len(founders) - len(not_gt_couples)

def down_propagate_front(family, children):
  down_front = map(lambda x: children.get(x.id, set([])), family)
  down_front = set([]).union(*down_front)
  return down_front

def up_propagate_front(family, resolve):
  up_front = map(lambda x: set([resolve(x.father), resolve(x.mother)]), family)
  up_front = set([]).union(*up_front) - set([None])
  return up_front

def propagate_family_helper(family, children, resolve):
  # FIXME, remove tail recursion
  pre_size = len(family)
  down_front = down_propagate_front(family, children)
  family = family.union(down_front)
  up_front = up_propagate_front(family, resolve)
  family = family.union(up_front)
  if len(family) > pre_size:
    return propagate_family_helper(family, children, resolve)
  else:
    return family

def propagate_family(family, children):
  return list(propagate_family_helper(set(family), children))

def split_disjoint(family, children):
  if len(family) == 0:
    return []
  # HACK required by bug in CoreOmeroWrapper (see ticket:101)
  if hasattr(family[0], 'omero_id'):
    by_omero_id = {}
    for x in family:
      by_omero_id[x.omero_id] = x
    def resolve(x):
      if x is None:
        return None
      return by_omero_id[x.omero_id]
  else:
    def resolve(x):
      return x
  splits = []
  family = set(family)
  while len(family) > 0:
    item = iter(family).next()
    split = propagate_family_helper(set([item]), children, resolve)
    family = family - split
    splits.append(list(split))
  return splits

def grow_family(seeds, children, genotyped, max_complexity=MAX_COMPLEXITY):
  """
  Will grow family, following two ways parental relationships, from
  the list of seeds up to the largest possible complexity lower equal
  to the assigned max_complexity.

  :param seeds: initial group of individuals, it should be a family,
                possibly composed by a single individual
  :type  seeds: list of individuals

  :param genotyped: the genotyped status of all the individuals in family
  :type genotyped: a dict like object that returns the genotyped
                   status (bool) when indexed by individual.id

  :param max_complexity: the maximal acceptable bit complexity
  :type max_complexity: integer, default %d
  :rtype: a list with the resulting family
  """ % MAX_COMPLEXITY

  family = set(seeds)
  bc = 0
  delta_size = len(family)
  while delta_size > 0:
    down_front = down_propagate_front(family, children)
    new_fam = family.union(down_front)
    up_front = up_propagate_front(new_fam)
    new_fam = new_fam.union(up_front)
    # FIXME: to compute bit_complexity at each cycle is rather stupid,
    # since it could be computed incrementally.
    delta_size = len(new_fam) - len(family)
    bc = compute_bit_complexity(list(new_fam), genotyped)
    if bc > max_complexity:
      break
    family = new_fam
  return list(family)

def split_family(family, genotyped, max_complexity=MAX_COMPLEXITY):
  """
  Split a family pedigree in partially overlapping sub pedigrees with
  bit complexity lower than max_complexity.

  This is a specialized version of splitting that aims to support
  imputation of genotype of non-genotyped individuals. It tries to produce
  subgraphs that minimize the required computational load.

  The total number of unique individuals contained in the resulting
  families will be lower or equal to the number of unique individuals
  contained in the given family.

  :param family: a list of individuals
  :type  family: list

  :param genotyped: the genotyped status of all the individuals in family
  :type genotyped: a dict like object that returns the genotyped
                   status (bool) when indexed by individual.id

  :param max_complexity: the requested maximal complexity
  :type max_complexity: integer (default %d)
  :rtype: list of families with bit complexity lower than max_complexity

  %s

  """ % (MAX_COMPLEXITY, INDIVIDUAL_DEFINITION_DOC)

  if compute_bit_complexity(family, genotyped) < max_complexity:
    return [family]

  founders, non_founders, couples, children = analyze(family)
  non_founders_not_genotyped = filter(lambda i: not genotyped[i.id],
                                      non_founders)

  # distance = {}
  # for c in it.combinations(non_founders_not_genotyped):
  #   distance[c] = distance(c[0], c[1])

  # trivial implementation: sort on number of children
  def number_of_children(i):
    return len(children.get(i, []))
  fams = []
  while len(non_founders_not_genotyped) > 0:
    non_founders_not_genotyped = sorted(non_founders_not_genotyped,
                                        key=number_of_children)
    i = non_founders_not_genotyped[0]
    f = grow_family([i], children, max_complexity)
    cbn1 = compute_bit_complexity(f, genotyped)
    non_founders_not_genotyped = list(set(non_founders_not_genotyped) -
                                      set(f))
    cbn = compute_bit_complexity(f, genotyped)
    fams.append(f)
  return fams
