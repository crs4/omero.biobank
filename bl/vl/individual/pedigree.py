"""
A quick and dirty collection of pedigree manipulation functions.


"""
import itertools as it

INDIVIDUAL_DEFINITION_DOC = """
  An individual I is, as far this function is concerned, an object that has the following attributes:

   - I.id       an unique string identifying this individual
   - I.father   the individual mother or None if not known
   - I.mother   the individual father or None if not known
   - I.sex      FIXME (an integer? a constant?)
   - I.genotyped a boolean
"""

MAX_COMPLEXITY=19


#--------------------------------------------------------------------------------------

def import_pedigree(recorder, istream):
  """
  Given a stream of individuals it will record them so that it is
  guaranteed that parents are recorded before their children.

  :param recorder: a recorder object
  :type recorder:  an object that presents a method with signature
                   .record( , father, mother) FIXME

  :param istream: the stream of individuals that should be imported
  :type istream:  an iterator of individuals I.

  %s

  Note: The implementation is very naive. In the worst case, it will
  be quadratic in the number of individual.
  """ % INDIVIDUAL_DEFINITION_DOC
  def register(i_map):
    work_to_do = False
    for k in i_map.keys():
      registered, i, gender, father, mother = i_map[k]
      if registered:
        continue
      if father is None and mother is None:
        i_map[k] = (True, recorder.record(k, gender, None, None), gender, None, None)
      elif not father or not mother:
        work_to_do = True
      elif i_map[father][0] and i_map[mother][0]:
        i_map[k] = (True, recorder.record(k,
                                          gender, i_map[father][1], i_map[mother][1]),
                    gender, None, None)
      else:
        work_to_do = True
    return work_to_do

  i_map = {}
  for x in istream:
    i_map[x.id] = (False, None, x.gender, x.father, x.mother)

  work_to_do = register(i_map)
  while work_to_do:
    work_to_do = register(i_map)


def analyze(family):
  """
  Analyze pedigree to extract:
     - F  the list of founders
     - NF the list of non-founders
     - C  the list of couples
     - CH a dictionary of children set with individual id as key.

  :param family: a list of individuals
  :type  family: list
  :rtype: tuple(F, NF, C, CH)

  %s

  """ % INDIVIDUAL_DEFINITION_DOC
  founders     = []
  non_founders = []
  children = {}
  couples = set([])
  for i in family:
    if i.father is None and i.mother is None:
      founders.append(i)
    else:
      non_founders.append(i)
      children.setdefault(i.father, set()).add(i)
      children.setdefault(i.mother, set()).add(i)
      couples.add((i.father, i.mother))
  # for c in it.combinations(founders):
  #   if (not (c[0].genotyped or c[1].genotyped)
  #       and children[c[0].id] - children[c[1].id]):
  #     couples.append(c)
  return (founders, non_founders, list(couples), children)

def compute_bit_complexity(family):
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
  :rtype: integer

  %s

  """ % INDIVIDUAL_DEFINITION_DOC
  founders, non_founders, couples, children = analyze(family)
  not_gt_couples = filter(lambda c : not (c[0].genotyped or c[1].genotyped),
                          couples)
  return 2*len(non_founders) - len(founders) - len(not_gt_couples)

def down_propagate_front(family, children):
  down_front = map(lambda x: set(children.get(x, [])), family)
  down_front = set([]).union(*down_front)
  return down_front

def up_propagate_front(family):
  up_front = map(lambda x: set([x.father, x.mother]), family)
  up_front = set([]).union(*up_front) - set([None])
  return up_front

def propagate_family_helper(family, children):
  pre_size = len(family)
  down_front = down_propagate_front(family, children)
  family = family.union(down_front)
  up_front = up_propagate_front(family)
  family = family.union(up_front)
  if len(family) > pre_size:
    return propagate_family_helper(family, children)
  else:
    return family

def propagate_family(family, children):
  return list(propagate_family_helper(set(family), children))

def split_disjoint(family, children):
  splits = []
  family = set(family)
  while len(family) > 0:
    split = propagate_family_helper(set([family.pop()]), children)
    family = family - split
    splits.append(list(split))
  return splits

def grow_family(seeds, children, max_complexity=MAX_COMPLEXITY):
  """
  Will grow family, following two ways parental relationships, from
  the list of seeds up to the largest possible complexity lower equal
  to the assigned max_complexity.

  :param seeds: initial group of individuals, it should be a family,
                possibly composed by a single individual
  :type  seeds: list of individuals
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
    bc = compute_bit_complexity(list(new_fam))
    if bc > max_complexity:
      break
    family = new_fam
  return list(family)

def split_family(family, max_complexity=MAX_COMPLEXITY):
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
  :param max_complexity: the requested maximal complexity
  :type max_complexity: integer (default %d)
  :rtype: list of families with bit complexity lower than max_complexity

  %s

  """ % (MAX_COMPLEXITY, INDIVIDUAL_DEFINITION_DOC)

  if compute_bit_complexity(family) < max_complexity:
    return [family]

  founders, non_founders, couples, children = analyze(family)
  non_founders_not_genotyped = filter(lambda i: not i.genotyped, non_founders)

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
    cbn1 = compute_bit_complexity(f)
    non_founders_not_genotyped = list(set(non_founders_not_genotyped) -
                                      set(f))
    cbn = compute_bit_complexity(f)
    fams.append(f)
  return fams
















