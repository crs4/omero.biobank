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

def propagate_family(family, children):
  down_front = down_propagate_front(family, children)
  up_front   = up_propagate_front(family)
  new_front = up_front.union(down_front)
  if len(new_front) == 0:
    return family
  else:
    return propagate_family(family + new_front, children)

MAX_COMPLEXITY=15
def grow_family(seeds, children, max_complexity=MAX_COMPLEXITY):
  """
  Will grow family, following two ways parental relationships, from
  the list of seeds up to the largest possible complexity lower equal
  to the assigned max_complexity.

  :param seeds: initial group of individuals, it should be a family,
                possibly composed by a single individual
  :type  seeds: set of individuals
  :param max_complexity: the maximal acceptable bit complexity
  :type max_complexity: integer, default %d
  :rtype: a set with the resulting family
  """ % MAX_COMPLEXITY

  family = seeds
  bc = 0
  while bc <= max_complexity:
    pre_size = len(family)
    down_front = down_propagate_front(family, children)
    family = family.union(down_front)
    up_front = up_propagate_front(family)
    family = family.union(up_front)
    if len(family) == pre_size:
      return family
    # FIXME: to compute bit_complexity at each cycle is rather stupid,
    # since it could be computed incrementally.
    bc = compute_bit_complexity(family)
  return family

def split_family(family, max_complexity=19):
  """
  Split a family pedigree in partially overlapping sub pedigrees with
  bit complexity lower than max_complexity.

  This is a specialized version of splitting that aims to support
  imputation of genotype of non-genotyped individuals. It tries to produce
  subgraphs that minimize the required computational load.

  :param family: a list of individuals
  :type  family: list
  :param max_complexity: the requested maximal complexity
  :type max_complexity: integer (default 19)
  :rtype: list of pedigrees with bit complexity lower than max_complexity

  %s

  """ % INDIVIDUAL_DEFINITION_DOC
  MAX_COMPLEXITY=19

  if compute_bit_complexity(family) < MAX_COMPLEXITY:
    return [family]

  children, non_founders, founders, couples = analyze(family)
  non_founders_not_genotyped = filter(lambda i: not i.genotyped, non_founders)

  distance = {}
  for c in it.combinations(non_founders_not_genotyped):
    distance[c] = distance(c[0], c[1])


















