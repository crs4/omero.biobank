class individual(object):
  MALE, FEMALE = 0, 1
  def __init__(self, label, father, mother, sex, genotyped):
    self.label = label
    self.father = father
    self.mother = mother
    self.sex    = sex
    self.genotyped = genotyped

def propagate(family, front):
  for father, mother in front:
    pass

def simulate(n_gen, n_founders, birth_rate, wedding_rate, in_breeding_rate):
  family = []
  couples = []
  for i in range(0, n_founders, 2):
    father = individual(i, None, None, individual.MALE, True)
    mother = individual(i+1, None, None, individual.FEMALE, True)
    couples.append((father, mother))
    family.append(father)
    family.append(mother)
  gen = 0
  front = couples
  while gen < n_gen:
    family, front = propagate(family, front)
  return family





