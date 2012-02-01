# BEGIN_COPYRIGHT
# END_COPYRIGHT

class IndividualStub(object):
  """
  A class that quacks as expected by import_pedigree.
  """
  def __init__(self, label, gender, father, mother):
    self.id = label
    self.gender = gender
    self.father = father
    self.mother = mother

  def is_male(self):
    return self.gender.upper() == 'MALE'

  def is_female(self):
    return self.gender.upper() == 'FEMALE'
  
  def __hash__(self):
    return hash(self.id)

  def __eq__(self, obj):
    return hash(self) == hash(obj)

  def __ne__(self, obj):
    return not self.__eq__(obj)

  def __str__(self):
    return '%s (%s) [%s, %s]' % (self.id, self.gender,
                                 self.father.id if self.father else None,
                                 self.mother.id if self.mother else None)
