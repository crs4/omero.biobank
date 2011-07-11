class EHR(object):

  def __init__(self, recs):
    self.recs = dict([(r['archetype'], r) for r in recs])

  def matches(self, archetype, field=None, value=None):
    if not archetype in self.recs:
      return False
    if field is None:
      return True
    #--
    if not field in self.recs[archetype]['fields']:
      return False
    if value is None:
      return True
    #--
    return value == self.recs[archetype]['fields'][field]

