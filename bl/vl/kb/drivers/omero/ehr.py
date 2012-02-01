# BEGIN_COPYRIGHT
# END_COPYRIGHT

class EHR(object):

  def __init__(self, recs):
    self.recs = {}
    for r in recs:
      self.recs.setdefault(r['archetype'], []).append(r)

  def get_field_values(self, archetype, field):
    res = []
    for r in self.recs.get(archetype, []):
      if field in r['fields']:
        res.append((r['timestamp'], r['fields'][field]))
    return res

  def matches(self, archetype, field=None, value=None):
    if not archetype in self.recs:
      return False
    if field is None:
      return True
    for r in self.recs[archetype]:
      if field in r['fields']:
        if value is None or value == r['fields'][field]:
          return True
    else:
      return False
