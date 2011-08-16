class EHR(object):

  def __init__(self, recs):
    self.recs = {}
    for r in recs:
      self.recs.setdefault(r['archetype'], []).append(r)

  def get_field_values(self, archetype, field):
    """returns list of (timestamp, value) for FIELD for all copies of
    ARCHETYPE records in self."""

    res = []
    for r in self.recs.get(archetype, []):
      if field in r['fields']:
        res.append((r['timestamp'], r['fields'][field]))
    return res


  def matches(self, archetype, field=None, value=None):
    """returns True if self contains a record with archetype ARCHETYPE
    that will contain (optionally) field FIELD and the latter has
    (optionally) value VALUE"""
    if not archetype in self.recs:
      return False
    if field is None:
      return True

    for r in self.recs[archetype]:
      if field in r['fields']:
        break
    else:
      return False

    if value is None:
      return True

    return value == r['fields'][field]

