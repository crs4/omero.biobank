# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""

This example script shows how one could interrogate KB to gather EHR info.


get all individuals that are affected by X and that have a recent blood
pressure measurement above Y.

"""

def foo():
ehr_query = """
select r/individual
from EHR r contains
EVALUATION  e [openEHR-EHR-EVALUATION.problem-diagnosis.v1] and
OBSERVATION o [openEHR-EHR-OBSERVATION.blood_pressure.v1]
where
e/evaluation[at0000.1]/ITEM_TREE[at0001]/ELEMENT[at0002.1]/value/value == $dc
and
o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/value >= 140
"""

sql_query = """
select *
from foo
where
id > 10
"""

  for i in kb.do_ehr_query(ehr_query,
                           {'dc' : 'terminology://icd10/diagnosis?code=E10'}):
    print i.id


def parser(query):
  """
  Trivial (and very limited) AQL parser implementation.

  """
  query = query.replace('\n', ' ')
  '\A(.*)\s*from\s+EHR\s+([a-zA-Z]\w*)\s*(.*)\Z'



ref_archetype=='openEHR-EHR-EVALUATION.problem-diagnosis.v1'
&
atype_field=='at002.1'
&
string_field == 'terminology://icd10/diagnosis?code=E10'



