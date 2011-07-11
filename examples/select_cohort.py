"""

Select a Cohort
======================================

This example describes how one can use vl to select a cohort of
individuals.

The basic idea is that the selected individuals, e.g.,
by phenotype and age, are enrolled in an ad-hoc study.

For instance, in this example, we will select an affected and a control
group with the same proportion of male/female.

FIXME extend example with age at onset.

"""

from bl.vl.kb import KnowledgeBase as KB
import numpy as np

import argparse
import os, sys
import time
import itertools as it
import logging
logging.basicConfig(level=logging.INFO)


#------------------------------------------------------------------------------
def make_parser():
  parser = argparse.ArgumentParser(description="Basic computations example")
  parser.add_argument('-H', '--host', type=str,
                      help='omero host system',
                      default='localhost')
  parser.add_argument('-U', '--user', type=str,
                      help='omero user',
                      default='root')
  parser.add_argument('-P', '--passwd', type=str,
                      help='omero user passwd')
  return parser
#------------------------------------------------------------------------------

DIAGNOSIS = 'openEHR-EHR-EVALUATION.problem-diagnosis.v1'
DIAGNOSIS_FIELD = 'at0002.1'
DIABETES_TYPE_1 = 'icd10-cm:E10'
#--
EXCLUSION = 'openEHR-EHR-EVALUATION.exclusion-problem_diagnosis.v1'
EXCLUSION_FIELD = 'at0002.1'

class App(object):

  def __init__(self, host, user, passwd):
    self.kb = KB(driver='omero')(host, user, passwd)
    self.logger = logging.getLogger()

  def get_ehr_iterator(self):
    inds = self.kb.get_objects(self.kb.Individual)
    inds_by_vid = dict([(i.id, i) for i in inds])
    for e in self.kb.get_ehr_iterator():
      if not e[0] in inds_by_vid:
        #FIXME we need to do this for potential stray records left by testing
        continue
      yield (inds_by_vid[e[0]], e[1])

  # FIXME in a future version, we will be able to issue actual aql
  # statements.
  def do_enrollment(self):
    controls = []
    affected = []
    for i, ehr in self.get_ehr_iterator():
      if ehr.matches(DIAGNOSIS, DIAGNOSIS_FIELD, DIABETES_TYPE_1):
        affected.append(i)
      elif ehr.matches(EXCLUSION): # FIXME we assume that this is enough
        controls.append(i)
    print 'there are %d controls' % len(controls)
    print 'there are %d affected with E10' % len(affected)

def main():
  parser = make_parser()
  args = parser.parse_args()
  if not (args.passwd):
    parser.print_help()
    sys.exit(1)

  app = App(args.host, args.user, args.passwd)
  app.do_enrollment()

if __name__ == "__main__":
    main()
