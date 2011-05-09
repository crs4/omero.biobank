"""

Gender Check
============


The goal of this example is to show a basic workflow that will:

   * select all the individuals enrolled in a study for each
     individual, check if they have an associated genotype dataset and
     identify an actual storage instance of the same;

   * invoke an external tool (chipal) to compute genotype;

   * compare computational result with the putative one.

"""
from bl.vl.sample.kb     import KBError
from bl.vl.sample.kb     import KnowledgeBase as sKB
from bl.vl.individual.kb import KnowledgeBase as iKB

import sys, os
import logging
import argparse
import csv

class App(object):

  def __init__(self):
    parser = self.make_parser()
    args = parser.parse_args()
    host, user, passwd, keep_tokens = \
          args.host, args.user, args.passwd, args.keep_tokens
    #-
    self.skb = sKB(driver='omero')(host, user, passwd, keep_tokens)
    self.ikb = iKB(driver='omero')(host, user, passwd, keep_tokens)
    self.known_enrollments = {}
    self.logger = logging.getLogger()
    #-
    self.study = self.skb.get_study_by_label(args.study_label)
    self.output_file = args.output_file
    self.preload()
    self.gm = self.ikb.get_gender_table()
    self.gm_by_object = {}
    self.gm_by_object[self.gm["MALE"].id] = "MALE"
    self.gm_by_object[self.gm["FEMALE"].id] = "FEMALE"
  #-----------------------------------------------------------------------
  def make_parser(self):
    parser = argparse.ArgumentParser(description="A magic importer")
    parser.add_argument('-H', '--host', type=str,
                        help='omero host system',
                        default='localhost')
    parser.add_argument('-U', '--user', type=str,
                        help='omero user',
                        default='root')
    parser.add_argument('-P', '--passwd', type=str,
                        help='omero user passwd')
    parser.add_argument('-K', '--keep-tokens', type=int,
                        default=1, help='omero tokens for open session')

    parser.add_argument('-S', '--study-label', type=str,
                        help='selected study label')
    parser.add_argument('-o', '--output-file', type=argparse.FileType('w'),
                        help='output file', default=sys.stdout)
    return parser
  #-----------------------------------------------------------------------
  def preload(self):
    self.logger.info('start pre-loading known enrolled individuals')
    known_enrollments = self.ikb.get_enrolled(self.study)
    for e in known_enrollments:
      self.known_enrollments[e.studyCode] = e
    self.logger.info('done pre-loading known enrolled individuals')
    self.logger.info('there are %d enrolled individuals in study %s' %
                     (len(self.known_enrollments), self.study.label))
    #--

  def run(self):
    fieldnames = 'study label gender path mimetype size sha1'.split()
    tsv = csv.DictWriter(self.output_file, fieldnames, delimiter='\t')
    tsv.writeheader()
    for l,e in self.known_enrollments.iteritems():
      affy_datasets = self.skb.get_descendants(e.individual,
                                               self.skb.AffymetrixCel)
      if affy_datasets:
        for ds in affy_datasets:
          dos = self.skb.get_data_objects(ds)
          if dos:
            do = dos[0]
            r = {'study': self.study.label,
                 'label': e.studyCode,
                 'gender': self.gm_by_object[e.individual.gender.id],
                 'path' : do.path,
                 'mimetype' : do.mimetype,
                 'sha1' : do.sha1,
                 'size' : do.size
                 }
            tsv.writerow(r)

def main():
  app = App()
  app.run()

if __name__ == "__main__":
    main()





