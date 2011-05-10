"""
Call Genotypes
==============

The goal of this example is to show a basic workflow that will: FIXME
"""
from bl.vl.sample.kb     import KBError
from bl.vl.sample.kb     import KnowledgeBase as sKB

import logging
logging.basicConfig(level=logging.INFO)
import sys, os, argparse, csv


class App(object):

  def __init__(self):
    self.logger = logging.getLogger("call_gt")
    parser = self.make_parser()
    args = parser.parse_args()
    if not (args.host and args.user and args.passwd and
            args.data_collection_vid):
      self.logger.critical("missing command line arguments")
      sys.exit(1)
    host, user, passwd, keep_tokens = \
          args.host, args.user, args.passwd, args.keep_tokens
    self.dc_vid = args.data_collection_vid
    #-
    self.skb = sKB(driver='omero')(host, user, passwd, keep_tokens)
    #-
    self.study = self.skb.get_study_by_label(args.study_label)
    self.output_file = args.output_file
    self.preload()
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
    parser.add_argument('--data-collection-vid', type=str,
                        help='data collection vid')
    parser.add_argument('-o', '--output-file', type=argparse.FileType('w'),
                        help='output file', default=sys.stdout)
    return parser
  #-----------------------------------------------------------------------
  def preload(self):
    self.logger.info('start pre-loading known data collections')
    data_collections = self.skb.get_bio_samples(self.skb.DataCollection)
    self.data_collections = {}
    for dc in data_collections:
      self.data_collections[dc.id] = dc
    self.logger.info('done pre-loading data collections')
    self.logger.info('there are %d DataCollection(s) in the kb' %
                     len(self.data_collections))


  def run(self):
    data_collection = self.data_collections[self.dc_vid]
    fieldnames = 'path mimetype size sha1'.split()
    tsv = csv.DictWriter(self.output_file, fieldnames, delimiter='\t')
    tsv.writeheader()
    for item in self.skb.get_data_collection_items(data_collection):
      ds = item.dataSample
      dos = self.skb.get_data_objects(ds)
      if dos:
        do = dos[0]
        r = {'path' : do.path,
             'mimetype' : do.mimetype,
             'sha1' : do.sha1,
             'size' : do.size}
        tsv.writerow(r)


def main():
  app = App()
  app.run()


if __name__ == "__main__":
    main()
