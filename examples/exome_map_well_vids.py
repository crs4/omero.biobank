# BEGIN_COPYRIGHT
# END_COPYRIGHT

""" ...

Replaces the plate barcode label with the plate label and fix well label in the 'source'
column of the tsv input files for DataSample(s) produced by
prepare_import_exome_chips.py
"""
import sys, os, argparse, csv
from contextlib import nested
try:
  from collections import Counter
except ImportError:
  sys.exit("ERROR: This script needs python >=2.7")

from bl.vl.utils import LOG_LEVELS, get_logger
from bl.vl.kb import KnowledgeBase as KB
import bl.vl.utils.ome_utils as vlu


STUDY = "EXOMECHIP"


class VidMapper(object):

  def __init__(self, host, user, passwd, logger, study):
    self.logger = logger
    self.study = study
    self.kb = KB(driver="omero")(host, user, passwd)
    plates = self.kb.get_objects(self.kb.TiterPlate)
    self.logger.info("fetched %d plates" % len(plates))
    self.plate_map = {}
    for p in plates:
      self.plate_map[p.barcode] = p.label
    s = self.kb.get_study(self.study)
  

  def map_vid(self, r):
    pw_code = r["source"]
    pl_barcode = pw_code.split(":")[0]
    w_label = pw_code.split(":")[1]
    if w_label[1] == "0":
      w_label = "%s%s" %  (w_label[0],  w_label[2])
    r["source"] ="%s:%s" % ( self.plate_map[pl_barcode], w_label )


def make_parser():
  parser = argparse.ArgumentParser(description="map exome chip vids")
  parser.add_argument('-i', '--input-file', metavar='FILE', required=True, help='input file')
  parser.add_argument('-o', '--output-file', metavar='FILE', required=True, help='output file')
  parser.add_argument('--logfile', type=str, help='log file (default=stderr)')
  parser.add_argument('--loglevel', type=str, choices=LOG_LEVELS,
                      help='logging level', default='INFO')
  parser.add_argument('-H', '--host', type=str, help='omero hostname')
  parser.add_argument('-U', '--user', type=str, help='omero user')
  parser.add_argument('-P', '--passwd', type=str, help='omero passwd')
  parser.add_argument('-s', '--study', metavar='STRING', default=STUDY, help='study label')
  return parser

def main(argv):
  parser = make_parser()
  args = parser.parse_args(argv)
  logger = get_logger("main", level=args.loglevel, filename=args.logfile)

  with nested(open(args.input_file), open(args.output_file, "w")) as (f, fo):
    reader = csv.DictReader(f, delimiter="\t")
    writer = csv.DictWriter(fo, reader.fieldnames, delimiter="\t",
                            lineterminator=os.linesep)
    writer.writeheader()
    try:
      host = args.host or vlu.ome_host()
      user = args.user or vlu.ome_user()
      passwd = args.passwd or vlu.ome_passwd()
    except ValueError, ve:
      logger.critical(ve)
      sys.exit(ve)
    vid_mapper = VidMapper(host, user, passwd, logger, args.study)
    counter = Counter()
    for r in reader:
      counter["input_lines"] += 1
      try:
        vid_mapper.map_vid(r)
      except ValueError:
        counter["skipped_lines"] += 1
        continue
      else:
        counter["output_lines"] += 1
        writer.writerow(r)
    for k, v in counter.iteritems():
      logger.info("%s: %d" % (k, v))


if __name__ == "__main__":
  main(sys.argv[1:])
