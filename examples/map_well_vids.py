""" ...

Replaces the individual label with the platewell VID in the 'source'
column of the tsv input files for DataSample(s) produced by
write_immuno_dataset.py
"""
import sys, os, argparse, csv, logging
from contextlib import nested

from bl.vl.kb import KnowledgeBase as KB

LOG_FORMAT = '%(asctime)s|%(levelname)-8s|%(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'
LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']


STUDY = "IMMUNOCHIP"


def make_parser():
  parser = argparse.ArgumentParser(description="map immuno vids")
  parser.add_argument('-i', '--input-file', metavar='FILE', required=True,
                      help='input file')
  parser.add_argument('-o', '--output-file', metavar='FILE', required=True,
                      help='output file')
  parser.add_argument('--logfile', type=str, help='log file (default=stderr)')
  parser.add_argument('--loglevel', type=str, choices=LOG_LEVELS,
                      help='logging level', default='INFO')
  parser.add_argument('-H', '--host', type=str, help='omero hostname',
                      default='localhost')
  parser.add_argument('-U', '--user', type=str, help='omero user',
                      default='root')
  parser.add_argument('-P', '--passwd', type=str, help='omero password',
                      required=True)
  return parser


def main(argv):
  parser = make_parser()
  args = parser.parse_args(argv)
  log_level = getattr(logging, args.loglevel)
  kwargs = {'format': LOG_FORMAT, 'datefmt': LOG_DATEFMT, 'level': log_level}
  if args.logfile:
    kwargs['filename'] = args.logfile
  logging.basicConfig(**kwargs)
  logger = logging.getLogger()
  with nested(open(args.input_file), open(args.output_file, "w")) as (f, fo):
    reader = csv.DictReader(f, delimiter="\t")
    writer = csv.DictWriter(fo, reader.fieldnames, delimiter="\t",
                            lineterminator=os.linesep)
    writer.writeheader()
    vid_mapper = VidMapper(args.host, args.user, args.passwd, logger)
    for r in reader:
      try:
        vid_mapper.map_vid(r)
      except ValueError:
        continue
      else:
        writer.writerow(r)


class VidMapper(object):

  def __init__(self, host, user, passwd, logger):
    self.logger = logger
    self.kb = KB(driver="omero")(host, user, passwd)
    plates = self.kb.get_objects(self.kb.TiterPlate)
    self.logger.info("fetched %d plates" % len(plates))
    self.plate_map = {}
    self.enroll_map = {}
    for p in plates:
      self.plate_map[p.ome_obj.id._val] = p.barcode
    s = self.kb.get_study(STUDY)
    enrolls = self.kb.get_enrolled(s)
    self.logger.info("fetched %d enrollments" % len(enrolls))
    for e in enrolls:
      wells = [w for w in self.kb.get_vessels_by_individual(e.individual,
                                                       "PlateWell")]
      self.enroll_map[e.studyCode] = wells

  def map_vid(self, r):
    en_code = r["source"]
    pl_barcode = en_code.split("|")[1]
    wells = self.enroll_map[en_code]
    self.logger.info("found %d wells for %s" % (len(wells), en_code))
    for w in wells:
      if self.plate_map[w.container.ome_obj.id._val] == pl_barcode:
        r["source"] = w.id
        break
    else:
      msg = "no well for plate %s" % pl_barcode
      self.logger.warn(msg)
      raise ValueError(msg)


if __name__ == "__main__":
  main(sys.argv[1:])
