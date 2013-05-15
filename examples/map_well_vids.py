# BEGIN_COPYRIGHT
# END_COPYRIGHT

""" ...

Replaces the individual label with the platewell VID in the 'source'
column of the tsv input files for DataSample(s) produced by
write_immuno_dataset.py
"""
import sys, os, argparse, csv, logging
from contextlib import nested
try:
  from collections import Counter
except ImportError:
  sys.exit("ERROR: This script needs python >=2.7")
from bl.vl.kb import KnowledgeBase as KB
import bl.vl.utils.ome_utils as vlu

LOG_FORMAT = '%(asctime)s|%(levelname)-8s|%(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'
LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']


STUDY = "IMMUNOCHIP"


class VidMapper(object):

  def __init__(self, host, user, passwd, logger, study):
    self.logger = logger
    self.study = study
    self.kb = KB(driver="omero")(host, user, passwd)
    plates = self.kb.get_objects(self.kb.TiterPlate)
    self.logger.info("fetched %d plates" % len(plates))
    self.plate_map = {}
    self.enroll_map = {}
    for p in plates:
      self.plate_map[p.omero_id] = p.barcode
    s = self.kb.get_study(self.study)
    enrolls = self.kb.get_enrolled(s)
    self.logger.info("fetched %d enrollments" % len(enrolls))
    for e in enrolls:
      self.logger.debug('Retrieving wells for %s' % e.studyCode)
      wells = [w for w in self.kb.get_vessels_by_individual(e.individual,
                                                            "PlateWell")]
      self.enroll_map[e.studyCode] = wells

  def map_vid(self, r):
    en_code = r["source"]
    pl_barcode = en_code.split("|")[1]
    try:
      wells = self.enroll_map[en_code]
    except KeyError:
      msg = "%s is not enrolled in %s" % (en_code, self.study)
      self.logger.error(msg)
      raise ValueError(msg)
    self.logger.info("found %d wells for %s" % (len(wells), en_code))
    imm_wells = [w for w in wells
                 if self.plate_map[w.container.omero_id] == pl_barcode]
    if len(imm_wells) > 1:
      msg = ("more than 1 (%d) immuno wells for plate %s" %
             (len(imm_wells), pl_barcode))
      self.logger.error(msg)
      raise ValueError(msg)
    elif len(imm_wells) == 0:
      msg = "no immuno well for plate %s" % pl_barcode
      self.logger.warn(msg)
      raise ValueError(msg)
    else:
      r["source"] = imm_wells[0].id


def make_parser():
  parser = argparse.ArgumentParser(description="map immuno vids")
  parser.add_argument('-i', '--input-file', metavar='FILE', required=True,
                      help='input file')
  parser.add_argument('-o', '--output-file', metavar='FILE', required=True,
                      help='output file')
  parser.add_argument('--logfile', type=str, help='log file (default=stderr)')
  parser.add_argument('--loglevel', type=str, choices=LOG_LEVELS,
                      help='logging level', default='INFO')
  parser.add_argument('-H', '--host', type=str, help='omero hostname')
  parser.add_argument('-U', '--user', type=str, help='omero user')
  parser.add_argument('-P', '--passwd', type=str, help='omero passwd')
  parser.add_argument('-s', '--study', metavar='STRING', default=STUDY,
                      help='study label')
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
