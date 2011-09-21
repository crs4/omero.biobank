"""
Read a Galaxy genome segment extractor output in interval format and
perform a lookup in the dbSNP index database (see build_index) to get
the true rs label; replace the putative rs label with the true one in
the marker definitions file.
"""

## FIXME: we should also replace the mask with the standard length
## one. We need the alleles for that -- SNP pos is (len(mask)-1)/2
##
## Possible solution: put both the the A/B/C/D letter and the true
## allele list in the fastq ID.

import argparse, shelve, csv, os
from contextlib import nested
from common import check_mask, MARKER_DEF_FIELDS


HELP_DOC = __doc__


def get_rs_labels_map(logger, args):
  rs_labels_map = {}
  index = None
  try:
    index = shelve.open(args.index_file, "r")
    with open(args.input_file) as f:
      input_bn, index_bn = map(os.path.basename,
                               (args.input_file, args.index_file))
      logger.info("looking up %r against %r" % (input_bn, index_bn))
      reader = csv.reader(f, delimiter="\t")
      for i, r in enumerate(reader):
        try:
          label = r[3]
          seq = r[4].upper()
        except IndexError:
          msg = "%r: bad input format, bailing out" % bn
          logger.critical(msg)
          raise ValueError(msg)
        else:
          rs_labels_map[label] = index.get(seq, [])
      logger.info("processed %d records overall" % (i+1))
  finally:
    if index:
      index.close()
  return rs_labels_map


def write_output(logger, args, rs_labels_map):
  with nested(open(args.marker_def_file),
              open(args.output_file, "w")) as (f, outf):
    logger.info("reannotating %r" % os.path.basename(f.name))
    marker_count = 0
    reader = csv.DictReader(f, delimiter="\t")
    outf.write("\t".join(MARKER_DEF_FIELDS)+"\n")
    for r in reader:
      marker_count += 1
      rs_labels = rs_labels_map.get(r['label'], [])
      if len(rs_labels) != 1:
        logger.warning("%r maps to the following rs_labels: %r" %
                       (r['label'], rs_labels))
        r['rs_label'] = 'None'
      else:
        r['rs_label'] = rs_labels[0]
      outf.write("%s\t%s\t%s\n" % (r['label'], r['rs_label'], r['mask']))
  return marker_count


def make_parser(parser):
  parser.add_argument("-i", "--input-file", metavar="FILE", help="input file")
  parser.add_argument("-o", '--output-file', metavar='FILE',
                      help='output file')
  parser.add_argument("--index-file", metavar="FILE", help="dbSNP index file")
  parser.add_argument("--marker-def-file", metavar="FILE",
                      help="original marker definitions file")


def main(logger, args):
  rs_labels_map = get_rs_labels_map(logger, args)
  marker_count = write_output(logger, args, rs_labels_map)
  logger.info("reannotated %d markers" % marker_count)


def do_register(registration_list):
  registration_list.append(('lookup_index', HELP_DOC, make_parser, main))
