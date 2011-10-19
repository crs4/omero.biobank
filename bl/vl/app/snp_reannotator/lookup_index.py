"""
Read a Galaxy genome segment extractor output in interval format and
perform a lookup in the dbSNP index database (see build_index) to get
the true rs label; output a new marker definitions file with the true
rs label and the extended mask.
"""

import shelve, csv, os
from contextlib import nested
from common import MARKER_DEF_FIELDS, SeqNameSerializer, build_index_key


HELP_DOC = __doc__


def build_mask(seq, alleles):
  snp_pos = len(seq)/2
  alleles = "/".join(list(alleles))
  return "%s[%s]%s" % (seq[:snp_pos], alleles, seq[snp_pos+1:])


def write_output(logger, args):
  serializer = SeqNameSerializer()
  index = None
  try:
    index = shelve.open(args.index_file, "r")
    with nested(open(args.input_file), open(args.output_file,'w')) as (f, outf):
      outf.write("\t".join(MARKER_DEF_FIELDS)+"\n")
      input_bn, index_bn, output_bn = map(
        os.path.basename,
        (args.input_file, args.index_file, args.output_file)
        )
      logger.info("looking up %r against %r" % (input_bn, index_bn))
      reader = csv.reader(f, delimiter="\t")
      for i, r in enumerate(reader):
        try:
          label = r[3]
          seq = r[-1].upper()
        except IndexError:
          msg = "%r: bad input format, bailing out" % input_bn
          logger.critical(msg)
          raise ValueError(msg)
        else:
          label, _, _, alleles = serializer.deserialize(label)
          mask = build_mask(seq, alleles)
          key = build_index_key(seq)
          tags = index.get(key, [])
          if len(tags) != 1:
            logger.warning("%r maps to != 1 tags: %r" % (label, tags))
            rs_label = 'None'
          else:
            rs_label, _, _, _ = serializer.deserialize(tags[0])
          outf.write("%s\t%s\t%s\n" % (label, rs_label, mask))
      logger.info("processed %d records overall" % (i+1))
  finally:
    if index:
      index.close()


def make_parser(parser):
  parser.add_argument("-i", "--input-file", metavar="FILE", help="input file")
  parser.add_argument("-o", '--output-file', metavar='FILE',
                      help='output file')
  parser.add_argument("--index-file", metavar="FILE", help="dbSNP index file")


def main(logger, args):
  write_output(logger, args)


def do_register(registration_list):
  registration_list.append(('lookup_index', HELP_DOC, make_parser, main))
