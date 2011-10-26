"""
Read a marker definitions file and a Galaxy genome segment extractor
output in interval format; perform a lookup in the dbSNP index
database (see build_index) to get the true rs label; output a new
marker definitions file with the true rs label and the extended mask.
"""

import shelve, csv
from contextlib import nested
from common import MARKER_DEF_FIELDS, SeqNameSerializer, build_index_key


HELP_DOC = __doc__


class Status(object):
  ADDED = "ADDED"
  CONFIRMED = "CONFIRMED"
  MULTI_MATCH = "MULTI_MATCH"
  NO_INFO = "NO_INFO"
  NO_MATCH = "NO_MATCH"
  REPLACED = "REPLACED"


def build_mask(seq, alleles):
  snp_pos = len(seq)/2
  alleles = "/".join(list(alleles))
  return "%s[%s]%s" % (seq[:snp_pos], alleles, seq[snp_pos+1:])


def get_extracted_seqs(fn):
  with open(fn) as f:
    reader = csv.reader(f, delimiter="\t")
    data = {}
    serializer = SeqNameSerializer()
    for r in reader:
      try:
        label, _, _, alleles = serializer.deserialize(r[3])
        seq = r[-1].upper()
      except IndexError:
        raise ValueError("%r: bad input format" % fn)
      data[label] = (seq, alleles)
    return data


def write_output(logger, args):
  serializer = SeqNameSerializer()
  index = None
  fields = MARKER_DEF_FIELDS + ("status", "extended_mask")
  try:
    index = shelve.open(args.index_file, "r")
    logger.info("getting extracted sequences")
    extracted_seqs = get_extracted_seqs(args.input_file)
    with nested(open(args.orig_file), open(args.output_file,'w')) as (f, outf):
      outf.write("\t".join(fields)+"\n")
      reader = csv.DictReader(f, delimiter="\t")
      logger.info("looking up against %r" % args.index_file)
      for i, r in enumerate(reader):
        label = r['label']
        old_rs_label = r['rs_label']
        mask = r['mask']
        try:
          seq, alleles = extracted_seqs[label]
        except KeyError:
          rs_label = extended_mask = 'None'
          status = Status.NO_INFO
        else:
          extended_mask = build_mask(seq, alleles)
          key = build_index_key(seq)
          tags = index.get(key, [])
          n_matches = len(tags)
          if n_matches != 1:
            logger.warning("%r maps to %d tags: %r" % (label, n_matches, tags))
            rs_label = 'None'
            status = Status.NO_MATCH if n_matches == 0 else Status.MULTI_MATCH
          else:
            rs_label, _, _, _ = serializer.deserialize(tags[0])
            if old_rs_label == "None":
              status = Status.ADDED
            else:
              status = (Status.CONFIRMED if rs_label == old_rs_label
                        else Status.REPLACED)
        outf.write("%s\n" % "\t".join((label, rs_label, mask, r['allele_a'],
                                       r['allele_b'], status, extended_mask)))
      logger.info("processed %d records overall" % (i+1))
  finally:
    if index:
      index.close()


def make_parser(parser):
  parser.add_argument("-i", "--input-file", metavar="FILE", required=True,
                      help="input file (segment extractor output)")
  parser.add_argument('-O', '--orig-file', metavar='FILE', required=True,
                      help='original VL marker definitions file')
  parser.add_argument("-o", '--output-file', metavar='FILE', required=True,
                      help='output reannotated VL marker definitions file')
  parser.add_argument("--index-file", metavar="FILE", required=True,
                      help="dbSNP index file")


def main(logger, args):
  write_output(logger, args)


def do_register(registration_list):
  registration_list.append(('lookup_index', HELP_DOC, make_parser, main))
