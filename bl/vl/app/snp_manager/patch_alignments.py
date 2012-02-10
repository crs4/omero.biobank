# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
Patch VL marker alignments file, adding dummy lines for unaligned SNPs.
"""

import os, csv
from common import MARKER_AL_FIELDS, DUMMY_AL_VALUES


HELP_DOC = __doc__


def get_al_records(align_in_file):
  al_records = {}
  with open(align_in_file) as f:
    reader = csv.DictReader(f, delimiter='\t')
    for r in reader:
      al_records.setdefault(r['marker_vid'], []).append(r)
  return al_records


def make_parser(parser):
  parser.add_argument('-a', '--align-in-file', metavar='FILE', required=True,
                      help='input marker alignments file')
  parser.add_argument('-d', '--def-file', metavar='FILE', required=True,
                      help='input marker definitions file')
  parser.add_argument('-o', '--align-out-file', metavar='FILE', required=True,
                      help='output marker alignments file')
  parser.add_argument('--reftag', metavar='STRING', required=True,
                      help='reference genome tag')


def main(logger, args):
  logger.info("reading alignment records")
  al_records = get_al_records(args.align_in_file)
  logger.info("patching alignment file")
  with open(args.def_file) as f, open(args.align_out_file, 'w') as outf:
    reader = csv.DictReader(f, delimiter='\t')
    writer = csv.DictWriter(outf, MARKER_AL_FIELDS, delimiter='\t',
                            lineterminator=os.linesep)
    writer.writeheader()
    _tag = args.reftag
    _chr = DUMMY_AL_VALUES["chromosome"]
    _pos = DUMMY_AL_VALUES["pos"]
    _strand = DUMMY_AL_VALUES["strand"]
    _allele = DUMMY_AL_VALUES["allele"]
    for r in reader:
      out_records = al_records.get(r['label'], [dict(zip(
        MARKER_AL_FIELDS, [r['label'], _tag, _chr, _pos, _strand, _allele, '0']
        ))])
      for out_r in out_records:
        writer.writerow(out_r)
    logger.info("wrote %s" % outf.name)


def do_register(registration_list):
  registration_list.append(('patch_alignments', HELP_DOC, make_parser, main))
