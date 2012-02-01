# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
Extract marker data from KB
===========================
"""

import os, csv, logging
import itertools as it

from bl.vl.app.importer.core import Core


class Markers(Core):

  SUPPORTED_FIELDS_SETS = ['alignment', 'mapping', 'definition']

  @classmethod
  def to_tuple(klass, s):
    if s:
      return tuple([x.strip() for x in s.strip()[1:-1].split(',')])
    return s

  def __init__(self, host=None, user=None, passwd=None, keep_tokens=1,
               definition_source=None, markers_set=None,
               operator='Alfred E. Neumann'):
    self.logger = logging.getLogger()
    super(Markers, self).__init__(host, user, passwd, keep_tokens=keep_tokens)
    self.definition_source = self.to_tuple(definition_source)
    self.markers_set = self.to_tuple(markers_set)

  def preload(self, load_alignment=False):
    if not self.markers_set and self.definition_source:
      return self.preload_from_source(load_alignment)
    else:
      return self.preload_from_marker_set(load_alignment)

  def preload_from_source(self, load_alignment):
    self.logger.info('start preloading markers from source')
    source, context, release = self.definition_source
    selector = ('(source=="%s") & (context=="%s") & (release=="%s")' %
                self.definition_source)
    mrk_defs = self.kb.get_snp_marker_definitions(selector=selector)
    self.logger.info('done preloading related markers')
    if load_alignment:
      selector = '|'.join(["(marker_vid=='%s')" % k for k in mrk_vids])
      snp_algns = self.kb.get_snp_alignments(selector=selector)
    else:
      snp_algns = False
    mset = None
    mrks = None
    return mset, mrks, mrk_defs, snp_algns

  def preload_from_marker_set(self, load_alignment):
    self.logger.info('start preloading markers from markers set')
    maker, model, release = self.markers_set
    mset = self.kb.get_snp_markers_set(maker, model, release)
    if not mset:
      raise ValueError('unknown markers_set')
    mrks = self.kb.get_snp_markers_set_content(mset)
    mrk_vids = mrks['marker_vid']
    selector = '|'.join(["(vid=='%s')" % k for k in mrk_vids])
    mrk_defs = self.kb.get_snp_marker_definitions(selector=selector)
    assert len(mrk_defs) == len(mrk_vids)
    self.logger.info('done preloading related markers')
    if load_alignment:
      selector = '|'.join(["(marker_vid=='%s')" % k for k in mrk_vids])
      snp_algns = self.kb.get_snp_alignments(selector=selector)
    else:
      snp_algns = False
    return mset, mrks, mrk_defs, snp_algns

  def dump_alignment(self, ofile):
    mset, mrks, mrk_defs, snp_algns = self.preload(True)
    vid_to_algns = {}
    for x in snp_algns:
      # this is a numpy.void object, handle with care
      vid_to_algns[x['marker_vid']] = x
    vid_to_def = dict([x for x in it.izip(mrk_defs['vid'],
                                          it.izip(mrk_defs['rs_label'],
                                                  mrk_defs['label']))])
    fieldnames = 'marker_vid rs_label ref_genome chromosome pos'.split()
    fieldnames = fieldnames + 'global_pos strand allele copies'.split()
    tsv = csv.DictWriter(ofile, fieldnames, delimiter='\t',
                         lineterminator=os.linesep)
    tsv.writeheader()
    for m in mrks:
      vid = m['marker_vid']
      rec = vid_to_algns[vid]
      r = {
        'marker_vid': vid,
        'rs_label': vid_to_def[vid][0],
        'ref_genome': rec[1],
        'chromosome': rec[2],
        'pos': rec[3],
        'global_pos': rec[4],
        'strand': rec[5],
        'allele': rec[6],
        'copies': rec[7],
        }
      tsv.writerow(r)

  def dump_definition(self, ofile):
    mset, mrks, mrk_defs, _ = self.preload()
    vid_to_def = dict([x for x in it.izip(mrk_defs['vid'],
                                          it.izip(mrk_defs['mask'],
                                                  mrk_defs['rs_label']))])
    fieldnames = 'marker_vid rs_label mask'.split()
    tsv = csv.DictWriter(ofile, fieldnames, delimiter='\t',
                         lineterminator=os.linesep)
    tsv.writeheader()
    for m in mrks:
      vid = m['marker_vid']
      mdef = vid_to_def[vid]
      r = {
        'marker_vid': vid,
        'mask': mdef[0],
        'rs_label': mdef[1],
        }
      tsv.writerow(r)

  def dump_mapping(self, ofile):
    _, _, mrk_defs, _ = self.preload()
    fieldnames = 'vid mask'.split()
    tsv = csv.DictWriter(ofile, fieldnames, delimiter='\t')
    tsv.writeheader()
    for vid, mask in it.izip(mrk_defs['vid'], mrk_defs['mask']):
      r = {
        'vid': vid,
        'mask': mask,
        }
      tsv.writerow(r)

  def count_markers(self, ms):
    return len(self.kb.get_snp_markers_set_content(ms))

  def dump_markers_sets(self, ofile):
    mss = self.kb.get_objects(self.kb.SNPMarkersSet)
    fieldnames = 'maker model release set_vid size'.split()
    tsv = csv.DictWriter(ofile, fieldnames, delimiter='\t')
    tsv.writeheader()
    for ms in mss:
      r = {
        'maker': ms.maker,
        'model': ms.model,
        'release': ms.release,
        'set_vid': ms.markersSetVID,
        'size': self.count_markers(ms),
        }
      tsv.writerow(r)

  def dump(self, fields_set, ofile):
    if not (self.markers_set or self.definition_source):
      return self.dump_markers_sets(ofile)
    assert fields_set in self.SUPPORTED_FIELDS_SETS
    if fields_set == 'definition':
      return self.dump_definition(ofile)
    elif fields_set == 'mapping':
      return self.dump_mapping(ofile)
    elif fields_set == 'alignment':
      return self.dump_alignment(ofile)


help_doc = """
Extract marker-related info from the KB
"""


def make_parser(parser):
  parser.add_argument('--definition-source', metavar="STRING",
                      help="a (source,context,release) tuple")
  parser.add_argument('--markers-set', metavar="STRING",
                      help="a (maker,model,release) tuple")
  parser.add_argument('--fields-set', metavar="STRING",
                      choices=Markers.SUPPORTED_FIELDS_SETS,
                      help="choose all the fields listed in this set")


def implementation(args):
  markers = Markers(host=args.host, user=args.user, passwd=args.passwd,
                    keep_tokens=args.keep_tokens,
                    definition_source=args.definition_source,
                    markers_set=args.markers_set)
  markers.dump(args.fields_set, args.ofile)


def do_register(registration_list):
  registration_list.append(('markers', help_doc, make_parser, implementation))
