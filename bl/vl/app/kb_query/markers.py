"""
Extract in tabular form data from KB
====================================


 $ kb_query markers --definition-source (XXX,YYYY,ZZZZ) --marker-set=(Affymetrix,foo,hg28) --fields-set definition


"""

from bl.vl.sample.kb import KBError
from bl.vl.app.importer.core import Core, BadRecord
from version import version

from bl.vl.sample.kb.drivers.omero.dependency_tree import DependencyTree

import csv, json
import time, sys
import itertools as it

import logging

class Markers(Core):
  """
  An utility class that handles the dumping of KB marker related data
  in tabular form.
  """

  SUPPORTED_FIELDS_SETS    = ['alignment', 'mapping', 'definition']

  @classmethod
  def to_tuple(klass, s):
    if s:
      return tuple([x.strip() for x in s.strip()[1:-1].split(',')])
    return s

  def __init__(self, host=None, user=None, passwd=None, keep_tokens=1,
               definition_source=None,
               markers_set=None,
               operator='Alfred E. Neumann'):
    """
    FIXME
    """
    self.logger = logging.getLogger()
    super(Markers, self).__init__(host, user, passwd, keep_tokens=keep_tokens)
    self.definition_source = self.to_tuple(definition_source)
    self.markers_set = self.to_tuple(markers_set)

  def preload(self, load_aligment=False):
    if not self.markers_set:
      raise ValueError('markers-set should be provided')

    if self.definition_source:
      pass

    self.logger.info('start preloading related markers')
    #--
    maker, model, release = self.markers_set
    selector = ("(maker=='%s')&(model=='%s')&(release=='%s')" %
                (maker, model, release))
    #--
    mrk_set = self.gkb.get_snp_markers_sets(selector=selector)
    if not mrk_set:
      raise ValueError('unknown markers_set')
    #--
    mrk_set_vid = mrk_set['vid'][0]
    selector = "(vid=='%s')" % mrk_set_vid
    mrks = self.gkb.get_snp_markers_set(selector=selector)
    mrk_vids = mrks['marker_vid']
    #--
    selector = '|'.join(["(vid=='%s')" % k for k in mrk_vids])
    mrk_defs = self.gkb.get_snp_marker_definitions(selector=selector)
    assert len(mrk_defs) == len(mrk_vids)
    self.logger.info('done preloading related markers')
    #--
    if load_aligment:
      snp_algns = self.gkb.get_snp_alignments(selector=selector)
    else:
      snp_algns = False
    #--
    return mrk_set_vid, mrks, mrk_defs, snp_algns

  def dump_alignment(self, ofile):
    """
    dumps

       marker_vid rs_label ref_genome chromosome pos global_pos strand allele copies
    """

    mrk_set_vid, mrks, mrk_defs, snp_algns = self.preload(True)

    vid_to_algns = {}
    for x in snp_algns:
      vid_to_algns[x['marker_vid']] = x # this is a numpy.void object, handle with care
    #--
    vid_to_def = dict([ x for x in it.izip(mrk_defs['vid'],
                                           it.izip(mrk_defs['rs_label'],
                                                   mrk_defs['label']))])
    #--
    fieldnames = 'marker_vid rs_label ref_genome chromosome pos'.split()
    fieldnames = fieldnames + 'global_pos strand allele copies'.split()
    tsv = csv.DictWriter(ofile, fieldnames, delimiter='\t')
    tsv.writeheader()
    for m in mrks:
      vid = m['marker_vid']
      # marker_vid ref_genome chromosome pos global_pos strand allele copies op_vid
      rec = vid_to_algns[vid]
      r = {'marker_vid' : vid,
           'rs_label' : vid_to_def[vid],
           'ref_genome' : rec[1],
           'chromosome' : rec[2],
           'pos' : rec[3],
           'global_pos' : rec[4],
           'strand' : rec[5],
           'allele' : rec[6],
           'copies' : rec[7],
           }
      tsv.writerow(r)

  def dump_definition(self, ofile):
    """
    dumps

       marker_vid rs_label mask
    """
    mrk_set_vid, mrks, mrk_defs, _ = self.preload()

    vid_to_mask = dict([ x for x in it.izip(mrk_defs['vid'],
                                            it.izip(mrk_defs['mask'],
                                                    mrk_defs['rs_label']))])
    #--

    fieldnames = 'marker_vid rs_label mask'.split()
    tsv = csv.DictWriter(ofile, fieldnames, delimiter='\t')
    tsv.writeheader()
    for m in mrks:
      vid = m['marker_vid']
      mdef = vid_to_def[vid]
      r = {'marker_vid' : vid,
           'mask' : mdef[0],
           'rs_label' : mdef[1]
           }
      tsv.writerow(r)

  def dump_mapping(self, ofile):
    """
    dumps

      markers_set_vid label rs_label marker_indx allele_flip
    """
    mrk_set_vid, mrks, mrk_defs, _ = self.preload()
    vid_to_def = dict([ x for x in it.izip(mrk_defs['vid'],
                                           it.izip(mrk_defs['rs_label'],
                                                   mrk_defs['label']))])
    #--
    fieldnames = 'markers_set_vid label rs_label marker_indx allele_flip'.split()
    tsv = csv.DictWriter(ofile, fieldnames, delimiter='\t')
    tsv.writeheader()
    for m in mrks:
      mdef = vid_to_def[m['marker_vid']]
      r = {'markers_set_vid' : mrk_set_vid,
           'label' :    mdef[0],
           'rs_label' : mdef[1],
           'marker_indx' : m['marker_indx'],
           'allele_flip' : 'True' if m['allele_flip'] else 'False',
           }
      tsv.writerow(r)

  def count_markers(self, ms):
    selector = "(vid=='%s')" % ms.markersSetVID
    return len(self.gkb.get_snp_markers_set(selector=selector))

  def dump_markers_sets(self, ofile):
    mss = self.skb.get_objects(self.skb.SNPMarkersSet)
    fieldnames = 'vid maker model release set_vid size'.split()
    tsv = csv.DictWriter(ofile, fieldnames, delimiter='\t')
    tsv.writeheader()
    for ms in mss:
      r = {'vid' : ms.id,
           'maker' : ms.maker, 'model' : ms.model, 'release' : ms.release,
           'set_vid' : ms.markersSetVID, 'size' : self.count_markers(ms)}
      tsv.writerow(r)

  def dump(self, fields_set, ofile):
    if not self.markers_set:
      return self.dump_markers_sets(ofile)

    assert fields_set in self.SUPPORTED_FIELDS_SETS
    if fields_set == 'definition':
      return self.dump_definition(ofile)
    elif fields_set == 'mapping':
      return self.dump_mapping(ofile)
    elif fields_set == 'alignment':
      return self.dump_alignment(ofile)


#-------------------------------------------------------------------------
help_doc = """
Extract markers related info from the KB.
"""

def make_parser_markers(parser):
  parser.add_argument('--definition-source', type=str,
                      help="marker definition source, a tuple (source,context,release)")
  parser.add_argument('--markers-set', type=str,
                      help="a tuple (maker,model,release)")
  parser.add_argument('--fields-set', type=str,
                      choices=Markers.SUPPORTED_FIELDS_SETS,
                      help="""choose all the fields listed in this set""")

def import_markers_implementation(args):
  #--
  markers = Markers(host=args.host, user=args.user, passwd=args.passwd,
                    keep_tokens=args.keep_tokens,
                    definition_source=args.definition_source,
                    markers_set=args.markers_set)
  markers.dump(args.fields_set, args.ofile)

def do_register(registration_list):
  registration_list.append(('markers', help_doc,
                            make_parser_markers,
                            import_markers_implementation))


