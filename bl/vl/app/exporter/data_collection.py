"""
Export DataCollection definition
================================

FIXME

"""

from bl.vl.sample.kb import KBError
from bl.vl.app.importer.core import Core, BadRecord
from version import version

import csv, json
import time, sys
import itertools as it


class Dumper(Core):
  """
  An utility class that handles the actual dumping of marker definitions
  into VL.
  """
  def __init__(self, host=None, user=None, passwd=None, keep_tokens=1,
               operator='Alfred E. Neumann'):
    """
    FIXME
    """
    self.logger = logger
    super(Dumper, self).__init__(host, user, passwd)


#-------------------------------------------------------------------------
help_doc = """
export data collection definition.  In default, it will only produce
the minimal output that could be used to import back the data collection.
"""

def make_parser_data_collection(parser):
  parser.add_argument('--label', type=str,
                      help="""data collection label""")

def import_data_collection_implementation(args):
  #--
  dumper = Dumper(host=args.host, user=args.user, passwd=args.passwd,
                keep_tokens=args.keep_tokens)
  dumper.dump(args.list_markers,
              args.maker, args.model, args.release, args.ofile)

def do_register(registration_list):
  registration_list.append(('data_collection', help_doc,
                            make_parser_data_collection,
                            import_data_collection_implementation))


