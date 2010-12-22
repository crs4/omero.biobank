"""
Import dbSNP dump into an omeroTable
"""

import logging
LOG_FILENAME = "dbSNP_to_omeroTables.log"
logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)
import sys, os, optparse

import omero
import omero_Tables_ice
import omero_SharedResources_ice

import vl.lib.utils as vl_utils

def make_parser():
    parser = optparse.OptionParser(usage='%prog [OPTIONS] DBSNP_DUMP_FILE OME_TABLE_FILENAME')
    parser.set_description(__doc__.lstrip())
    parser.add_option('--hostname', type='str', metavar='STRING',
                      help='omero server hostname [%default]',
                      default='localhost')
    parser.add_option('--user', type='str', metavar='STRING',
                      help='omero server user name [%default]',
                      default='root')
    parser.add_option('--password', type='str', metavar='STRING',
                      help='omero server password [%default]',
                      default='omero')
    return parser


def main(argv):
    logger = logging.getLogger('main')
    
    parser = make_parser()
    opt, args = parser.parse_args()
    try:
        dbsnp_dump = args[0]
        ome_table_fn = args[1]
    except IndexError:
        parser.print_help()
        sys.exit(2)
    

if __name__ == '__main__':
    main(sys.argv)
