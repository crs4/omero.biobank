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

DBSNP_TABLE = 'snp131'
GENOME_BUILD = 'hg19'

def make_table_columns():
    logger  = logging.getLogger('make_table_columns')
    logger.debug('Creating table columns')
    vid_col = omero.grid.StringColumn('vid', 'Row VID', len(vl_utils.make_vid()), None)
    table_col = omero.grid.StringColumn('dbSNP_table', 'Referenced table in dbSNP',
                                        len(DBSNP_TABLE), None)
    gene_build_col = omero.grid.StringColumn('gene_build', 'Genome build', len(GENOME_BUILD), None)
    bin_col = omero.grid.LongColumn('bin', 'Indexing field to speed chromosome range queries', None)
    chrom_col = omero.grid.StringColumn('chrom', 'Reference sequence chromosome or scaffold', 31, None)
    chrom_start_col = omero.grid.LongColumn('chrom_start', 'Start position in chrom', None)
    chrom_end_col = omero.grid.LongColumn('chrom_end', 'End position in chrom', None)
    name_col = omero.grid.StringColumn('name', 'dbSNP Reference SNP identifier', 15, None)
    return [vid_col, table_col, gene_build_col, bin_col,
            chrom_col, chrom_start_col, chrom_end_col, name_col]

def initialize_table(file_name, session):
    logger = logging.getLogger('initialize_table')
    logger.debug('Initializing table with file name %s' % file_name)
    r = session.sharedResources()
    m = r.repositories()
    i = m.descriptions[0].id.val
    table = r.newTable(i, file_name)
    cols = make_table_columns()
    table.initialize(cols)
    return table

def save_row(bin, chrom, chrom_start, chrom_end, name, table):
    logger = logging.getLogger('save_row')
    logger.debug('Saving data: %s - %s - %s - %s - %s' % (bin, chrom, chrom_start, chrom_end, name))
    cols = table.getHeaders()
    cols[0].values = [vl_utils.make_vid()]
    cols[1].values = [DBSNP_TABLE]
    cols[2].values = [GENOME_BUILD]
    cols[3].values = [int(bin)]
    cols[4].values = [chrom]
    cols[5].values = [int(chrom_start)]
    cols[6].values = [int(chrom_end)]
    cols[7].values = [name]
    table.addData(cols)

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
        dbsnp_dump_fn = args[0]
        ome_table_fn = args[1]
    except IndexError:
        parser.print_help()
        sys.exit(2)

    client = omero.client(opt.hostname)
    session = client.createSession(opt.user, opt.password)
    
    ome_table = initialize_table(ome_table_fn, session)

    dbsnp_dump = open(dbsnp_dump_fn)
    for line in dbsnp_dump:
        bin, chrom, chr_start, chr_end, name = line.split()[:5]
        save_row(bin, chrom, chr_start, chr_end, name, ome_table)
    
    client.closeSession()

if __name__ == '__main__':
    main(sys.argv)
