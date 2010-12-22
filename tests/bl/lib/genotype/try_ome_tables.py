import omero
import omero_Tables_ice
import omero_SharedResources_ice

import vl.lib.utils as vl_utils

import time


def make_dummy_row():
    vid_col = omero.grid.StringColumn('vid','',len(vl_utils.make_vid()), None)
    vid_col.values = [vl_utils.make_vid()]
    table_col = omero.grid.StringColumn('table', '', 15, None)
    table_col.values = ['SNP131']
    gene_build_col = omero.grid.StringColumn('gene_build', '', 10, None)
    gene_build_col.values = ['FOO']
    bin_col = omero.grid.LongColumn('bin', '', None)
    bin_col.values = [12]
    chrom_col = omero.grid.StringColumn('chrom', '', 31, None)
    chrom_col.values = ['CHR22']
    chrom_start_col = omero.grid.LongColumn('chrom_start', '', None)
    chrom_start_col.values = [100]
    chrom_end_col = omero.grid.LongColumn('chrom_end', '', None)
    chrom_end_col.values = [120]
    name_col = omero.grid.StringColumn('name', '', 15, None)
    name_col.values = ['rs1000']
    return [vid_col, table_col, gene_build_col, bin_col, chrom_col,
            chrom_start_col, chrom_end_col, name_col]

def make_table(file_name):
    c = omero.client('localhost')
    s = c.createSession('root', 'omero')
    r = s.sharedResources()
    m = r.repositories()
    i = m.descriptions[0].id.val
    t = r.newTable(i, file_name)
    #Table columns
    vid_col = omero.grid.StringColumn('vid', 'Row VID', len(vl_utils.make_vid()), None)
    table_col = omero.grid.StringColumn('table', 'Referencing table from dbSNP', 15, None)
    gene_build_col = omero.grid.StringColumn('gene_build', 'Genome build', 10, None)
    bin_col = omero.grid.LongColumn('bin', 'Indexing field to speed chromosome range queries', None)
    chrom_col = omero.grid.StringColumn('chrom', 'Reference sequence chromosome or scaffold', 31, None)
    chrom_start_col = omero.grid.LongColumn('chrom_start', 'Start position in chrom', None)
    chrom_end_col = omero.grid.LongColumn('chrom_end', 'End position in chrom', None)
    name_col = omero.grid.StringColumn('name', 'dbSNP Reference SNP identifier', 15, None)
    t.initialize([vid_col, table_col, gene_build_col, bin_col, chrom_col,
                  chrom_start_col, chrom_end_col, name_col])
    print vid_col
    t.addData(make_dummy_row())
    t.addData(make_dummy_row())
    c.closeSession()

def read_table(file_name):
    c = omero.client('localhost')
    s = c.createSession('root', 'omero')
    qs = s.getQueryService()
    ofile = qs.findByString('OriginalFile', 'name', file_name, None)
    if ofile:
        r = s.sharedResources()
        t = r.openTable(ofile)
        print t
        print t.getNumberOfRows()
        data = t.read([0,1,2,3], 0, t.getNumberOfRows())
        #print data
        c.closeSession()
    else:
        print 'No table found'

# Right now, the only way to 'delete' an omeroTable is to remove the OriginalFile entry
# from the database, the file containing the table in not delete from the file system
def clear_table(file_name):
    c = omero.client('localhost')
    s = c.createSession('root', 'omero')
    qs = s.getQueryService()
    ofile = qs.findByString('OriginalFile', 'name', file_name, None)
    us = s.getUpdateService()
    us.deleteObject(ofile)
    c.closeSession()

fname = 'DummyTable.h5'
make_table(fname)
read_table(fname)
clear_table(fname)
read_table(fname)
