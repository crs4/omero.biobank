# BEGIN_COPYRIGHT
# END_COPYRIGHT


"""
Create a sqlite db from marker set importer files

"""

# FIXME: the code below is a fast hack with no error catching and,
# probably, an extremely inefficient use of sqlite.

import os, sys, csv
import logging, time
import sqlite3

from bl.core.utils import NullLogger

HELP_DOC = __doc__

DEFAULT_DB_NAME='rs.db'

def create_table(logger, cursor):
    sql_cmd = """
    CREATE TABLE rs_table(
                    label  TEXT,
                    mask   TEXT,
                    permutation INTEGER,
                    UNIQUE (label) ON CONFLICT IGNORE
                   );
    """
    logger.debug('{}'.format(sql_cmd))
    cursor.execute(sql_cmd)

def create_temp_table(logger, cursor):
    sql_cmd = """
    CREATE TABLE temp(
                    label  TEXT, UNIQUE (label) ON CONFLICT ABORT
                   );
    """
    logger.debug('{}'.format(sql_cmd))
    cursor.execute(sql_cmd)

def load_temp_table(logger, cursor, join_file):
    logger.info('loading join table from {}'.format(join_file))
    with open(join_file) as f:
        reader = csv.DictReader(f, delimiter="\t")
        for r in reader:
            sql_stat = 'INSERT INTO temp VALUES ("{}")'.format(r['label'])
            cursor.execute(sql_stat)

def dump_join_data(logger, cursor):
    logger.info('dumping join results')
    sql_stat = """
    SELECT rs_table.label, rs_table.mask, rs_table.permutation FROM rs_table
           INNER JOIN temp  ON rs_table.label = temp.label;
    """
    cursor.execute(sql_stat)
    logger.debug(sql_stat)    
    res = cursor.fetchall()
    writer = csv.DictWriter(sys.stdout,
                            fieldnames=['label', 'mask', 'index',
                                        'permutation'],
                            delimiter='\t')
    writer.writeheader()
    for i, r in enumerate(res):
        rec = {'label': r[0], 'mask': r[1], 'index': str(i),
               'permutation': r[2]}
        writer.writerow(rec)
    
def initialize_db(logger, db_name):
    logger.info('initializing db file {}'.format(db_name))
    conn = sqlite3.connect(db_name)
    create_table(logger, conn.cursor())
    conn.commit()
    conn.close()
    logger.info('done initializing db file')

def load_file(logger, db_name, fname):
    logger.info('ingesting data from {}'.format(fname))
    conn = sqlite3.connect(db_name)
    with open(fname) as f:
        c = conn.cursor()        
        reader = csv.DictReader(f, delimiter="\t")
        for r in reader:
            sql_stat = "INSERT INTO rs_table VALUES {}".format(
                str(tuple([r['label'], r['mask'], r['permutation']])))
            c.execute(sql_stat)
        conn.commit()
    conn.close()
    logger.info('done ingesting data from {}'.format(fname))

def drop_temp_table(logger, cursor):
    try:
        sql_stat = "DROP TABLE temp;"
        cursor.execute(sql_stat)
    except sqlite3.OperationalError as e:
        logger.debug('drop_table-> %s' % e)

def dump_join(logger, db_name, join_file):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    drop_temp_table(logger, cursor)    
    create_temp_table(logger, cursor)
    load_temp_table(logger, cursor, join_file)
    dump_join_data(logger, cursor)
    drop_temp_table(logger, cursor)
    conn.close()
    
def make_parser(parser):
    parser.add_argument("--db-file", type=str, default=DEFAULT_DB_NAME,
                        help="name of the db file that should be used")
    parser.add_argument("--reset", action='store_true', help="reset db")
    parser.add_argument('--files', nargs='*')
    parser.add_argument('--join', type=str,
                        help='tsv file with a label column')

def main(logger, args):
    db_name = args.db_file
    if args.reset and os.path.exists(db_name):
        os.unlink(db_name)
    if not os.path.exists(db_name):
        initialize_db(logger, db_name)
    if args.files:
        logger.info("processing %d files" % len(args.files))
        for fname in args.files:
            logger.info("processing %s" % fname)        
            load_file(logger, db_name, fname)
        logger.info("all loaded")
    if args.join:
        dump_join(logger, db_name, args.join)

def do_register(registration_list):
  registration_list.append(('manage_db', HELP_DOC, make_parser, main))
