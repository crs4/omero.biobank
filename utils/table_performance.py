"""
Test table performance.
"""

from __future__ import division
import sys, os, argparse, uuid, time, socket

import omero
import omero.model  # had to add this to prevent an error ---simleo
# pylint: disable=W0611
import omero_Tables_ice
import omero_SharedResources_ice
# pylint: enable=W0611
import omero.scripts as scripts

import numpy as np


TABLE_NAME = 'ometable_test.h5'
ROWS, COLS = 100, 10000
ROWS_PER_CHUNK = 10

VID_SIZE = 34

OME_HOST = os.getenv('OME_HOST', socket.gethostname())
OME_USER = os.getenv('OME_USER', 'root')
OME_PASSWD = os.getenv('OME_PASSWD', 'romeo')


#-- helper functions & classes --
def make_a_vid():
    return "V0%s" % uuid.uuid4().hex.upper()


def get_original_file(session, name):
    qs = session.getQueryService()
    ofile = qs.findByString('OriginalFile', 'name', name, None)
    if ofile is None:
        raise ValueError("no table found with name %r" % (TABLE_NAME,))
    return ofile


def get_table_path(session, ofile):
    config = session.getConfigService()
    d = config.getConfigValue("omero.data.dir")
    return os.path.join(d, "Files", "%d" % ofile.id.val)


def open_table(session):
    ofile = get_original_file(session, TABLE_NAME)
    r = session.sharedResources()
    t = r.openTable(ofile)
    return t


def get_call_rates(session, threshold=0.05):
    table = open_table(session)
    nrows = table.getNumberOfRows()
    col_headers = [h.name for h in table.getHeaders()]
    conf_index = col_headers.index("confidence")
    print "reading from table"
    start = time.time()
    data = table.read([conf_index], 0, nrows)
    print "confidence data read in %.3f s" % (time.time() - start)
    start = time.time()
    col = data.columns[0]
    r = [sum(x <= threshold for x in row) / col.size for row in col.values]
    table.close()
    print "call rates computed in %.3f s" % (time.time() - start)
    return r


def get_call_rates_pytables(session, threshold=0.05):
    import tables
    ofile = get_original_file(session, TABLE_NAME)
    path = get_table_path(session, ofile)
    print "reading from %r" % (path,)
    start = time.time()
    with tables.openFile(path) as f:
        table = f.root.OME.Measurements
        call_rates = [(row["confidence"] <= threshold).mean()
                      for row in table.iterrows()]
    print "data read & call rates computed in %.3f s" % (time.time() - start)
    return call_rates


class TableManager(object):

    def __init__(self, session, nrows, ncols):
        # pylint: disable=E1101
        self.vids = omero.grid.StringColumn('vid', 'GDO VID', VID_SIZE)
        self.op_vids = omero.grid.StringColumn('op_vid', 'op VID', VID_SIZE)
        self.probs = omero.grid.FloatArrayColumn('probs', 'probs', 2*ncols)
        self.confs = omero.grid.FloatArrayColumn('confidence', 'confs', ncols)
        # pylint: enable=E1101
        self.columns = [self.vids, self.op_vids, self.probs, self.confs]
        self.table = None
        self.session, self.nrows, self.ncols = session, nrows, ncols

    def init_table(self):
        start = time.time()
        r = self.session.sharedResources()
        m = r.repositories()
        i = m.descriptions[0].id.val
        self.table = r.newTable(i, TABLE_NAME)
        self.table.initialize(self.columns)
        print "table initialized in %.3f s" % (time.time() - start)

    def __chunk_sizes(self, rows_per_chunk):
        n_chunks, rem = divmod(self.nrows, rows_per_chunk)
        for _ in xrange(n_chunks):
            yield rows_per_chunk
        if rem:
            yield rem

    def __populate_table(self, rows_per_chunk=ROWS_PER_CHUNK):
        for cs in self.__chunk_sizes(rows_per_chunk):
            for col in self.vids, self.op_vids:
                col.values = [make_a_vid() for _ in xrange(cs)]
            self.probs.values = [
                np.random.random(2*self.ncols).astype(np.float32)
                for _ in xrange(cs)
                ]
            self.confs.values = [
                np.random.random(self.ncols).astype(np.float32)
                for _ in xrange(cs)
                ]
            self.table.addData(self.columns)
        self.table.close()

    def __generate_rows(self, n):
        return [(
            make_a_vid(), make_a_vid(),
            np.random.random(2*self.ncols).astype(np.float32),
            np.random.random(self.ncols).astype(np.float32),
            ) for _ in xrange(n)]

    def __populate_table_pytables(self, rows_per_chunk=ROWS_PER_CHUNK):
        import tables
        self.table.close()
        path = get_table_path(self.session, self.table.getOriginalFile())
        with tables.openFile(path, "r+") as f:
            t = f.root.OME.Measurements
            for cs in self.__chunk_sizes(rows_per_chunk):
                t.append(self.__generate_rows(cs))

    def populate_table(self, pytables=False):
        start = time.time()
        if pytables:
            self.__populate_table_pytables()
        else:
            self.__populate_table()
        print "table populated in %.3f s" % (time.time() - start)
#----------------------


#-- sub-commands --
def create_table(session, nrows, ncols, pytables=False):
    print "creating %d by %d table" % (nrows, ncols)
    tm = TableManager(session, nrows, ncols)
    tm.init_table()
    tm.populate_table(pytables=pytables)


def run_test(session, pytables=False):
    if pytables:
        r = get_call_rates_pytables(session)
    else:
        r = get_call_rates(session)
    return r


def drop_table(session):
    print "cleaning up the mess"
    qs = session.getQueryService()
    ofiles = qs.findAllByString(
        'OriginalFile', 'name', TABLE_NAME, True, None
        )  # *all* tables with that name, make sure we get a clean state
    us = session.getUpdateService()
    for of in ofiles:
        print "  deleting original file #%d" % of.id.val
        us.deleteObject(of)
#------------------


def upload_and_run(client, args, wait_secs=3, block_secs=1):
    session = client.getSession()
    script_path = os.path.abspath(__file__)
    with open(script_path) as f:
        script_text = f.read()
    svc = session.getScriptService()
    script_id = svc.getScriptID(script_path)
    if script_id < 0:
        script_id = svc.uploadOfficialScript(script_path, script_text)
        print "uploaded script as original file #%s" % script_id
    else:
        of = session.getQueryService().get("OriginalFile", script_id)
        svc.editScript(of, script_text)
        print "replaced contents of original file #%s" % script_id
    params = svc.getParams(script_id)
    remote_args = ['command=%s' % args.func.__name__]
    if args.func == create_table:
        remote_args.extend([
            'nrows=%s' % args.nrows,
            'ncols=%s' % args.ncols,
            'pytables=%s' % args.pytables,
            ])
    elif args.func == run_test:
        remote_args.extend(['pytables=%s' % args.pytables])
    m = scripts.parse_inputs(remote_args, params)
    try:
        proc = svc.runScript(script_id, m, None)
    except omero.ValidationException as e:
        print 'bad parameters: %s' % e
        return
    cb = scripts.ProcessCallbackI(client, proc)
    try:
        while proc.poll() is None:
            cb.block(1000*block_secs)
        rv = proc.getResults(wait_secs)
    finally:
        cb.close()
    try:
        r = omero.rtypes.unwrap(rv['callrate'])
    except KeyError:
        r = None
    for stream_name in "stdout", "stderr":
        f = rv.get(stream_name)
        if f and f.val:
            print "\nremote script - %s:" % stream_name
            stream = getattr(sys, stream_name)
            client.download(ofile=f.val, filehandle=stream)
    return r


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__.strip())
    parser.add_argument("--server", action="store_true", help="run on server")
    parser.add_argument("--pytables", action="store_true",
                        help="use pytables directly (forces --server to True)")
    subparsers = parser.add_subparsers()
    #--
    p = subparsers.add_parser('create', help="create table")
    p.add_argument('-r', '--nrows', type=int, metavar="INT", default=ROWS,
                   help="number of rows")
    p.add_argument('-c', '--ncols', type=int, metavar="INT", default=COLS,
                   help="number of columns")
    p.set_defaults(func=create_table)
    #--
    p = subparsers.add_parser('run', help="run test")
    p.set_defaults(func=run_test)
    #--
    p = subparsers.add_parser('clean', help="remove table(s)")
    p.set_defaults(func=drop_table)
    #--
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    print 'connecting to %s' % OME_HOST
    client = omero.client(OME_HOST)
    session = client.createSession(OME_USER, OME_PASSWD)
    r = None
    if args.pytables:
        args.server = True
    if args.server:
        r = upload_and_run(client, args)
    else:
        if args.func == create_table:
            create_table(session, args.nrows, args.ncols)
        elif args.func == drop_table:
            drop_table(session)
        else:
            r = run_test(session, pytables=False)
    print
    if r is not None:
        with open("call_rates.txt", "w") as fo:
            fo.write("\n".join("%.3f" % _ for _ in r)+"\n")
        print "call rates dumped to", fo.name
    else:
        print "no result from remote script"
    client.closeSession()


def remote_main():
    client = scripts.client(
        __file__, __doc__.strip(),
        scripts.String("command", optional=False),
        scripts.Long("nrows", optional=True, default=ROWS),
        scripts.Long("ncols", optional=True, default=COLS),
        scripts.Bool("pytables", optional=True, default=False),
        scripts.List("callrate").ofType(omero.rtypes.rdouble(0)).out(),
        )
    command = client.getInput("command").val
    if command == "create_table":
        create_table(
            client.getSession(),
            client.getInput("nrows").val,
            client.getInput("ncols").val,
            pytables=client.getInput("pytables").val,
            )
    elif command == "run_test":
        r = run_test(
            client.getSession(),
            pytables=client.getInput("pytables").val,
            )
        client.setOutput("callrate", omero.rtypes.wrap(r))
    elif command == "drop_table":
        drop_table(client.getSession())


if __name__ == '__main__':
    if socket.gethostname() == OME_HOST:
        remote_main()
    else:
        main()
