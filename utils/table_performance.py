from __future__ import division
import os, argparse, uuid, time

import omero
import omero.model  # had to add this to prevent an error ---simleo
# pylint: disable=W0611
import omero_Tables_ice
import omero_SharedResources_ice
# pylint: enable=W0611
import omero.scripts as scripts

import numpy as np


TABLE_NAME = 'ometable_test.h5'
VID_SIZE = 34

OME_HOST = os.getenv('OME_HOST', 'localhost')
OME_USER = os.getenv('OME_USER', 'root')
OME_PASSWD = os.getenv('OME_PASSWD', 'romeo')


#-- helper functions --
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


def get_call_rate(session, threshold=0.05):
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
    s = sum(sum(x <= threshold for x in row) for row in col.values)
    call_rate = s / (nrows * col.size)
    table.close()
    print "call rate computed in %.3f s" % (time.time() - start)
    return call_rate


def get_call_rate_pytables(session, threshold=0.05):
    import tables
    ofile = get_original_file(session, TABLE_NAME)
    path = get_table_path(session, ofile)
    print "reading from %r" % (path,)
    start = time.time()
    with tables.openFile(path) as f:
        table = f.root.OME.Measurements
        call_rate = sum((row["confidence"] <= threshold).mean()
                        for row in table.iterrows()) / table.nrows
    print "data read & call rate computed in %.3f s" % (time.time() - start)
    return call_rate
#----------------------


#-- sub-commands --
def create_table(session, nrows, ncols):
    # pylint: disable=E1101
    vids = omero.grid.StringColumn('vid', 'GDO VID', VID_SIZE)
    op_vids = omero.grid.StringColumn('op_vid', 'Last op VID', VID_SIZE)
    probs = omero.grid.FloatArrayColumn('probs', 'Probs', 2*ncols)
    confs = omero.grid.FloatArrayColumn('confidence', 'Confs', ncols)
    # pylint: enable=E1101
    print "creating %d by %d table" % (nrows, ncols)
    start = time.time()
    r = session.sharedResources()
    m = r.repositories()
    i = m.descriptions[0].id.val
    table = r.newTable(i, TABLE_NAME)
    table.initialize([vids, op_vids, probs, confs])
    #--
    for col in vids, op_vids:
        col.values = [make_a_vid() for _ in xrange(nrows)]
    probs.values = [np.random.random(2*ncols).astype(np.float32)
                    for _ in xrange(nrows)]
    confs.values = [np.random.random(ncols).astype(np.float32)
                    for _ in xrange(nrows)]
    table.addData([vids, op_vids, probs, confs])
    table.close()
    print "table created in %.3f s" % (time.time() - start)


def run_test(session, pytables=False):
    if pytables:
        r = get_call_rate_pytables(session)
    else:
        r = get_call_rate(session)
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


#-- server-side execution --
def get_script_text(path, func):
    code = []
    #--
    with open(path) as f:
        for l in f:
            if l.startswith("def get_script_text"):
                break
            code.append(l.rstrip())
    #--
    code.append(
        'client = scripts.client("table_performance.py", "table performance",'
        )
    if func is create_table:
        code.extend([
            '  scripts.Long("nrows"), scripts.Long("ncols"))',
            'create_table(client.getSession(),'
            '  client.getInput("nrows").val,',
            '  client.getInput("ncols").val)',
            ])
    elif func is run_test:
        code.extend([
            '  scripts.Double("callrate").out())',
            'r = run_test(client.getSession())',  # FIXME: add pytables switch
            'client.setOutput("callrate", omero.rtypes.rdouble(r))',
            ])
    else:
        code.extend([
            ')',
            'drop_table(client.getSession())',
            ])
    #--
    return "\n".join(code)


def upload_and_run(client, args, wait_secs=3, block_secs=1):
    session = client.getSession()
    script_path = os.path.abspath(__file__)
    script_text = get_script_text(script_path, args.func)
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
    if args.func == create_table:
        remote_args = ['nrows=%s' % args.nrows, 'ncols=%s' % args.ncols]
    elif args.func == run_test:
        remote_args = []  # FIXME: add the pytables switch
    else:
        remote_args = []
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
        r = rv['callrate'].val
    except KeyError:
        print "no result from remote script"
        r = None
    # FIXME: return stdout/err
    return r
#-- server-side execution --


def build_parser():
    parser = argparse.ArgumentParser(description="test tables")
    parser.add_argument("--server", action="store_true", help="run on server")
    parser.add_argument("--pytables", action="store_true",
                        help="use pytables directly (forces --server to True)")
    subparsers = parser.add_subparsers()
    #--
    p = subparsers.add_parser('create', help="create table")
    p.add_argument('-r', '--nrows', type=int, metavar="INT", default=100,
                   help="number of rows")
    p.add_argument('-c', '--ncols', type=int, metavar="INT", default=10000,
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
            r = run_test(session, pytables=args.pytables)
    if r is not None:
        print "call rate: %f" % r
    client.closeSession()


if __name__ == '__main__':
    main()
