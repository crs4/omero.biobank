import sys, shelve

try:
  db_fn = sys.argv[1]
except IndexError:
  sys.exit("Usage: %s DB_FILE" % sys.argv[0])

db = None
try:
  db = shelve.open(db_fn, 'r')
  with open(db_fn+".dump", "w") as outf:
    for k, v in db.iteritems():
      outf.write("%s\t%r\n" % (k, v))
    print "wrote %r" % outf.name
finally:
  if db:
    db.close()
