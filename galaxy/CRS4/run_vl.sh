#!/bin/sh

cd `dirname $0`
python ./scripts/paster.py serve vl_universe_wsgi.ini --pid-file=vl_webapp.pid --log-file=vl_webapp.log $@
