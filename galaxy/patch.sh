#!/bin/bash
#
# usage: bash patch.sh GALAXY_DIR

GALAXY_DIR=$1

#-----------------------------------------------------------
FILES="run_vl.sh vl_universe_wsgi.ini"

CP=cp
PATCH=patch
DIFF=diff

${DIFF} -C 3 tool_conf.xml.sample tool_conf.xml > tool_conf.patch

${CP} -a tools ${GALAXY_DIR}
${CP} -a lib ${GALAXY_DIR}
${PATCH} ${GALAXY_DIR}/tool_conf.xml tool_conf.patch

for f in ${FILES}
do
    ${CP} $f ${GALAXY_DIR}
done


