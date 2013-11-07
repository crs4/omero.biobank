#!/bin/bash

# This script can be used to handle the graph_manager daemon as a cronjob
# add to the crontab a line like
#
# */5 * * * * $BIOBANK_REPO_PATH/daemons/cron_manager.sh &
#
# values for DAEMON_DIR and OME_HOME are set as an example, please remember
# to change them to your actual paths

DAEMON_DIR=/opt/biobank/daemons
LOGS_DIR=$DAEMON_DIR/logs
PID_FILE=/tmp/graph_manager.pid
OME_HOME=/opt/omero

export PYTHONPATH=$OME_HOME/dist/lib/python/:$PYTHONPATH

nohup python $DAEMON_DIR/graph_manager.py --logfile $LOGS_DIR/graph_manager.log --pid-file $PID_FILE
