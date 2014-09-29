#!/bin/bash

# This script can be used to handle the galaxy_menus service as a cronjob
# add to the crontab a line like
#
# */5 * * * * $BIOBANK_REPO_PATH/services/galaxy_menus_service_cron.sh &

SERVICE_DIR=/opt/biobank/services
PID_FILE=/tmp/biobank_galaxy_menus_service.pid
HOST=0.0.0.0
PORT=8080

nohup python $SERVICE_DIR/galaxy_menus.py --pid-file $PID_FILE --host $HOST --port $PORT
