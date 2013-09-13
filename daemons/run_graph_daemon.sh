#!/bin/bash

LOG_FILE_PATH="./graph_daemon.log"
LOG_LEVEL="INFO"
PID_FILE_PATH="./graph_daemon.pid"

if [ ! -f $PID_FILE_PATH ] ; then
  touch $PID_FILE_PATH
  chmod 700 $PID_FILE_PATH
  python ./graph_manager.py --loglevel $LOG_LEVEL --logfile $LOG_FILE_PATH
  rm -f $PID_FILE_PATH
fi
