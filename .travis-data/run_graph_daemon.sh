#!/bin/bash

if [[ "$GRAPH_ENGINE" == "neo4j" ]]; then
  python ./daemons/graph_manager.py --logfile /tmp/graph_manager.log &
fi
