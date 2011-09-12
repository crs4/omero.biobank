#!/bin/bash

SNP_REANNOTATOR="../../../tools/snp_reannotator --loglevel DEBUG"

if [ "$1" == "--clean" ]; then
    rm -fv *.log *marker_definitions.tsv
    exit 0
fi

${SNP_REANNOTATOR} --logfile convert_dbsnp.log convert_dbsnp -d ./ -o dbsnp_marker_definitions.tsv

${SNP_REANNOTATOR} --logfile convert_affy.log convert_affy -i affyTest.csv -o affy_marker_definitions.tsv
