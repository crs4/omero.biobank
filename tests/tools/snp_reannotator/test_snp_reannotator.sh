#!/bin/bash

SNP_REANNOTATOR=../../../tools/snp_reannotator

if [ "$1" == "--clean" ]; then
    rm -fv *.log *marker_definitions.tsv
    exit 0
fi

${SNP_REANNOTATOR} --loglevel DEBUG --logfile convert_dbsnp.log convert_dbsnp -d ./ -o dbsnp_marker_definitions.tsv
