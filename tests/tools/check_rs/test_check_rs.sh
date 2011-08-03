#!/bin/bash

CHECK_RS=../../../tools/check_rs
IN_F=Affymetrix_GenomeWideSNP_6_na31_MT.tsv
OUT_F=Affymetrix_GenomeWideSNP_6_na31_MT_reannot.tsv

if [ "$1" == "--clean" ]; then
    rm -fv ${OUT_F} dbsnp_index*
    exit 0
fi

${CHECK_RS} ${IN_F} rs_chMT.fas -N 16 -M 128 -o ${OUT_F} --log-level=DEBUG
