#!/bin/bash

die() {
    echo $1 1>&2
    exit 1
}

export BASEDIR=$(cd $(dirname ${BASH_SOURCE}); pwd; cd - >/dev/null)
export WORK=${BASEDIR}/work

TOOL="../../../tools/snp_manager --loglevel DEBUG"

if [ "$1" == "--clean" ]; then
    rm -rfv ${WORK}
    exit 0
fi

mkdir -p ${WORK}

echo "Testing convert_dbsnp"
${TOOL} --logfile ${WORK}/convert_dbsnp.log convert_dbsnp -d ${BASEDIR} -o ${WORK}/dbsnp_marker_definitions.tsv || die "test failed"

echo "Testing convert_affy"
${TOOL} --logfile ${WORK}/convert_affy.log convert_affy -i affyTest.csv -o ${WORK}/affy_marker_definitions.tsv || die "test failed"

echo "Testing convert_ill"
${TOOL} --logfile ${WORK}/convert_ill.log convert_ill -i illTest.csv -o ${WORK}/ill_marker_definitions.tsv || die "test failed"

echo "Testing markers_to_fastq"
${TOOL} --logfile ${WORK}/markers_to_fastq.log markers_to_fastq -i markerDefinitionsTest.tsv -o ${WORK}/reads.fastq || die "test failed"

echo "Testing convert_sam"
${TOOL} --logfile ${WORK}/convert_sam_to_ma.log convert_sam -i reads.sam -o ${WORK}/marker_alignment.tsv --reftag hg18 || die "test convert to ma failed"
${TOOL} --logfile ${WORK}/convert_sam_to_se.log convert_sam -i reads.sam -o ${WORK}/segment_extractor.tsv --reftag hg18 --output-format segment_extractor || die "test convert to se failed"

echo "Testing build_index"
${TOOL} --logfile ${WORK}/build_index.log build_index -i extracted_segments.tsv -o ${WORK} --reftag hg18 || die "test failed"
pushd ${WORK} >/dev/null
python ${BASEDIR}/dump_db.py dbsnp_index_hg18_251.db || die "dump db failed"
popd >/dev/null

echo "Testing lookup_index"
${TOOL} --logfile ${WORK}/lookup_index.log lookup_index -i test_extracted_segments.tsv --index-file ${WORK}/dbsnp_index_hg18_251.db -O affy_marker_definitions_mod.tsv -o ${WORK}/affy_marker_definitions_reannot.tsv --align-file affy_marker_alignments.tsv || die "test failed"

echo "Testing patch_alignments"
python generate_alignments.py ${WORK}/affy_marker_definitions.tsv ${WORK}/al.tsv hg18  || die "generate alignments failed"
${TOOL} --logfile ${WORK}/patch_alignments.log patch_alignments -a ${WORK}/al.tsv -d ${WORK}/affy_marker_definitions.tsv -o ${WORK}/al_patched.tsv --reftag hg18  || die "test failed"
