die() {
    echo $1 1>&2
    exit 1
}

export OME_HOST="localhost"
export OME_USER="root"
export OME_PASSWD="romeo"

export BASEDIR=$(cd $(dirname ${BASH_SOURCE}); pwd; cd - >/dev/null)
export WORK=${BASEDIR}/work

CREATE_MDEF_TABLE="../../../tools/create_tables -H ${OME_HOST} -U ${OME_USER} -P ${OME_PASSWD} --markers --do-it"
IMPORTER='../../../tools/importer --operator aen'
KB_QUERY='../../../tools/kb_query --operator aen'
ADD_ALLELE_FLIP='python ../../../examples/add_allele_flip.py'
BUILD_SSC_IMPORT='python ../../../examples/build_ssc_import_files.py'
GDOIZE_MS='../../../tools/gdoize_ms'

STUDY_LABEL=GDO_TEST_STUDY
MS_LABEL=GDO_TEST_MS
DEVICE_LABEL=GDO_TEST_DEVICE

check_mdef_table() {
    local msg="marker def table not found, create it with ${CREATE_MDEF_TABLE}"
    ${BASEDIR}/check_mdef_table || die "${msg}"
}


check_mdef_table
mkdir -p ${WORK}

cat <<EOF >${WORK}/study.tsv
label	description
${STUDY_LABEL}	test
EOF
${IMPORTER} -i ${WORK}/study.tsv -o ${WORK}/study_map.tsv \
    study || die "import study failed"

cat <<EOF >${WORK}/individual.tsv
label	gender	father	mother
A001	male	None	None
A002	female	None	None
A003	male	A001	A002
A004	female	A001	A002
A005	male	A003	A004
A006	male	A003	A004
A007	male	A003	A004
A008	female	A003	A004
A009	female	A003	A004
A010	male	A003	A004
EOF
${IMPORTER} -i ${WORK}/individual.tsv -o ${WORK}/individual_map.tsv \
    individual --study ${STUDY_LABEL} || die "import individuals failed"

python ${BASEDIR}/make_marker_defs.py 100 ${WORK}/marker_definitions.tsv
${IMPORTER} -i ${WORK}/marker_definitions.tsv \
    -o ${WORK}/marker_definitions_map.tsv \
    marker_definition --study ${STUDY_LABEL} --source CRS4 \
    --context TEST --release 1 --ref-genome hg19 \
    --dbsnp-build 132 || die "import marker definitions failed"

${KB_QUERY} -o ${WORK}/marker_definitions_vids.tsv map_vid \
    -i ${WORK}/marker_definitions.tsv --study ${STUDY_LABEL} \
    --source-type Marker \
    --column label || die "map vid on marker definitions failed"

${ADD_ALLELE_FLIP} ${WORK}/marker_definitions_vids.tsv \
    ${WORK}/marker_set.tsv || die "add allele flip failed"

${IMPORTER} -i ${WORK}/marker_set.tsv \
    -o ${WORK}/marker_set_map.tsv markers_set \
    --study ${STUDY_LABEL} --label ${MS_LABEL} \
    --maker CRS4 --model TEST --release 1 || die "import marker set failed"

cat <<EOF >${WORK}/device.tsv
device_type	label	maker	model	release	markers_set
GenotypingProgram	${DEVICE_LABEL}	CRS4	chipal	0.1.0	${MS_LABEL}
EOF
${KB_QUERY} -o ${WORK}/device_vids.tsv map_vid -i ${WORK}/device.tsv \
    --source-type SNPMarkersSet --column markers_set,markers_set \
    --study ${STUDY_LABEL} || die "map vid of marker set on device file failed"

${IMPORTER} -i ${WORK}/device_vids.tsv -o ${WORK}/device_map.tsv device \
    --study ${STUDY_LABEL} || die "import device failed"

python ${BASEDIR}/make_marker_align.py ${WORK}/marker_definitions_map.tsv \
    ${WORK}/marker_alignments.tsv
# this is basically a no-op
# ${KB_QUERY} -o ${WORK}/marker_alignments_vids.tsv map_vid \
#     -i ${WORK}/marker_alignments.tsv \
#     --source-type Marker --column marker_vid,marker_vid \
#     --study ${STUDY_LABEL} || die "map vid on marker alignments failed"

${IMPORTER} -i ${WORK}/marker_alignments.tsv \
    -o ${WORK}/marker_alignments_map.tsv \
    marker_alignment --markers-set ${MS_LABEL} --ref-genome='hg19' \
    --study ${STUDY_LABEL} || die "import marker alignments failed"

pushd ${WORK}
python ${BASEDIR}/make_ssc.py ${MS_LABEL} individual.tsv
popd
${BUILD_SSC_IMPORT} --ssc-dir ${WORK}/ssc --map-file ${WORK}/ssc_map.tsv \
    --device-label=${DEVICE_LABEL} --marker-set-label=${MS_LABEL} \
    --ds-fn=${WORK}/ssc_data_samples.tsv \
    --do-fn=${WORK}/ssc_data_objects.tsv || die "build ssc import files failed"

${KB_QUERY} -o /tmp/temp.tsv map_vid -i ${WORK}/ssc_data_samples.tsv \
    --study ${STUDY_LABEL} --source-type Individual \
    --column source || die "intermediate map vid on data samples failed"
${KB_QUERY} -o ${WORK}/ssc_data_samples_vids.tsv map_vid -i /tmp/temp.tsv \
    --study ${STUDY_LABEL} --source-type Device \
    --column device,device || die "final map vid on data samples failed"
rm -fv /tmp/temp.tsv

${IMPORTER} -i ${WORK}/ssc_data_samples_vids.tsv \
    -o ${WORK}/ssc_data_samples_vids_mapping.tsv \
    data_sample --study ${STUDY_LABEL} \
    --source-type Individual || die "import data samples failed"

${KB_QUERY} -o ${WORK}/ssc_data_objects_vids.tsv map_vid \
    -i ${WORK}/ssc_data_objects.tsv \
    --study ${STUDY_LABEL} --source-type DataSample \
    --column data_sample_label,data_sample || die "map vid on dos failed"

${IMPORTER} -i ${WORK}/ssc_data_objects_vids.tsv \
    -o ${WORK}/ssc_data_objects_vids_mapping.tsv \
    data_object --study ${STUDY_LABEL} || die "import dos failed"

${GDOIZE_MS} -s ${STUDY_LABEL} -m ${MS_LABEL}
