die() {
    echo $1 1>&2
    exit 1
}

export OME_HOST=${OME_HOST:="localhost"}
export OME_USER=${OME_USER:="root"}
export OME_PASSWD=${OME_PASSWD:="romeo"}

export BASEDIR=$(cd $(dirname ${BASH_SOURCE}); pwd; cd - >/dev/null)
export WORK=${BASEDIR}/test_agnostic

export OMERO_BIOBANK_EXTRA_MODULES=affymetrix_chips

IMPORTER='../../../tools/importer --operator aen'
KB_QUERY='../../../tools/kb_query --operator aen'

STUDY_LABEL=IMPORTER_AGNOSTIC_TEST_STUDY


if [ "$1" == "--clean" ]; then
    rm -rfv ${WORK}
    exit 0
fi

mkdir -p ${WORK}


cat <<EOF >${WORK}/study.tsv
label	description
${STUDY_LABEL}	test
EOF
${IMPORTER} -i ${WORK}/study.tsv -o ${WORK}/study_map.tsv \
    study || die "import study failed"


cat <<EOF >${WORK}/tubes.tsv
label	content	status
LAB0	DNA	CONTENTUSABLE
LAB1	DNA	CONTENTUSABLE
LAB2	DNA	CONTENTUSABLE
LAB3	DNA	CONTENTUSABLE
LAB4	DNA	CONTENTUSABLE
EOF
${IMPORTER} -i ${WORK}/tubes.tsv -o ${WORK}/vid_tubes.tsv \
    agnostic --study ${STUDY_LABEL} --object-type Tube --object-fields="label,content,status" --object-defaults="initialVolume=1.0,currentVolume=1.0"  || die "import tubes failed"

cat <<EOF >${WORK}/affy_array.tsv
source	label	assayType	content	status
LAB0	A001	GENOMEWIDESNP_6	DNA	CONTENTUSABLE
LAB1	A002	GENOMEWIDESNP_6	DNA	CONTENTUSABLE
LAB2	A003	GENOMEWIDESNP_6	DNA	CONTENTUSABLE
LAB3	A004	GENOMEWIDESNP_6	DNA	CONTENTUSABLE
LAB4	A005	GENOMEWIDESNP_6	DNA	CONTENTUSABLE
EOF

${KB_QUERY} -o ${WORK}/affy_array_vids.tsv map_vid \
    -i ${WORK}/affy_array.tsv --column source \
    --source-type Tube \
    --study ${STUDY_LABEL} || die "map affy_array sample vid failed"

${IMPORTER} -i ${WORK}/affy_array_vids.tsv -o ${WORK}/affy_arrays_mapped.tsv \
    agnostic --study ${STUDY_LABEL} --source-type Tube --object-type AffymetrixArray --object-fields="label,content,status,assayType" --object-defaults="initialVolume=1.0,currentVolume=1.0"  || die "import AffymetrixArrays failed"
