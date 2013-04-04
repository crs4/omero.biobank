die() {
    echo $1 1>&2
    exit 1
}

cleanup () {
    rm -fv *mapping* *mapped* marker_definitions.tsv markers_sets.tsv \
        markers_sets_16.tsv data_samples_mset1.tsv group_foo.tsv \
        marker_alignments.tsv foo_device.tsv
}


if [ "$1" == "--clean" ]; then
    cleanup
    exit 0
fi


export OME_HOST="localhost"
export OME_USER="root"
export OME_PASSWD="romeo"

IMPORTER='../../../tools/importer --operator aen'
KB_QUERY='../../../tools/kb_query --operator aen'
CREATE_TABLES='../../../tools/create_tables'
DATA_DIR='./small'
STUDY_LABEL=TEST_${RANDOM}${RANDOM}


echo 'Running tests on dataset:' ${DATA_DIR}

${CREATE_TABLES}

${IMPORTER} -i ${DATA_DIR}/study.tsv -o study_mapping.tsv study \
    --label ${STUDY_LABEL} || die "import study failed"

${IMPORTER} -i ${DATA_DIR}/individuals.tsv -o individual_mapping.tsv \
    individual --study ${STUDY_LABEL} || die "import individual failed"

${KB_QUERY} -o blood_sample_mapped.tsv map_vid \
    -i ${DATA_DIR}/blood_samples.tsv --column individual_label \
    --source-type Individual \
    --study ${STUDY_LABEL} || die "map blood sample vid failed"

${IMPORTER} -i blood_sample_mapped.tsv -o blood_sample_mapping.tsv \
    biosample --study ${STUDY_LABEL} --source-type Individual \
    --vessel-content BLOOD --vessel-status CONTENTUSABLE \
    --vessel-type Tube || die "import blood sample failed"

${KB_QUERY} -o dna_sample_mapped.tsv map_vid -i ${DATA_DIR}/dna_samples.tsv \
    --column sample_label --source-type Tube \
    --study ${STUDY_LABEL} || die "map dna sample vid failed"

${IMPORTER} -i dna_sample_mapped.tsv -o dna_sample_mapping.tsv \
    biosample --study ${STUDY_LABEL} --source-type Tube \
    --vessel-content DNA --vessel-status CONTENTUSABLE \
    --vessel-type Tube || die "import dna sample failed"

${IMPORTER} -i ${DATA_DIR}/titer_plates.tsv -o titer_plate_mapping.tsv \
    samples_container --container-type=TiterPlate --study ${STUDY_LABEL} \
    --plate-shape=32x48 || die "import plate failed"

${KB_QUERY} -o plate_well_mapped_1.tsv map_vid \
    -i ${DATA_DIR}/plate_wells.tsv --column sample_label \
    --source-type Tube --study ${STUDY_LABEL} || die "map well vid 1 failed"

${KB_QUERY} -o plate_well_mapped_2.tsv map_vid -i plate_well_mapped_1.tsv \
    --column plate_label,plate --source-type TiterPlate \
    --study ${STUDY_LABEL} || die "map well vid 2 failed"

${IMPORTER} -i plate_well_mapped_2.tsv -o plate_well_mapping.tsv \
    biosample --study ${STUDY_LABEL} --source-type Tube \
    --action-category ALIQUOTING --vessel-status CONTENTUSABLE \
    --vessel-type PlateWell || die "import well failed"

${IMPORTER} -i ${DATA_DIR}/devices.tsv -o devices_mapping.tsv device \
    --study ${STUDY_LABEL} || die "import device failed"

${KB_QUERY} -o data_sample_mapped_1.tsv map_vid \
    -i ${DATA_DIR}/data_samples.tsv --column sample_label \
    --source-type PlateWell \
    --study ${STUDY_LABEL} || die "map data sample vid 1 failed"

${KB_QUERY} -o data_sample_mapped_2.tsv map_vid -i data_sample_mapped_1.tsv \
    --column device_label,device --source-type Chip \
    --study ${STUDY_LABEL} || die "map data sample vid 2 failed"

SCANNER=$(python -c "from bl.vl.kb import KnowledgeBase as KB; kb = KB(driver='omero')('${OME_HOST}', '${OME_USER}', '${OME_PASSWD}'); print kb.get_device('pula01').id")
${IMPORTER} -i data_sample_mapped_2.tsv -o data_sample_mapping.tsv \
    data_sample --study ${STUDY_LABEL} --source-type PlateWell \
    --device-type Chip --scanner ${SCANNER} || die "import data sample failed"

${KB_QUERY} -o data_object_mapped.tsv map_vid -i ${DATA_DIR}/data_objects.tsv \
    --column data_sample_label,data_sample --source-type DataSample \
    --study ${STUDY_LABEL} || die "map data object vid failed"

${IMPORTER} -i data_object_mapped.tsv -o data_object_mapping.tsv \
    data_object --study ${STUDY_LABEL} \
    --mimetype=x-vl/affymetrix-cel || die "import data object failed"

${KB_QUERY} -o data_collection_mapped.tsv map_vid \
    -i ${DATA_DIR}/data_collections.tsv \
    --column data_sample_label,data_sample --source-type DataSample \
    --study ${STUDY_LABEL} || die "map data collection vid failed"

${IMPORTER} -i data_collection_mapped.tsv -o data_collection_mapping.tsv \
    data_collection \
    --study ${STUDY_LABEL} || die "import data collection failed"

${KB_QUERY} -o diagnosis_mapped.tsv map_vid -i ${DATA_DIR}/diagnosis.tsv \
    --column individual_label,individual --source-type Individual \
    --study ${STUDY_LABEL} || die "map diagnosis vid failed"

${IMPORTER} -i diagnosis_mapped.tsv diagnosis \
    --study ${STUDY_LABEL} || die "import diagnosis failed"

FOO_GROUP=foo-`date +"%F-%R"`
${KB_QUERY} --ofile group_foo.tsv selector --study ${STUDY_LABEL} \
    --group-label ${FOO_GROUP} --total-number=4 --male-fraction=0.5 \
    --reference-disease=icd10-cm:G35 \
    --control-fraction=0.5 || die "select group failed"

${IMPORTER} -i group_foo.tsv group || die "import selected group failed"
