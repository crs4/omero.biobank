die() {
    echo $1 1>&2
    exit 1
}

export OME_HOST=${OME_HOST:="localhost"}
export OME_USER=${OME_USER:="root"}
export OME_PASSWD=${OME_PASSWD:="romeo"}

export BASEDIR=$(cd $(dirname ${BASH_SOURCE}); pwd; cd - >/dev/null)
export WORK=${BASEDIR}/work/test_importer

IMPORTER='../../../tools/importer --operator aen'
KB_QUERY='../../../tools/kb_query --operator aen'
CREATE_TABLES='../../../tools/create_tables'
STUDY_LABEL=IMPORTER_TEST_STUDY

mkdir -p ${WORK}
${CREATE_TABLES}

cat <<EOF >${WORK}/study.tsv
label	description
${STUDY_LABEL}	test
EOF
${IMPORTER} -i ${WORK}/study.tsv -o ${WORK}/study_map.tsv \
    study || die "import study failed"


cat <<EOF >${WORK}/individuals.tsv
study	label	gender	father	mother
${STUDY_LABEL}	I001	male	None	None
${STUDY_LABEL}	I002	female	None	None
${STUDY_LABEL}	I003	male	I001	I002
${STUDY_LABEL}	I004	female	I001	I002
${STUDY_LABEL}	I005	male	I003	I004
${STUDY_LABEL}	I006	male	I003	I004
EOF
${IMPORTER} -i ${WORK}/individuals.tsv -o ${WORK}/individual_map.tsv \
    individual --study ${STUDY_LABEL} || die "import individual failed"


cat <<EOF >${WORK}/blood_samples.tsv
label	individual_label
I001-bs-2	I001
I002-bs-2	I002
I003-bs-2	I003
I004-bs-2	I004
I005-bs-2	I005
I006-bs-2	I006
EOF
${KB_QUERY} -o ${WORK}/blood_sample_vids.tsv map_vid \
    -i ${WORK}/blood_samples.tsv --column individual_label \
    --source-type Individual \
    --study ${STUDY_LABEL} || die "map blood sample vid failed"

${IMPORTER} -i ${WORK}/blood_sample_vids.tsv -o ${WORK}/blood_sample_map.tsv \
    biosample --study ${STUDY_LABEL} --source-type Individual \
    --vessel-content BLOOD --vessel-status CONTENTUSABLE \
    --vessel-type Tube || die "import blood sample failed"


cat <<EOF >${WORK}/dna_samples.tsv
label	barcode	sample_label	used_volume	current_volume	status
I001-dna-4	789870	I001-bs-2	0.3	0.2	USABLE
I002-dna-4	789871	I002-bs-2	0.31	0.21	USABLE
I003-dna-4	789872	I003-bs-2	0.32	0.22	USABLE
I004-dna-4	789873	I004-bs-2	0.33	0.23	USABLE
I005-dna-4	789874	I005-bs-2	0.34	0.24	USABLE
I006-dna-4	789875	I006-bs-2	0.35	0.25	USABLE
EOF
${KB_QUERY} -o ${WORK}/dna_sample_vids.tsv map_vid -i ${WORK}/dna_samples.tsv \
    --column sample_label --source-type Tube \
    --study ${STUDY_LABEL} || die "map dna sample vid failed"

${IMPORTER} -i ${WORK}/dna_sample_vids.tsv -o ${WORK}/dna_sample_map.tsv \
    biosample --study ${STUDY_LABEL} --source-type Tube \
    --vessel-content DNA --vessel-status CONTENTUSABLE \
    --vessel-type Tube || die "import dna sample failed"

cat <<EOF >${WORK}/titer_plates.tsv
study	label	barcode	rows	columns	maker	model
TEST01	P001	1234567	32	48	foomaker	foomodel
TEST01	P002	1234568	32	48	foomaker	foomodel
TEST01	P003	1234569	32	48	foomaker	foomodel
TEST01	P004	1234570	32	48	foomaker	foomaker
EOF
${IMPORTER} -i ${WORK}/titer_plates.tsv -o ${WORK}/titer_plate_map.tsv \
    samples_container --container-type=TiterPlate --study ${STUDY_LABEL} \
    --plate-shape=32x48 || die "import plate failed"

cat <<EOF >${WORK}/plate_wells.tsv
study	label	plate_label	row	column	sample_label	used_volume	current_volume
TEST01	A01	P001	1	1	I001-dna-4	0.01	0.01
TEST01	A02	P001	1	2	I002-dna-4	0.02	0.02
TEST01	A03	P002	1	3	I003-dna-4	0.03	0.03
TEST01	E04	P003	5	4	I004-dna-4	0.04	0.04
TEST01	A05	P004	1	5	I005-dna-4	0.05	0.05
TEST01	B06	P004	2	6	I006-dna-4	0.06	0.06
EOF
${KB_QUERY} -o ${WORK}/plate_well_vids_1.tsv map_vid \
    -i ${WORK}/plate_wells.tsv --column sample_label \
    --source-type Tube --study ${STUDY_LABEL} || die "map well vid 1 failed"

${KB_QUERY} -o ${WORK}/plate_well_vids_2.tsv map_vid \
    -i ${WORK}/plate_well_vids_1.tsv \
    --column plate_label,plate --source-type TiterPlate \
    --study ${STUDY_LABEL} || die "map well vid 2 failed"

${IMPORTER} -i ${WORK}/plate_well_vids_2.tsv -o ${WORK}/plate_well_map.tsv \
    biosample --study ${STUDY_LABEL} --source-type Tube \
    --action-category ALIQUOTING --vessel-status CONTENTUSABLE \
    --vessel-type PlateWell || die "import well failed"

cat <<EOF >${WORK}/devices.tsv
device_type	label	barcode	maker	model	release	location
Scanner	pula01	8989898	Affymetrix	GeneChip Scanner 3000	7G	Pula Ed.5
Chip	chip001	8329482	Affymetrix	Genome-Wide Human SNP Array	6.0	None
Chip	chip002	8329483	Affymetrix	Genome-Wide Human SNP Array	6.0	None
Chip	chip003	8329484	Affymetrix	Genome-Wide Human SNP Array	6.0	None
Chip	chip004	8329485	Affymetrix	Genome-Wide Human SNP Array	6.0	None
Chip	chip005	8329486	Affymetrix	Genome-Wide Human SNP Array	6.0	None
Chip	chip006	8329487	Affymetrix	Genome-Wide Human SNP Array	6.0	None
EOF
${IMPORTER} -i ${WORK}/devices.tsv -o devices_map.tsv device \
    --study ${STUDY_LABEL} || die "import device failed"

cat <<EOF >${WORK}/data_samples.tsv
study	label	sample_label	device_label	status
TEST01	foobar-00	P001:A01	chip001	USABLE
TEST01	foobar-01	P001:A02	chip002	USABLE
TEST01	foobar-02	P002:A03	chip003	USABLE
TEST01	foobar-03	P003:E04	chip004	USABLE
TEST01	foobar-04	P004:A05	chip005	USABLE
TEST01	foobar-05	P004:B06	chip006	USABLE
EOF
${KB_QUERY} -o ${WORK}/data_sample_vids_1.tsv map_vid \
    -i ${WORK}/data_samples.tsv --column sample_label \
    --source-type PlateWell \
    --study ${STUDY_LABEL} || die "map data sample vid 1 failed"

${KB_QUERY} -o ${WORK}/data_sample_vids_2.tsv map_vid \
    -i ${WORK}/data_sample_vids_1.tsv \
    --column device_label,device --source-type Chip \
    --study ${STUDY_LABEL} || die "map data sample vid 2 failed"

SCANNER=$(python -c "from bl.vl.kb import KnowledgeBase as KB; kb = KB(driver='omero')('${OME_HOST}', '${OME_USER}', '${OME_PASSWD}'); print kb.get_device('pula01').id")
${IMPORTER} -i ${WORK}/data_sample_vids_2.tsv -o data_sample_map.tsv \
    data_sample --study ${STUDY_LABEL} --source-type PlateWell \
    --device-type Chip --scanner ${SCANNER} || die "import data sample failed"

cat <<EOF >${WORK}/data_objects.tsv
study	path	data_sample_label	mimetype	size	sha1
TEST01	file:/share/fs/v000.cel	foobar-00	x-vl/affymetrix-cel	8989	SHA1SHA1
TEST01	file:/share/fs/v001.cel	foobar-01	x-vl/affymetrix-cel	8989	SHA1SHA1
TEST01	file:/share/fs/v002.cel	foobar-02	x-vl/affymetrix-cel	8989	SHA1SHA1
TEST01	file:/share/fs/v003.cel	foobar-03	x-vl/affymetrix-cel	8989	SHA1SHA1
TEST01	file:/share/fs/v004.cel	foobar-04	x-vl/affymetrix-cel	8989	SHA1SHA1
TEST01	file:/share/fs/v005.cel	foobar-05	x-vl/affymetrix-cel	8989	SHA1SHA1
TEST01	file:/share/fs/v051.cel	foobar-05	x-vl/affymetrix-cel	8989	SHA1SHA1
EOF
${KB_QUERY} -o ${WORK}/data_object_vids.tsv map_vid \
    -i ${WORK}/data_objects.tsv \
    --column data_sample_label,data_sample --source-type DataSample \
    --study ${STUDY_LABEL} || die "map data object vid failed"

${IMPORTER} -i ${WORK}/data_object_vids.tsv -o data_object_map.tsv \
    data_object --study ${STUDY_LABEL} \
    --mimetype=x-vl/affymetrix-cel || die "import data object failed"

cat <<EOF >${WORK}/data_collections.tsv
study	label	data_sample_label
TEST01	DC-29	foobar-00
TEST01	DC-29	foobar-01
TEST01	DC-29	foobar-02
TEST01	DC-29	foobar-03
TEST01	DC-29	foobar-04
TEST01	DC-28	foobar-00
TEST01	DC-28	foobar-01
TEST01	DC-28	foobar-02
EOF
${KB_QUERY} -o ${WORK}/data_collection_vids.tsv map_vid \
    -i ${WORK}/data_collections.tsv \
    --column data_sample_label,data_sample --source-type DataSample \
    --study ${STUDY_LABEL} || die "map data collection vid failed"

${IMPORTER} -i ${WORK}/data_collection_vids.tsv -o data_collection_map.tsv \
    data_collection \
    --study ${STUDY_LABEL} || die "import data collection failed"

cat <<EOF >${WORK}/diagnosis.tsv
study	individual_label	timestamp	diagnosis
TEST01	I001	1310057541608	exclusion-problem_diagnosis
TEST01	I002	1310057541608	icd10-cm:G35
TEST01	I003	1310057541608	icd10-cm:E10
TEST01	I004	1310057541608	exclusion-problem_diagnosis
TEST01	I005	1310057541608	icd10-cm:G35
TEST01	I006	1310057541608	exclusion-problem_diagnosis
TEST01	I003	1310057541700	icd10-cm:E10
TEST01	I003	1310057541700	icd10-cm:G35
EOF
${KB_QUERY} -o ${WORK}/diagnosis_vids.tsv map_vid \
    -i ${WORK}/diagnosis.tsv \
    --column individual_label,individual --source-type Individual \
    --study ${STUDY_LABEL} || die "map diagnosis vid failed"

${IMPORTER} -i ${WORK}/diagnosis_vids.tsv diagnosis \
    --study ${STUDY_LABEL} || die "import diagnosis failed"

FOO_GROUP=foo-`date +"%F-%R"`
${KB_QUERY} --ofile ${WORK}/group.tsv selector --study ${STUDY_LABEL} \
    --group-label ${FOO_GROUP} --total-number=4 --male-fraction=0.5 \
    --reference-disease=icd10-cm:G35 \
    --control-fraction=0.5 || die "select group failed"

${IMPORTER} -i ${WORK}/group.tsv group || die "import selected group failed"
