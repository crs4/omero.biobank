die() {
    echo $1 1>&2
    exit 1
}


IMPORTER='../../../tools/importer -U root -P romeo --operator aen'
KB_QUERY='../../../tools/kb_query -U root -P romeo --operator aen'
CREATE_FAKE_GDO='python create_fake_gdo.py --U root -P romeo'

DATA_DIR='./small'

echo 'Running tests on dataset:' ${DATA_DIR}

STUDY_LABEL=TEST01


${IMPORTER} -i ${DATA_DIR}/study.tsv -o study_mapping.tsv study || die "import study failed"

${IMPORTER} -i ${DATA_DIR}/individuals.tsv -o individual_mapping.tsv individual || die "import individual failed"

${KB_QUERY} -o blood_sample_mapped.tsv \
             map_vid -i ${DATA_DIR}/blood_samples.tsv \
                 --column individual_label\
                 --source-type Individual \
                 --study ${STUDY_LABEL} || die "map blood sample vid failed"

${IMPORTER} -i blood_sample_mapped.tsv -o blood_sample_mapping.tsv \
             biosample \
             --study ${STUDY_LABEL} --source-type Individual \
             --vessel-content BLOOD --vessel-status CONTENTUSABLE \
             --vessel-type Tube || die "import blood sample failed"


${KB_QUERY} -o dna_sample_mapped.tsv \
             map_vid -i ${DATA_DIR}/dna_samples.tsv \
                 --column sample_label \
                 --source-type Tube \
                 --study ${STUDY_LABEL} || die "map dna sample vid failed"

${IMPORTER} -i dna_sample_mapped.tsv -o dna_sample_mapping.tsv \
             biosample \
             --study ${STUDY_LABEL} --source-type Tube \
             --vessel-content DNA --vessel-status CONTENTUSABLE \
             --vessel-type Tube || die "import dna sample failed"

${IMPORTER} -i ${DATA_DIR}/titer_plates.tsv -o titer_plate_mapping.tsv \
            titer_plate --study ${STUDY_LABEL} --plate-shape=32x48 \
            --maker=foomak --model=foomod || die "import plate failed"

${KB_QUERY} -o plate_well_mapped_1.tsv \
             map_vid -i ${DATA_DIR}/plate_wells.tsv \
                 --column sample_label \
                 --source-type Tube \
                 --study ${STUDY_LABEL} || die "map well vid 1 failed"

${KB_QUERY} -o plate_well_mapped_2.tsv \
             map_vid -i plate_well_mapped_1.tsv \
                 --column plate_label,plate \
                 --source-type TiterPlate \
                 --study ${STUDY_LABEL} || die "map well vid 2 failed"

${IMPORTER} -i plate_well_mapped_2.tsv -o plate_well_mapping.tsv \
             biosample \
             --study ${STUDY_LABEL} --source-type Tube \
             --action-category ALIQUOTING \
             --vessel-status CONTENTUSABLE \
             --vessel-type PlateWell || die "import well failed"


${IMPORTER} -i ${DATA_DIR}/devices.tsv -o devices_mapping.tsv device \
    --study ${STUDY_LABEL} || die "import device failed"

${KB_QUERY} -o data_sample_mapped_1.tsv \
             map_vid -i ${DATA_DIR}/data_samples.tsv \
                 --column sample_label \
                 --source-type PlateWell \
                 --study ${STUDY_LABEL} || die "map data sample vid 1 failed"

${KB_QUERY} -o data_sample_mapped_2.tsv \
             map_vid -i data_sample_mapped_1.tsv \
                 --column device_label,device \
                 --source-type Chip \
                 --study ${STUDY_LABEL} || die "map data sample vid 2 failed"

SCANNER=$(python -c "from bl.vl.kb import KnowledgeBase as KB; kb = KB(driver='omero')('localhost', 'root', 'romeo'); print kb.get_device('pula01').id")
${IMPORTER} -i data_sample_mapped_2.tsv -o data_sample_mapping.tsv \
             data_sample \
             --study ${STUDY_LABEL} --source-type PlateWell \
             --device-type Chip \
             --scanner ${SCANNER} || die "import data sample failed"

${KB_QUERY} -o data_object_mapped.tsv \
             map_vid -i ${DATA_DIR}/data_objects.tsv \
                 --column data_sample_label,data_sample \
                 --source-type DataSample \
                 --study ${STUDY_LABEL} || die "map data object vid failed"


${IMPORTER} -i data_object_mapped.tsv -o data_object_mapping.tsv \
             data_object \
             --study ${STUDY_LABEL} \
             --mimetype=x-vl/affymetrix-cel || die "import data object failed"


${KB_QUERY} -o data_collection_mapped.tsv \
             map_vid -i ${DATA_DIR}/data_collections.tsv \
                 --column data_sample_label,data_sample \
                 --source-type DataSample \
                 --study ${STUDY_LABEL} || die "map data collection vid failed"


${IMPORTER} -i data_collection_mapped.tsv -o data_collection_mapping.tsv \
             data_collection \
             --study ${STUDY_LABEL} || die "import data collection failed"



#-----------------
# use the following command to scratch and recreate the markers tables
# THIS IS A VERY DANGEOURS THING TO DO.
# If you are not sure, DO NOT DO IT!
#../../../tools/create_tables  -U root -P romeo --do-it
#-----------------


${KB_QUERY} -o diagnosis_mapped.tsv \
             map_vid -i ${DATA_DIR}/diagnosis.tsv \
                 --column individual_label,individual \
                 --source-type Individual \
                 --study ${STUDY_LABEL} || die "map diagnosis vid failed"

${IMPORTER} -i diagnosis_mapped.tsv \
             diagnosis \
             --study ${STUDY_LABEL} || die "import diagnosis failed"

# this will generate the marker_defintions file.
python ./make_marker_defs.py 100

${IMPORTER} -i marker_definitions.tsv \
            -o marker_definition_mapping.tsv \
            marker_definition --study ${STUDY_LABEL} --source CRS4 \
            --context TEST --release `date +"%F-%R"` \
            --ref-genome hg19 \
            --dbsnp-build 132 || die "import marker definition failed"


python <<EOF
import csv, random

i = csv.DictReader(open('marker_definition_mapping.tsv'), delimiter='\t')
o = csv.DictWriter(open('marker_alignments.tsv', 'w'), 
                   fieldnames=['marker_vid', 'chromosome', 'pos', 
                               'allele', 'strand', 'copies'],
                   delimiter='\t')
o.writeheader()
for r in i:
  y = {'marker_vid' : r['vid'], 'chromosome' : random.randrange(1, 26),
       'pos' : random.randrange(1, 200000000),
       'allele'  : random.choice('AB'),
       'strand'  : random.choice([True, False]),
       'copies'  : 1}
  o.writerow(y)

EOF
# ${KB_QUERY} -o marker_alignment_mapped.tsv \
#             map_vid -i ${DATA_DIR}/marker_alignments.tsv\
#             --source-type Marker --column label,marker_vid

${IMPORTER} -i marker_alignments.tsv \
            marker_alignment --study ${STUDY_LABEL} \
            --ref-genome hgFake || die "import marker alignment failed"


# ${KB_QUERY} -o markers_set_mapped.tsv \
#             map_vid -i ${DATA_DIR}/markers_sets.tsv \
#             --source-type Marker --column marker_label,marker_vid

echo "* define a marker set that uses all known markers"
python <<EOF
import csv, random

i = csv.DictReader(open('marker_definition_mapping.tsv'), delimiter='\t')
o = csv.DictWriter(open('markers_sets.tsv', 'w'), 
                   fieldnames=['marker_vid', 'marker_indx',
                               'allele_flip'],
                   delimiter='\t')
o.writeheader()
for k,r in enumerate(i):
  y = {'marker_vid' : r['vid'], 
       'allele_flip' : random.choice([True, False]),
       'marker_indx'  : k}
  o.writerow(y)

EOF

${IMPORTER} -i markers_sets.tsv \
            -o markers_set_mapping.tsv \
            markers_set \
            --study ${STUDY_LABEL} \
            --label MSET0-`date +"%F-%R"` \
            --maker CRS4 --model MSET0 \
            --release `date +"%F-%R"` || die "import marker set 0 failed"


MSET1=MSET1-`date +"%F-%R"`

echo "* define ${MSET1} a marker set that uses 16 known markers"
python <<EOF
import csv, random

i = csv.DictReader(open('marker_definition_mapping.tsv'), delimiter='\t')
o = csv.DictWriter(open('markers_sets_16.tsv', 'w'), 
                   fieldnames=['marker_vid', 'marker_indx',
                               'allele_flip'],
                   delimiter='\t')
o.writeheader()

recs = [ r for r in i]
for k,r in enumerate(random.sample(recs, 16)):
  y = {'marker_vid' : r['vid'], 
       'allele_flip' : random.choice([True, False]),
       'marker_indx'  : k}
  o.writerow(y)

EOF

${IMPORTER} -i markers_sets_16.tsv \
            -o markers_sets_16_mapping.tsv \
            markers_set \
            --study ${STUDY_LABEL} \
            --label ${MSET1} \
            --maker CRS4 --model MSET1 \
            --release `date +"%F-%R"` || die "import marker set 1 failed"

MSET_VID=$(python -c "from bl.vl.kb import KnowledgeBase as KB; kb = KB(driver='omero')('localhost', 'root', 'romeo'); print kb.get_snp_markers_set(label='${MSET1}').id")

echo "* define a  GenotypingProgram device that generates datasets on ${MSET1}"
DEVICE_FILE=foo_device.tsv
python -c "print 'device_type\tlabel\tmaker\tmodel\trelease\tmarkers_set'" > ${DEVICE_FILE}
python -c "print 'GenotypingProgram\t${MSET1}\tCRS4\tTest\t${MSET1}\t${MSET_VID}'" >> ${DEVICE_FILE}

${IMPORTER} -i foo_device.tsv -o foo_device_mapping.tsv device \
    --study ${STUDY_LABEL} || die "import foo device failed"

#DEVICE_VID=`grep ${MSET1} foo_device_mapping.tsv | perl -ane "print @F[3];"`
DEVICE_VID=$(python -c "from bl.vl.kb import KnowledgeBase as KB; kb = KB(driver='omero')('localhost', 'root', 'romeo'); print kb.get_device('${MSET1}').id")

echo "* extracting a subset of 3 individuals"

FOO_GROUP=foo-`date +"%F-%R"`

${KB_QUERY} --ofile group_foo.tsv \
            selector --study ${STUDY_LABEL} \
            --group-label ${FOO_GROUP} \
            --total-number=4 \
            --male-fraction=0.5\
            --reference-disease=icd10-cm:G35 \
            --control-fraction=0.5 || die "select group failed"

echo "* importing them as study ${FOO_GROUP}"
${IMPORTER} -i group_foo.tsv group || die "import selected group failed"

echo "* adding fake GenotypeDataSample(s) on ${MSET1}."
python <<EOF
import csv, random

i = csv.DictReader(open('group_foo.tsv'), delimiter='\t')
o = csv.DictWriter(open('data_samples_mset1.tsv', 'w'), 
                   fieldnames=['label', 'source', 'device'],
                   delimiter='\t')
o.writeheader()

for k,r in enumerate(i):
  y = {'label' : r['group_code'] + '.' + "${MSET1}",
       'source' : r['individual'],
       'device'  : "${DEVICE_VID}"}
  o.writerow(y)

EOF

${IMPORTER} -i data_samples_mset1.tsv -o data_samples_mset1_mapping.tsv \
             data_sample \
             --device-type GenotypingProgram \
             --data-sample-type GenotypeDataSample \
             --study ${STUDY_LABEL} \
             --source-type Individual || die "import data samples mset1 failed"



echo "* attaching fake DataObject(s) to data samples."
#${CREATE_FAKE_GDO} --data-samples data_samples_mset1.tsv -o ssc_file_list.tsv
