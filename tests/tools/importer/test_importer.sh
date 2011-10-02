IMPORTER='../../../tools/importer -P test --operator aen'
KB_QUERY='../../../tools/kb_query -P test --operator aen'

function help() {
    cat <<EOF
usage: $0 [-d DATASET]

with -d it will use the dataset contained in the DATASET dir. Defaults to
DATASET=./small dataset.
EOF
    exit 0

}

DATA_DIR='./small'
if getopts hd: opt ; then
    case ${opt} in 
    h)  
     help
    ;;
    d)  
     DATA_DIR=${OPTARG} 
    ;;
    esac
fi

echo 'Running tests on dataset:' ${DATA_DIR}

STUDY_LABEL=TEST01


${IMPORTER} -i ${DATA_DIR}/study.tsv -o study_mapping.tsv study
${IMPORTER} -i ${DATA_DIR}/individuals.tsv -o individual_mapping.tsv individual
${KB_QUERY} -o blood_sample_mapped.tsv \
             map_vid -i ${DATA_DIR}/blood_samples.tsv \
                 --column individual_label\
                 --source-type Individual \
                 --study ${STUDY_LABEL}

${IMPORTER} -i blood_sample_mapped.tsv -o blood_sample_mapping.tsv \
             biosample \
             --study ${STUDY_LABEL} --source-type Individual \
             --vessel-content BLOOD --vessel-status CONTENTUSABLE \
             --vessel-type Tube


${KB_QUERY} -o dna_sample_mapped.tsv \
             map_vid -i ${DATA_DIR}/dna_samples.tsv \
                 --column sample_label \
                 --source-type Tube \
                 --study ${STUDY_LABEL}

${IMPORTER} -i dna_sample_mapped.tsv -o dna_sample_mapping.tsv \
             biosample \
             --study ${STUDY_LABEL} --source-type Tube \
             --vessel-content DNA --vessel-status CONTENTUSABLE \
             --vessel-type Tube

${IMPORTER} -i ${DATA_DIR}/titer_plates.tsv -o titer_plate_mapping.tsv \
            titer_plate --study ${STUDY_LABEL} --plate-shape=32x48 \
            --maker=foomak --model=foomod

${KB_QUERY} -o plate_well_mapped_1.tsv \
             map_vid -i ${DATA_DIR}/plate_wells.tsv \
                 --column sample_label \
                 --source-type Tube \
                 --study ${STUDY_LABEL}

${KB_QUERY} -o plate_well_mapped_2.tsv \
             map_vid -i plate_well_mapped_1.tsv \
                 --column plate_label,plate \
                 --source-type TiterPlate \
                 --study ${STUDY_LABEL}

${IMPORTER} -i plate_well_mapped_2.tsv -o plate_well_mapping.tsv \
             biosample \
             --study ${STUDY_LABEL} --source-type Tube --action-category ALIQUOTING \
             --vessel-status CONTENTUSABLE --vessel-type PlateWell


${IMPORTER} -i ${DATA_DIR}/devices.tsv -o devices_mapping.tsv device --study ${STUDY_LABEL}

${KB_QUERY} -o data_sample_mapped_1.tsv \
             map_vid -i ${DATA_DIR}/data_samples.tsv \
                 --column sample_label \
                 --source-type PlateWell \
                 --study ${STUDY_LABEL}

${KB_QUERY} -o data_sample_mapped_2.tsv \
             map_vid -i data_sample_mapped_1.tsv \
                 --column device_label,device \
                 --source-type Chip \
                 --study ${STUDY_LABEL}

SCANNER=`grep pula01 devices_mapping.tsv | perl -ane "print @F[3];"`
${IMPORTER} -i data_sample_mapped_2.tsv -o data_sample_mapping.tsv \
             data_sample \
             --study ${STUDY_LABEL} --source-type PlateWell \
             --device-type Chip --scanner ${SCANNER}

${KB_QUERY} -o data_object_mapped.tsv \
             map_vid -i ${DATA_DIR}/data_objects.tsv \
                 --column data_sample_label,data_sample \
                 --source-type DataSample \
                 --study ${STUDY_LABEL}


${IMPORTER} -i data_object_mapped.tsv -o data_object_mapping.tsv \
             data_object \
             --study ${STUDY_LABEL} --mimetype=x-vl/affymetrix-cel




${KB_QUERY} -o data_collection_mapped.tsv \
             map_vid -i ${DATA_DIR}/data_collections.tsv \
                 --column data_sample_label,data_sample \
                 --source-type DataSample \
                 --study ${STUDY_LABEL}


${IMPORTER} -i data_collection_mapped.tsv -o data_collection_mapping.tsv \
             data_collection \
             --study ${STUDY_LABEL} 



#-----------------
# use the following command to scratch and recreate the markers tables
# THIS IS A VERY DANGEOURS THING TO DO.
# If you are not sure, DO NOT DO IT!
#../../../tools/create_tables  -P romeo --do-it
#-----------------



${KB_QUERY} -o diagnosis_mapped.tsv \
             map_vid -i ${DATA_DIR}/diagnosis.tsv \
                 --column individual_label,individual \
                 --source-type Individual \
                 --study ${STUDY_LABEL}

${IMPORTER} -i diagnosis_mapped.tsv \
             diagnosis \
             --study ${STUDY_LABEL} 



# this will generate the marker_defintions file.
python ./make_marker_defs.py 1000000

${IMPORTER} -i marker_definitions.tsv \
            -o marker_definition_mapping.tsv \
            marker_definition --study ${STUDY_LABEL} --source CRS4 \
            --context TEST --release `date +"%F-%R"`


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
            marker_alignment --study ${STUDY_LABEL} --ref-genome hgFake  \
            --message 'alignment done using libbwa'


# ${KB_QUERY} -o markers_set_mapped.tsv \
#             map_vid -i ${DATA_DIR}/markers_sets.tsv \
#             --source-type Marker --column marker_label,marker_vid

python <<EOF
import csv, random

i = csv.DictReader(open('marker_definition_mapping.tsv'), delimiter='\t')
o = csv.DictWriter(open('markers_sets.tsv', 'w'), 
                   fieldnames=['label', 'marker_vid', 'marker_indx',
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
            --label MSET0-`date +"%f-%R"` \
            --maker CRS4 --model TEST --release `date +"%F-%R"`


${KB_QUERY} --ofile group_foo.tsv selector --study ${STUDY_LABEL} --group-label foo \
            --total-number=2 \
            --male-fraction=0.5\
            --reference-disease=icd10-cm:G35 \
            --control-fraction=0.0

${IMPORTER} -i group_foo.tsv group 







