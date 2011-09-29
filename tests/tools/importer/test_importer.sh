IMPORTER='../../../tools/importer -P test --operator aen'
KB_QUERY='../../../tools/kb_query -P test --operator aen'

${IMPORTER} -i small/study.tsv -o study_mapping.tsv study
${IMPORTER} -i small/individual.tsv -o individual_mapping.tsv individual
${KB_QUERY} -o blood_sample_mapped.tsv \
             map_vid -i small/blood_sample.tsv \
                 --column individual_label\
                 --source-type Individual \
                 --study BSTUDY

${IMPORTER} -i blood_sample_mapped.tsv -o blood_sample_mapping.tsv \
             biosample \
             --study BSTUDY --source-type Individual \
             --vessel-content BLOOD --vessel-status CONTENTUSABLE \
             --vessel-type Tube

${KB_QUERY} -o dna_sample_mapped.tsv \
             map_vid -i small/dna_sample.tsv \
                 --column bio_sample_label \
                 --source-type Tube \
                 --study BSTUDY

${IMPORTER} -i dna_sample_mapped.tsv -o dna_sample_mapping.tsv \
             biosample \
             --study BSTUDY --source-type Tube \
             --vessel-content DNA --vessel-status CONTENTUSABLE \
             --vessel-type Tube

${IMPORTER} -i small/titer_plate.tsv -o titer_plate_mapping.tsv \
            titer_plate --study BSTUDY --plate-shape=32x48 \
            --maker=foomak --model=foomod

${KB_QUERY} -o plate_well_mapped_1.tsv \
             map_vid -i small/plate_well.tsv \
                 --column bio_sample_label \
                 --source-type Tube \
                 --study BSTUDY

${KB_QUERY} -o plate_well_mapped_2.tsv \
             map_vid -i plate_well_mapped_1.tsv \
                 --column plate_label,plate \
                 --source-type TiterPlate \
                 --study BSTUDY

${IMPORTER} -i plate_well_mapped_2.tsv -o plate_well_mapping.tsv \
             biosample \
             --study BSTUDY --source-type Tube --action-category ALIQUOTING \
             --vessel-status CONTENTUSABLE --vessel-type PlateWell


${IMPORTER} -i small/devices.tsv -o devices_mapping.tsv device --study BSTUDY


${KB_QUERY} -o data_sample_mapped_1.tsv \
             map_vid -i small/data_sample.tsv \
                 --column sample_label \
                 --source-type PlateWell \
                 --study BSTUDY

${KB_QUERY} -o data_sample_mapped_2.tsv \
             map_vid -i data_sample_mapped_1.tsv \
                 --column device_label,device \
                 --source-type Chip \
                 --study BSTUDY

SCANNER=`grep pula01 devices_mapping.tsv | perl -ane "print @F[3];"`
${IMPORTER} -i data_sample_mapped_2.tsv -o data_sample_mapping.tsv \
             data_sample \
             --study BSTUDY --source-type PlateWell \
             --device-type Chip --scanner ${SCANNER}

${KB_QUERY} -o data_object_mapped.tsv \
             map_vid -i small/data_object.tsv \
                 --column data_sample_label,data_sample \
                 --source-type DataSample \
                 --study BSTUDY


${IMPORTER} -i data_object_mapped.tsv -o data_object_mapping.tsv \
             data_object \
             --study BSTUDY --mimetype=x-vl/affymetrix-cel

${KB_QUERY} -o data_collection_mapped.tsv \
             map_vid -i small/data_collection.tsv \
                 --column data_sample_label,data_sample \
                 --source-type DataSample \
                 --study BSTUDY


${IMPORTER} -i data_collection_mapped.tsv -o data_collection_mapping.tsv \
             data_collection \
             --study BSTUDY 



#-----------------
# use the following command to scratch and recreate the markers tables
# THIS IS A VERY DANGEOURS THING TO DO.
# If you are not sure, DO NOT DO IT!
#../../../tools/create_tables  -P romeo --do-it
#-----------------



${KB_QUERY} -o diagnosis_mapped.tsv \
             map_vid -i small/diagnosis.tsv \
                 --column individual_label,individual \
                 --source-type Individual \
                 --study BSTUDY

${IMPORTER} -i diagnosis_mapped.tsv \
             diagnosis \
             --study BSTUDY 



${IMPORTER} -i small/marker_definition.tsv \
            -o marker_definition_mapping.tsv \
            marker_definition --study BSTUDY --source CNR-IGMB \
            --context TaqMan --release MSstatus


${KB_QUERY} -o marker_alignment_mapped.tsv \
            map_vid -i small/marker_alignment.tsv\
            --source-type Marker --column label,marker_vid

${IMPORTER} -i marker_alignment_mapped.tsv \
            marker_alignment --study BSTUDY --ref-genome hg28  \
            --message 'alignment done using libbwa'

${KB_QUERY} -o markers_set_mapped.tsv \
            map_vid -i small/markers_set.tsv \
            --source-type Marker --column marker_label,marker_vid

${IMPORTER} -i markers_set_mapped.tsv \
            markers_set \
            --study BSTUDY \
            --label MSET0 --maker CRS4 --model TaqMan --release MSstudy


${KB_QUERY} --ofile group_foo.tsv selector --study BSTUDY --group-label foo \
            --total-number=2 \
            --male-fraction=0.5\
            --reference-disease=icd10-cm:G35 \
            --control-fraction=0.0

${IMPORTER} -i group_foo.tsv group 







