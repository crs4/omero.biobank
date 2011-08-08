IMPORTER='../../../tools/importer -P romeo --operator aen'
KB_QUERY='../../../tools/kb_query -P romeo --operator aen'

if false; then

${IMPORTER} -i study.tsv -o study_mapping.tsv study
${IMPORTER} -i individual.tsv -o individual_mapping.tsv individual
${KB_QUERY} -o blood_sample_mapped.tsv \
             map_vid -i blood_sample.tsv \
                 --column individual_label\
                 --source-type Individual \
                 --study BSTUDY

${IMPORTER} -i blood_sample_mapped.tsv -o blood_sample_mapping.tsv \
             biosample \
             --study BSTUDY --source-type Individual \
             --vessel-content BLOOD --vessel-status CONTENTUSABLE \
             --vessel-type Tube

${KB_QUERY} -o dna_sample_mapped.tsv \
             map_vid -i dna_sample.tsv \
                 --column bio_sample_label \
                 --source-type Tube \
                 --study BSTUDY

${IMPORTER} -i dna_sample_mapped.tsv -o dna_sample_mapping.tsv \
             biosample \
             --study BSTUDY --source-type Tube \
             --vessel-content DNA --vessel-status CONTENTUSABLE \
             --vessel-type Tube

${IMPORTER} -i titer_plate.tsv -o titer_plate_mapping.tsv \
            titer_plate --study BSTUDY --plate-shape=32x48 \
            --maker=foomak --model=foomod

${KB_QUERY} -o plate_well_mapped_1.tsv \
             map_vid -i plate_well.tsv \
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


${IMPORTER} -i devices.tsv -o devices_mapping.tsv device --study BSTUDY


${KB_QUERY} -o data_sample_mapped_1.tsv \
             map_vid -i data_sample.tsv \
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
             map_vid -i data_object_mapped.tsv \
                 --column data_sample_label,data_sample \
                 --source-type DataSample \
                 --study BSTUDY


${IMPORTER} -i data_object_mapped.tsv -o data_object_mapping.tsv \
             data_object \
             --study BSTUDY --mimetype=x-vl/affymetrix-cel

${KB_QUERY} -o data_collection_mapped.tsv \
             map_vid -i data_collection.tsv \
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
             map_vid -i diagnosis.tsv \
                 --column individual_label,individual \
                 --source-type Individual \
                 --study BSTUDY

${IMPORTER} -i diagnosis_mapped.tsv \
             diagnosis \
             --study BSTUDY 



${IMPORTER} -i AppliedBioSystem_TaqMan_MSstatus.tsv \
            -o AppliedBioSystem_TaqMan_MSstatus_mapping.tsv \
            marker_definition -S BSTUDY --source CNR-IGMB \
            --context TaqMan --release MSstatus

fi

${IMPORTER} -i AppliedBioSystem_TaqMan_MSstatus_aligned.tsv marker_alignment -S BSTUDY --ref-genome hg28 --message 'alignment done using libbwa'


exit


${IMPORTER} -i taq_man_ms_status_markers_set.tsv markers_set -S BSTUDY --maker CRS4 --model TaqMan --release MSstudy







