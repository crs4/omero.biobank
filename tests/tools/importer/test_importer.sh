IMPORTER='../../../tools/importer -P romeo'
KB_QUERY='../../../tools/kb_query -P romeo'


${IMPORTER} -i study.tsv -o study_mapping.tsv --operator aen study
${IMPORTER} -i individual.tsv -o individual_mapping.tsv --operator aen \
            individual
${KB_QUERY} -o blood_sample_mapped.tsv \
             map -i blood_sample.tsv \
                 --column individual_label\
                 --source-type Individual \
                 --study BSTUDY

${IMPORTER} -i blood_sample_mapped.tsv -o blood_sample_mapping.tsv \
            --operator aen -P romeo \
             biosample \
             --study BSTUDY --source-type Individual \
             --container-content BLOOD --container-status CONTENTUSABLE \
             --container-type Tube

${KB_QUERY} -o dna_sample_mapped.tsv \
             map -i dna_sample.tsv \
                 --column bio_sample_label \
                 --source-type Tube \
                 --study BSTUDY

${IMPORTER} -i dna_sample_mapped.tsv -o dna_sample_mapping.tsv \
            -P romeo  --operator aen \
             biosample \
             --study BSTUDY --source-type Tube \
             --container-content DNA --container-status CONTENTUSABLE \
             --container-type Tube

exit
${IMPORTER} -i titer_plate.tsv titer_plate
${IMPORTER} -i plate_well.tsv plate_well
${IMPORTER} -i devices.tsv device
${IMPORTER} -i data_sample.tsv data_sample
${IMPORTER} -i data_object.tsv data_object
${IMPORTER} -i data_collection.tsv data_collection

#-----------------
# use the following command to scratch and recreate the markers tables
# THIS IS A VERY DANGEOURS THING TO DO.
# If you are not sure, DO NOT DO IT!
../../../tools/create_snp_tables  -P romeo --do-it
#-----------------
${IMPORTER} -i AppliedBioSystem_TaqMan_MSstatus.tsv marker_definition -S BSTUDY --source CNR-IGMB --context TaqMan --release MSstatus
${IMPORTER} -i AppliedBioSystem_TaqMan_MSstatus_aligned.tsv marker_alignment -S BSTUDY --ref-genome hg28 --message 'alignment done using libbwa'
${IMPORTER} -i taq_man_ms_status_markers_set.tsv markers_set -S BSTUDY --maker CRS4 --model TaqMan --release MSstudy







