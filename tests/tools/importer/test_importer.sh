IMPORTER='../../../tools/importer -P romeo'


${IMPORTER} -i individual.tsv individual -S BSTUDY
${IMPORTER} -i blood_sample.tsv blood_sample
${IMPORTER} -i dna_sample.tsv dna_sample
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







