SENTRIX_LOADER=../../utils/sentrix_load_mapping

rm -f illumina_chips.db
rm -f sentrix_load_mapping.log
rm -f csv.list sdfs.list

ls sentrix_load_data/CSV/*.csv > csv.list
ls sentrix_load_data/SDF/*.sdf > sdfs.list

${SENTRIX_LOADER} --csvs csv.list
${SENTRIX_LOADER} --sdfs sdfs.list
${SENTRIX_LOADER} --check-consistency











