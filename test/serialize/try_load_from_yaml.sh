export OMERO_BIOBANK_EXTRA_MODULES='illumina_chips'

DBNAME=test_illumina_chips.db
YMLFNAME=illumina_load.yml

python convert_illumina.py ${DBNAME} ${YMLFNAME} 
python try_load.py ${YMLFNAME}
