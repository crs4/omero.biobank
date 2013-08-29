export OMERO_BIOBANK_EXTRA_MODULES='illumina_chips'

DBNAME_DEFAULT=test_illumina_chips.db
YMLFNAME_DEFAULT=illumina_load.yml

DBNAME=${1:-${DBNAME_DEFAULT}}
YMLFNAME=${2:-${YMLFNAME_DEFAULT}}

python convert_illumina.py ${DBNAME} ${YMLFNAME} 
python try_load.py ${YMLFNAME}
