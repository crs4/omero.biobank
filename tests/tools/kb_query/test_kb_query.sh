KB_QUERY='../../../tools/kb_query -P romeo'

MARKERS_SET="(CRS4,TaqMan,MSstudy)"

${KB_QUERY} tabular --preferred-data-protocol hdfs --fields-set gender_check
${KB_QUERY} tabular --preferred-data-protocol hdfs --fields-set call_gt --data-collection DC-29

${KB_QUERY} markers --markers-set "${MARKERS_SET}" --fields-set definition
${KB_QUERY} markers --markers-set "${MARKERS_SET}" --fields-set mapping
${KB_QUERY} markers --markers-set "${MARKERS_SET}" --fields-set alignment









