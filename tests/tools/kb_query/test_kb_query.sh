KB_QUERY='../../../tools/kb_query -P romeo --operator aen'

MARKERS_SET="(CRS4,TaqMan,MSstudy)"

${KB_QUERY} global_stats

${KB_QUERY} --ofile foo.tsv selector --group-label foo \
            --total-number=2 \
            --male-fraction=0.5\
            --reference-disease=icd10-cm:G35 \
            --control-fraction=0.0




#${KB_QUERY} tabular --preferred-data-protocol hdfs --fields-set gender_check
#${KB_QUERY} tabular --preferred-data-protocol hdfs --fields-set call_gt --data-collection DC-29

#${KB_QUERY} markers --markers-set "${MARKERS_SET}" --fields-set definition
#${KB_QUERY} markers --markers-set "${MARKERS_SET}" --fields-set mapping
#${KB_QUERY} markers --markers-set "${MARKERS_SET}" --fields-set alignment









