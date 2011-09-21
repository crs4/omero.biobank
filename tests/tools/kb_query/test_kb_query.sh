KB_QUERY='../../../tools/kb_query -P romeo --operator aen'

MARKERS_SET="(CRS4,TaqMan,MSstudy)"

${KB_QUERY} global_stats

${KB_QUERY} --ofile foo.tsv selector --group-label foo \
            --total-number=2 \
            --male-fraction=0.5\
            --reference-disease=icd10-cm:G35 \
            --control-fraction=0.0

cat > foo.py <<EOF
writeheader('dc_id', 'gender', 'data_sample',
            'path', 'mimetype', 'size', 'sha1')
for i in Individuals(group):
  for d in DataSamples(i, 'AffymetrixCel'):
    for o in DataObjects(d):
      writerow(group.id, enum_label(i.gender), d.id,
               o.path, o.mimetype, o.size, o.sha1)
EOF

${KB_QUERY} --ofile foo.tsv query --group BSTUDY \
            --code-file foo.py



#${KB_QUERY} tabular --preferred-data-protocol hdfs --fields-set gender_check
#${KB_QUERY} tabular --preferred-data-protocol hdfs --fields-set call_gt --data-collection DC-29

#${KB_QUERY} markers --markers-set "${MARKERS_SET}" --fields-set definition
#${KB_QUERY} markers --markers-set "${MARKERS_SET}" --fields-set mapping
#${KB_QUERY} markers --markers-set "${MARKERS_SET}" --fields-set alignment









