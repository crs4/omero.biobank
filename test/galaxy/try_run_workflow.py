import uuid
ws = gi.get_workflows()
w = ws[0]

d = '/home/zag/work/vs/hg/galaxy-dist/sspace/SSPACE-BASIC-2.0_linux-x86_64/example/'
input_paths  = dict([
    ('contigs', d + 'contigs_abyss.fasta'),
    ('reads', d + 'SRR001665_red_1.fastq'),
    ('mates', d + 'SRR001665_red_2.fastq')
    ])

h = gi.run_workflow(w, input_paths, True)



