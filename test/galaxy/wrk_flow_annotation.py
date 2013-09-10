{
"inputs": {"contigs" : 
           {"type" : "DataCollection", 
            "fields":   {"contigs": 
                            {"port" : {"step" : "0", "name": "contigs"},
                             "mimetype" : "x-vl/fasta"},
                         "reads":
                            {"port" : {"step" : "1", "name": "reads"},
                             "mimetype" : "x-vl/fastq"},
                         "mates":
                            {"port" : {"step" : "2", "name": "mates"},
                            "mimetype" : "x-vl/fastq"}}}},
"outputs": {"scaffolding":
            {"type" : "DataCollection",
             "fields" : {"finalevidence": 
                         {"port": {"step":"3","name" : "finalevidence"},
                          "mimetype" : "text/plain"},
                         "summary": 
                         {"port": {"step" : "3", "name": "summaryfile"},
                          "mimetype" : "text/plain"}}}}
}
