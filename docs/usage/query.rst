How to query VL
===============

The ``kb_query`` is the basic command line tool that can be used to
extract information from VL. Similarly to the importer tool is
structured around a modular interface with context specific modules::


	usage: kb_query [-h] [--logfile LOGFILE]
	                [--loglevel {DEBUG,INFO,WARNING,CRITICAL}] [-o OFILE]
	                [-H HOST] [-U USER] [-P PASSWD] [-K KEEP_TOKENS] 
                        --operator  OPERATOR
	                {map_vid,global_stats} ...
	
	A magic kb_query app
	
	positional arguments:
	  {map_vid,global_stats}
	    map_vid             Map user defined objects label to vid. 
                                usage example:
	                        kb_query -H biobank05 -o bs_mapped.tsv map_vid
                                -i blood_sample.tsv 
                                --column 'individual_label' 
                                --study  BSTUDY --source-type Individual
	    global_stats        Extract global stats from KB in tabular form.
	
	optional arguments:
	  -h, --help            show this help message and exit
	  --logfile LOGFILE     logfile. Will write to stderr if not specified
	  --loglevel {DEBUG,INFO,WARNING,CRITICAL}
	                        logging level
	  -o OFILE, --ofile OFILE
	                        the output tsv file
	  -H HOST, --host HOST  omero host system
	  -U USER, --user USER  omero user
	  -P PASSWD, --passwd PASSWD
	                        omero user passwd
	  -K KEEP_TOKENS, --keep-tokens KEEP_TOKENS
	                        omero tokens for open session
	  --operator OPERATOR   operator identifier
	

