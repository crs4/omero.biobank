How to query VL
===============

The ``kb_query`` is the basic command line tool that can be used to
extract information from VL. Similarly to the importer tool is
structured around a modular interface with context specific modules::

  bash$ python ${bl_vl_root}/tools/kb_query --help
   usage: kb_query [-h] [-o OFILE] [-H HOST] [-U USER] [-P PASSWD]
                   [-K KEEP_TOKENS]
                   {tabular,ehr,markers} ...
   
   A magic kb_query app
   
   positional arguments:
     {tabular,ehr,markers}
       tabular             Extract data from the KB in tabular form.
       markers             Extract markers related info from the KB.
       ehr                 Extract ehr related info from the KB.
   
   optional arguments:
     -h, --help            show this help message and exit
     -o OFILE, --ofile OFILE
                           the output tsv file
     -H HOST, --host HOST  omero host system
     -U USER, --user USER  omero user
     -P PASSWD, --passwd PASSWD
                           omero user passwd
     -K KEEP_TOKENS, --keep-tokens KEEP_TOKENS
                           omero tokens for open session

FIXME current module names are not optimal (e.g., tabular??) and will
be changed.

kb_query tabular module
-----------------------

This module extracts in tabular form... FIXME






def make_parser_tabular(parser):
  parser.add_argument('--data-collection', type=str,
                      help="data collection label")
  parser.add_argument('--study-label', type=str,
                      help="study label")
  parser.add_argument('--preferred-data-protocol', type=str,
                      choices=Tabular.SUPPORTED_DATA_PROTOCOLS,
                      default='file',
                      help="""try, if possible, to provide
                      data object paths that use this protocol""")
  parser.add_argument('--fields-set', type=str,
                      choices=Tabular.SUPPORTED_FIELDS_SETS,
                      help="""choose all the fields listed in this set""")


 $ kb_query markers --definition-source (XXX,YYYY,ZZZZ) --marker-set=(Affymetrix,foo,hg28) --fields-set definition


def make_parser_markers(parser):
  parser.add_argument('--definition-source', type=str,
                      help="marker definition source, a tuple (source,context,release)")
  parser.add_argument('--markers-set', type=str,
                      help="a tuple (maker,model,release)")
  parser.add_argument('--fields-set', type=str,
                      choices=Markers.SUPPORTED_FIELDS_SETS,
                      help="""choose all the fields listed in this set""")
