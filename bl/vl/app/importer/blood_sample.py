
def make_parser_blood_sample(parser):
  parser.add_argument('-S', '--study', type=str,
                      help="""default study assumed for the reference individuals.
                      It will over-ride the study column value""")
  parser.add_argument('-V', '--initial-volume', type=float,
                      help="""default initial volume assigned to the blood sample.
                      It will over-ride the initial_volume column value""")
  parser.add_argument('-C', '--current-volume', type=float,
                      help="""default current volume assigned to the blood sample.
                      It will over-ride the initial_volume column value.""")

def import_blood_sample_implementation(args):
  pass


SUPPORTED_SUBMODULES = [
  ('blood_sample', 'import new blood sample definitions into a virgil system',
   make_parser_blood_sample, import_blood_sample_implementation),
  ]

