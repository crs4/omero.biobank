"""
Doc to be provided.

"""
from markers import Markers as Markers

params = {'host': None, 'user' : None, 'passwd' : None}

def get_params():
  return params

def open(host, user, passwd):
  global params
  params['host']   = host
  params['user']   = user
  params['passwd'] = passwd


def close():
  pass

#---------------------------------------------------------
marker_service = None

def get_marker_service():
  global marker_service
  if marker_service is None:
    marker_service = Markers(params['host'], params['user'], params['passwd'])
  return marker_service

def extend_snp_definition_table(stream, op_vid):
  """FIXME This is too dangerous for the casual user."""
  marker_service = get_marker_service()
  marker_service.extend_snp_definition_table(stream, op_vid)

def get_snp_definition_table_rows(selector, batch_size=50000):
  marker_service = get_marker_service()
  return marker_service.get_snp_definition_table_rows(selector, batch_size)

#------------
def extend_snp_alignment_table(stream, op_vid):
  marker_service = get_marker_service()
  marker_service.extend_snp_alignment_table(stream, op_vid)

def get_snp_alignment_table_rows(selector, batch_size=50000):
  marker_service = get_marker_service()
  return marker_service.get_snp_alignment_table_rows(selector, batch_size)

#------------
def extend_snp_set_table(stream, op_vid):
  marker_service = get_marker_service()
  marker_service.extend_snp_set_table(stream, op_vid)

def get_snp_set_table_rows(selector, batch_size=50000):
  marker_service = get_marker_service()
  return marker_service.get_snp_set_table_rows(selector, batch_size)



