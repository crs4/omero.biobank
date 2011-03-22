"""
Doc to be provided.

"""
from markers  import Markers as Markers
from gdo_repos import GdoRepos as GdoRepos

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

def extend_snp_definition_table(stream, op_vid, batch_size=50000):
  """FIXME This is too dangerous for the casual user."""
  marker_service = get_marker_service()
  return marker_service.extend_snp_definition_table(stream, op_vid, batch_size)

def get_snp_definition_table_rows(selector, batch_size=50000):
  marker_service = get_marker_service()
  return marker_service.get_snp_definition_table_rows(selector, batch_size)

#------------
def extend_snp_alignment_table(stream, op_vid, batch_size=50000):
  marker_service = get_marker_service()
  marker_service.extend_snp_alignment_table(stream, op_vid, batch_size)

def get_snp_alignment_table_rows(selector, batch_size=50000):
  marker_service = get_marker_service()
  return marker_service.get_snp_alignment_table_rows(selector, batch_size)

#------------
def extend_snp_set_table(stream, op_vid, batch_size=50000):
  marker_service = get_marker_service()
  return marker_service.extend_snp_set_table(stream, op_vid, batch_size)

def get_snp_set_table_rows(selector, batch_size=50000):
  marker_service = get_marker_service()
  return marker_service.get_snp_set_table_rows(selector, batch_size)


#------------------------------------------------------------------------------------

gdo_repo_service = None

def get_gdo_repo_service():
  global gdo_repo_service
  if gdo_repo_service is None:
    gdo_repo_service = GdoRepos(params['host'], params['user'], params['passwd'])
  return gdo_repo_service

def create_gdo_repository(set_vid):
  mrks = get_snp_set_table_rows('(vid=="%s")' % set_vid)
  if mrks is None:
    raise ValueError('Unknown set %s' % set_vid)
  gdo_repo_service = get_gdo_repo_service()
  return gdo_repo_service.create_repository(set_vid, mrks.shape[0])

def append_gdo(set_vid, probs, confidence, op_vid):
  gdo_repo_service = get_gdo_repo_service()
  return gdo_repo_service.append(set_vid, probs, confidence, op_vid)

def get_gdo(set_vid, vid):
  gdo_repo_service = get_gdo_repo_service()
  return gdo_repo_service.get(set_vid, vid)

def get_gdo_stream(set_vid, batch_size=10):
  gdo_repo_service = get_gdo_repo_service()
  return gdo_repo_service.get_gdo_stream(set_vid, batch_size)




