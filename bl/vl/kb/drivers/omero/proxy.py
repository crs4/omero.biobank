import omero.model as om
import omero.rtypes as ort

from proxy_core import ProxyCore

from wrapper import ObjectFactory, MetaWrapper

import action

KOK = MetaWrapper.__KNOWN_OME_KLASSES__

class Proxy(ProxyCore):
  """
  An omero driver for KB.

  """

  def __init__(self, host, user, passwd, session_keep_tokens=1):
    super(Proxy, self).__init__(host, user, passwd, session_keep_tokens)
    self.factory = ObjectFactory(proxy=self)
    #-- learn
    for k in KOK:
      klass = KOK[k]
      setattr(self, klass.get_ome_table(), klass)









