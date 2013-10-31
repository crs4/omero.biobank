"""
Serialize biobank objects to a yaml stream
==========================================

FIXME: describe yaml serialization


"""

from bl.vl.kb.serialize.serializer import Serializer
from bl.vl.kb.serialize.writers import write_yaml

class YamlSerializer(Serializer):
    def __init__(self, ostream, logger):
        super(YamlSerializer, self).__init__(logger)
        self.ostream = ostream
        
    def serialize(self, oid, klass, conf, vid):
        self.logger.debug('serializing %s oid' % oid)
        write_yaml(self.ostream, oid, klass, conf, vid=vid)


