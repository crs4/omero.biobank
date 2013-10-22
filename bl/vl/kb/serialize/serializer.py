"""
Serialize biobank objects 
=========================

The general usage scheme is as follows.

.. code-block:: python

    from bl.vl.kb.serialize import Serializer

    devices = kb.get_devices()

    engine = Serializer()
    for d in devices:
       d.serialize(engine)

"""

class Serializer(object):
    """kb serializer interface class.

    FIXME explain structure of conf, by_ref, by_vid, by_label


    """
    def __init__(self, logger):
        self.logger = logger
        self.seen_oids = set()
        self.logger.debug('Serializer initialized')

    def by_vid(self, vid):
        "A reference by vid wrapping"
        return {'by_vid' : vid}
    def by_ref(self, ref):
        "A reference by ref wrapping"
        return {'by_ref' : ref}

    def has_seen(self, oid):
        """Check if an object with id oid has already been seen."""
        self.logger.debug('has_seen(%s)->%s' % (oid, oid in self.seen_oids))
        return oid in self.seen_oids

    def register(self, oid):
        """Register oid in the list of oids already seen."""
        self.logger.debug('registering %s' % oid)
        self.seen_oids.add(oid)

    def serialize(self, oid, klass, conf, vid):
        """Serialize an object of class klass, with id oid, with
        attributes detailed by conf and with vid vid """
        raise NotImplementedError()

