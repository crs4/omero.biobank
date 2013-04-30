from neo4j import Neo4JDriver
import bl.vl.kb.config as vlconf


def build_driver(kb=None):

    def build_neo4j_driver(kb):
        d = Neo4JDriver(vlconf.GRAPH_ENGINE_URI, vlconf.GRAPH_ENGINE_USERNAME,
                        vlconf.GRAPH_ENGINE_PASSWORD, kb)
        return d

    def build_local_memory_driver(kb):
        raise NotImplementedError()

    drivers_map = {
        'neo4j': build_neo4j_driver,
        'local_memory': build_local_memory_driver,
    }
    return drivers_map[vlconf.GRAPH_ENGINE_DRIVER](kb)