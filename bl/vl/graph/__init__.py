from neo4j import Neo4JDriver
from local_pygraph import PygraphDriver
import bl.vl.kb.config as vlconf


def build_driver(kb=None):

    def build_neo4j_driver(kb):
        d = Neo4JDriver(vlconf.GRAPH_ENGINE_URI, vlconf.GRAPH_ENGINE_USERNAME,
                        vlconf.GRAPH_ENGINE_PASSWORD, kb)
        return d

    def build_local_memory_driver(kb):
        d = PygraphDriver(kb, kb.logger)
        return d

    drivers_map = {
        'neo4j': build_neo4j_driver,
        'pygraph': build_local_memory_driver,
    }
    return drivers_map[vlconf.GRAPH_ENGINE_DRIVER](kb)