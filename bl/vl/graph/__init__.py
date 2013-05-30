import bl.vl.utils.graph as grconf


def build_driver(kb=None):

    def build_neo4j_driver(kb):
        from drivers.neo4j import Neo4JDriver
        d = Neo4JDriver(grconf.graph_uri(), grconf.graph_username(),
                        grconf.graph_password(), kb)
        return d

    def build_local_memory_driver(kb):
        from drivers.local_pygraph import PygraphDriver
        d = PygraphDriver(kb, kb.logger)
        return d

    drivers_map = {
        'neo4j': build_neo4j_driver,
        'pygraph': build_local_memory_driver,
    }
    return drivers_map[grconf.graph_driver()](kb)
