from bl.vl.kb.drivers.graph import build_driver


class BlockedCallbackError(Exception):
    pass


class DependencyTree(object):

    def __init__(self, kb):
        self.graph_driver = build_driver(kb)

    def __getattr__(self, name):
        if name not in self.graph_driver.BLOCKED_PROXY_CALLBACKS:
            return getattr(self.graph_driver, name)
        else:
            raise BlockedCallbackError('%s.%s cannot be called using DependecyTree proxy' %
                                       (type(self.graph_driver).__name__, name))