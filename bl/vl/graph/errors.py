class DependencyTreeError(Exception):
    pass


class GraphConnectionError(Exception):
    pass


class MissingNodeError(Exception):
    pass


class MissingEdgeError(Exception):
    pass


class GraphOutOfSyncError(Exception):
    pass


class GraphAuthenticationError(Exception):
    pass