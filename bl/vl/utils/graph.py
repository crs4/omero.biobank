import bl.vl.kb.config as blconf
from bl.vl.utils import _get_env_variable


def graph_driver():
    var = 'GRAPH_ENGINE_DRIVER'
    try:
        return _get_env_variable(var)
    except ValueError:
        try:
            return getattr(blconf, var)
        except AttributeError:
            raise ValueError("Cant't find config valuer for %s" % var)


def graph_uri():
    var = 'GRAPH_ENGINE_URI'
    try:
        return _get_env_variable(var)
    except ValueError:
        try:
            return getattr(blconf, var)
        except AttributeError:
            raise ValueError("Cant't find config valuer for %s" % var)


def graph_username():
    var = 'GRAPH_ENGINE_USERNAME'
    try:
        return _get_env_variable(var)
    except ValueError:
        try:
            return getattr(blconf, var)
        except AttributeError:
            raise ValueError("Cant't find config valuer for %s" % var)


def graph_password():
    var = 'GRAPH_ENGINE_PASSWORD'
    try:
        return _get_env_variable(var)
    except ValueError:
        try:
            return getattr(blconf, var)
        except AttributeError:
            raise ValueError("Cant't find config valuer for %s" % var)


def build_edge_id(source_node_hash, dest_node_hash):
    return '%s::%s' % (source_node_hash, dest_node_hash)