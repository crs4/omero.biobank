import bl.vl.kb.config as blconf
from bl.vl.kb.drivers.omero.utils import _ome_env_variable


def messages_engine_enabled():
    var = 'MESSAGES_QUEUE_ENGINE_ENABLED'
    try:
        return eval(_ome_env_variable(var))
    except ValueError:
        try:
            return getattr(blconf, var)
        except AttributeError:
            raise ValueError("Cant't find config valuer for %s" % var)


def messages_engine_queue():
    var = 'MESSAGES_QUEUE_ENGINE_QUEUE'
    try:
        return _ome_env_variable(var)
    except ValueError:
        try:
            return getattr(blconf, var)
        except AttributeError:
            raise ValueError("Cant't find config valuer for %s" % var)


def messages_engine_host():
    var = 'MESSAGES_QUEUE_ENGINE_HOST'
    try:
        return _ome_env_variable(var)
    except ValueError:
        try:
            return getattr(blconf, var)
        except AttributeError:
            raise ValueError("Cant't find config valuer for %s" % var)


def messages_engine_port():
    var = 'MESSAGES_QUEUE_ENGINE_PORT'
    try:
        return _ome_env_variable(var)
    except ValueError:
        try:
            return getattr(blconf, var)
        except AttributeError:
            raise ValueError("Cant't find config valuer for %s" % var)


def messages_engine_username():
    var = 'MESSAGES_QUEUE_ENGINE_USERNAME'
    try:
        return _ome_env_variable(var)
    except ValueError:
        try:
            return getattr(blconf, var)
        except AttributeError:
            raise ValueError("Cant't find config valuer for %s" % var)


def messages_engine_password():
    var = 'MESSAGES_QUEUE_ENGINE_PASSWORD'
    try:
        return _ome_env_variable(var)
    except ValueError:
        try:
            return getattr(blconf, var)
        except AttributeError:
            raise ValueError("Cant't find config valuer for %s" % var)