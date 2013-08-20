"""
A set of utility classes and functions for low level loader chores.
"""

# all of this should go in proxy
REQUIRED = 'required'
OPTIONAL = 'optional'
VID = 'vid'
STRING = 'string'
BOOLEAN = 'boolean'
INT = 'int'
LONG = 'long'
FLOAT = 'float'
TEXT = 'text'
TIMESTAMP = 'timestamp'
SELF_TYPE = 'self-type'

DEWRAPPING = {
    VID: str,
    STRING: str,
    TEXT: str,
    FLOAT: float,
    INT: int,
    LONG: int,
    BOOLEAN: bool,
    }

def is_a_kb_object(vtype):
    return type(vtype) != str

def dewrap(desc, value):
    """Apply type conversion required by desc to value and return result."""
    return DEWRAPPING[desc](value)

def sort_by_dependency(graph):
    """Return list of graph nodes sorted by ascending depth of dependency."""
    touched, nodes, ordered = set(), set(graph.nodes()), []
    while len(nodes) > 0:
        selected = [n for n in nodes
                    if set(graph.incidents(n)).issubset(touched)]
        ordered += selected
        selected = set(selected)
        nodes -= selected
        touched |= selected
    return ordered

class Error(Exception):
    """Base class for exceptions in this module."""
    pass

class UnresolvedDependency(Error):
    """Exception raised when an object has an unresolved dependency"""
    def __init__(self, obj, key):
        self.obj = obj
        self.key = key

class UnknownKey(Error):
    """Exception raised when encountering an unknown key."""
    def __init__(self, obj, key):
        self.obj = obj
        self.key = key

def get_attribute(obj, key):
    if hasattr(obj, key):
        return getattr(obj, key)
    else:
        raise UnknownKey(obj, key)

def get_field_descriptor(object_type, field_name):
    while hasattr(object_type, '__fields__'):
        if field_name in object_type.__fields__:
            return object_type.__fields__[field_name]
        object_type = object_type.__base__
    else:
        raise UnknownKey(object_type, field_name)

