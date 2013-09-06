"""

No biobank dependencies here.

"""

import json

class Wrapper(object):
    # http://stackoverflow.com/questions/2827623/python-create-object-and-add-attributes-to-it
    def __init__(self, wrapped, parent=None):
        object.__setattr__(self, 'core', lambda: None)
        object.__setattr__(self, 'is_modified', False)        
        object.__setattr__(self, 'parent', parent)    
        setattr(self.core, 'wrapped', wrapped)

    def touch(self):
        object.__setattr__(self, 'is_modified', True)
        if self.parent:
            self.parent.touch()               

    def __setattr__(self, name, value):
        core = self.core
        if core.wrapped.has_key(name):
            core.wrapped[name] = value
            self.touch()
        else:
            raise KeyError('no property with name %s' % name)
    def __getattr__(self, name):
        core = self.core
        if core.wrapped.has_key(name):
            return core.wrapped[name]
        else:
            raise KeyError(name)

    def to_json(self):
        #return json.dumps(self.core.wrapped)
        return self.core.wrapped

    @classmethod
    def from_json(cls, jdef):
        return cls(json.loads(jdef))
        

class Tool(object):
    def __init__(self, step_dict, parent):
        self.step_dict = step_dict
        self.state = json.loads(step_dict['tool_state'])
        self.parent = parent
    @property
    def id(self):
        return self.step_dict['tool_id']
    @property
    def version(self):
        return self.step_dict['tool_version']
    @property
    def errors(self):
        return self.step_dict['tool_errors']
    @property
    def params(self):
        return self.state
    def __getitem__(self, key):
        return json.loads(self.state[key])
    def __setitem__(self, key, value):
        if not self.state.has_key(key):
            raise ValueError(key)
        self.state[key] = json.dumps(value)
        self.parent.touch()
        self.sync()
        
    def sync(self):
        self.step_dict['tool_state'] = json.dumps(self.state)
        
class Step(Wrapper):
    def __init__(self, step_dict, parent):
        super(Step, self).__init__(step_dict, parent)
        if step_dict['type'] == 'tool':
            setattr(self.core, 'tool', Tool(step_dict, self))
    
    @property
    def tool(self):
        return self.core.tool
        
        
class Workflow(Wrapper):
    """
    A modifiable Galaxy workflow description.
    """
    KNOWN_FORMAT_VERSIONS = [u'0.1']
    def __init__(self, wf_dict):
        super(Workflow, self).__init__(wf_dict)
        steps = wf_dict['steps']
        setattr(self.core, 'steps', 
                [Step(steps[str(x)], self) for x in xrange(len(steps))])
    @property
    def steps(self):
        return self.core.steps
        
    def __eq__(self, other):
        return  self.name == other.name

    def __hash__(self):
        return self.name


class History(Wrapper):
    """
    Fixme
    """
    def __init__(self, hdict, hdas):
        super(History, self).__init__(history_dict)
        details = history_dict['']
        setattr(self.core, 'steps', 
                [Step(steps[str(x)]) for x in xrange(len(steps))])
    @property
    def steps(self):
        return self.core.steps
        
    def sync(self):
        for s in self.steps:
            s.sync()

    def __eq__(self, other):
        return  self.id == other.id
        
    def __hash__(self):
        return self.id


class HistoryDatasetAssociation(Wrapper):
    """
    A python friendly wrapper for hda descriptions.
    """
    def __init__(self, hda_dict):
        super(HistoryDatasetAssociation, self).__init__(hda_dict)

    def __eq__(self, other):
        return  self.id == other.id
        
    def __hash__(self):
        return self.id


            
        
        
        
