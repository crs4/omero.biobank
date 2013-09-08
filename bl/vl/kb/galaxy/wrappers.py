from bl.vl.kb.galaxy.core_wrappers import Workflow as CoreWorkflow
from bl.vl.kb.galaxy.core_wrappers import History as CoreHistory

class Workflow(CoreWorkflow):
    def __init__(self, wf_id, wf_dict, wf_ports=None, wf_links=None):
        super(Workflow, self).__init__(wf_dict)
        setattr(self.core, 'id', wf_id)
        setattr(self.core, 'ports', wf_ports)
        setattr(self.core, 'links', wf_links)
        if wf_id is None:
            super(Workflow, self).touch()                  
    
    def touch(self):
        super(Workflow, self).touch()
        # forget all galaxy connections
        setattr(self.core, 'id', None)
        setattr(self.core, 'links', None)

    def clone(self):
        return self.__class__(None, self.core.wrapped)
        
    @property
    def id(self):
        return self.core.id
    @property
    def ports(self):
        return self.core.ports
    @property
    def links(self):
        return self.core.links

    def __eq__(self, other):
        return  (self.id == other.id 
                 and super(Workflow, self).__eq__(other))
     

class History(CoreHistory):
    pass



    
        
