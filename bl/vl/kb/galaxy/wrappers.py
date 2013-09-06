from bl.vl.kb.galaxy.core_wrappers import Workflow as CoreWorkflow
from bl.vl.kb.galaxy.core_wrappers import History as CoreHistory

class Workflow(CoreWorkflow):
    def __init__(self, wf_id, wf_dict, wf_inputs=None):
        super(Workflow, self).__init__(wf_dict)
        setattr(self.core, 'id', wf_id)
        setattr(self.core, 'inputs', wf_inputs)        
    
    def touch(self):
        super(Workflow, self).touch()
        # forget all galaxy connections
        setattr(self.core, 'id', None)
        setattr(self.core, 'inputs', None)    

    def clone(self):
        return self.__class__(None, self.core.wrapped)
        
    @property
    def id(self):
        return self.core.id
    @property
    def inputs(self):
        return self.core.inputs

    def __eq__(self, other):
        return  (self.id == other.id 
                 and super(Workflow, self).__eq__(other))
     

class History(CoreHistory):
    pass



    
        
