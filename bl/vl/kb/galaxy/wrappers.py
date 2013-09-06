from bl.vl.kb.galaxy.core_wrappers import Workflow as CoreWorkflow

class Workflow(CoreWorkflow):
    def __init__(self, wf_id, wf_dict):
        super(Workflow, self).__init__(wf_dict)
        setattr(self.core, 'id', wf_id)

    def clone(self):
        return self.__class__(None, self.to_json())
        
    @property
    def id(self):
        return self.core.id

    
        
