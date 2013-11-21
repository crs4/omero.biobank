"""
Context manager Adapter
=======================


"""
from abc import ABCMeta, abstractmethod


class ContextManager(object):
    __metaclass__ = ABCMeta
    
    def __init__(self, kb):
        self.kb = kb
    def __enter__(self):
        self.kb.push_context_manager(self)
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        self.kb.pop_context_manager()
        return False

    @abstractmethod
    def register(self, obj):
        pass
    @abstractmethod
    def deregister(self, obj):
        pass

class Sandbox(ContextManager):
    def __init__(self, kb):
        super(Sandbox, self).__init__(kb)
        self.kill_list = []

    def __exit__(self, exc_type, exc_value, traceback):
        while self.kill_list:
            self.kb.delete(self.kill_list.pop())
        return super(Sandbox, self).__exit__(exc_type, exc_value, traceback)

    def register(self, obj):
        self.kill_list.append(obj)

    def deregister(self, obj):
        if obj in self.kill_list:
            self.kill_list.remove(obj)


class ContextManagerAdapter(object):
    """
    FIXME
  
    """

    def __init__(self, kb):
        self.kb = kb

    def sandbox(self):
        return Sandbox(self.kb)
    
