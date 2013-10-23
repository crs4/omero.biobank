from bl.vl.kb.galaxy.core_wrappers import Workflow as CoreWorkflow
from bl.vl.kb.galaxy.core_wrappers import Library as CoreLibrary
from bl.vl.kb.galaxy.core_wrappers import Folder as CoreFolder
from bl.vl.kb.galaxy.core_wrappers import History as CoreHistory
from bl.vl.kb.galaxy.core_wrappers import HistoryDatasetAssociation \
                                       as CoreHistoryDatasetAssociation
from bl.vl.kb.galaxy.core_wrappers import LibraryDatasetDatasetAssociation \
                                       as CoreLibraryDatasetDatasetAssociation
from bl.vl.kb.galaxy.core_wrappers import LibraryDataset as CoreLibraryDataset

from copy import deepcopy


class Workflow(CoreWorkflow):

    def __init__(self, wf_id, wf_dict, wf_ports=None, wf_links=None):
        links = None if wf_links is None \
                else dict([(d['label'], k) for k, d in wf_links.iteritems()])
        super(Workflow, self).__init__(wf_dict)
        setattr(self.core, 'id', wf_id)
        setattr(self.core, 'ports', wf_ports)
        setattr(self.core, 'links', links)
        if wf_id is None:
            super(Workflow, self).touch()

    def touch(self):
        super(Workflow, self).touch()
        # forget all galaxy connections
        setattr(self.core, 'id', None)
        setattr(self.core, 'links', None)

    def clone(self):
        return self.__class__(None, deepcopy(self.core.wrapped))

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


class Library(CoreLibrary):

    def __init__(self, library_id, library_desc):
        super(CoreLibrary, self).__init__(library_desc)
        setattr(self.core, 'id', library_id)

    @property
    def id(self):
        return self.core.id


class Folder(CoreFolder):

    def __init__(self, folder_desc, library):
        super(Folder, self).__init__(folder_desc)
        setattr(self.core, 'library', library)

    @property
    def library(self):
        return self.core.library


class History(CoreHistory):

    def __init__(self, history_desc, history_das):
        super(History, self).__init__(history_desc)
        setattr(self.core, 'datasets', history_das)

    @property
    def datasets(self):
        return self.core.datasets


class HistoryDatasetAssociation(CoreHistoryDatasetAssociation):
    pass


class LibraryDataset(CoreLibraryDataset):
    pass


class LibraryDatasetDatasetAssociation(CoreLibraryDatasetDatasetAssociation):
    pass
