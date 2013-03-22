import json


class HistoryDetails(object):
    
    def __init__(self, history_data, history_type = None,
                 history_type_version = None, history_id = None,
                 input_ids = None):
        self.history_data = history_data
        self.history_type = history_type
        self.history_type_version = history_type_version
        self.history_id = history_id
        self.input_ids = input_ids

    @property
    def json_data(self):
        return json.dumps({'type'    : self.history_type,
                           'version' : self.history_type_version,
                           'id'      : self.history_id,
                           'inputs'  : self.input_ids,
                           'details' : self.history_data})


class GalaxyHistoryDetails(HistoryDetails):

    def __init__(self, history_data, history_type_version = None,
                 history_id = None, input_ids = None):
        super(GalaxyHistoryDetails, self).__init__(history_data, 'galaxy',
                                                   history_type_version,
                                                   history_id, input_ids)


class BiobankItem(object):

    def __init__(self, label = None, source_label = None,
                 source_type = None):
        self.label = label
        self.source_label = source_label
        self.source_type = source_type


class DataObjectItem(object):
    def __init__(self, path, mimetype, size = -1,
                 sha1 = 'N.A.'):
        self.path = path
        self.mimetype = mimetype
        self.size = size
        self.sha1 = sha1


class DataSampleItem(BiobankItem):

    def __init__(self, dataset_type, dataset_status, data_objects = None,
                 label = None, source_label = None,
                 source_type = None, device_label = None,
                 tags = None, history_dataset_id = None):
        super(DataSampleItem, self).__init__(label, source_label,
                                             source_type)
        self.dataset_type = dataset_type
        self.dataset_status = dataset_status
        self.data_objects = data_objects
        self.device_label = device_label
        self.tags = tags
        self.history_dataset_id = history_dataset_id


class SequencerOutputItem(DataSampleItem):
    
    def __init__self(self, dataset_status, paths = None,
                     label = None, source_label = None,
                     source_type = None, device_label = None,
                     tags = None, history_dataset_id = None):
        super(SequencerOutputItem, self).__init__('SequencerOutput', dataset_status,
                                                  paths, label, source_label,
                                                  source_type, device_label,
                                                  tags, history_dataset_id)


class SeqDataSampleItem(DataSampleItem):
    
    def __init__(self, dataset_status, sample_label = None,
                 paths = None, label = None, source_label = None,
                 source_type = None, device_label = None,
                 tags = None, history_dataset_id = None):
        super(SeqDataSampleItem, self).__init__('SeqDataSample', dataset_status,
                                                paths, label, source_label,
                                                source_type, device_label,
                                                tags, history_dataset_id)
        self.sample_label = sample_label
