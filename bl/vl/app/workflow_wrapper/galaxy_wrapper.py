import StringIO, csv, yaml, uuid, time
from datetime import datetime
from bioblend.galaxy import GalaxyInstance

from items import SequencerOutputItem, SeqDataSampleItem

class GalaxyWrapper(object):
    
    # In order to work config_file has to be a YAML file that must contain
    # the following section:
    #
    # ...
    # galaxy:
    #   api_key: your_api_key
    #   url: galaxy_url
    #   sequencer_output_importer_workflow:
    #     label: workflow_label
    #     history_dataset_label: label_into_the_worflow
    #     dsamples_dataset_label: label_into_the_worflow
    #     dobjects_dataset_label: label_into_the_worflow
    #   seq_data_sample_importer_workflow:
    #     label: worflow_label
    #     history_dataset_label: label_into_the_worflow
    #     dsamples_dataset_label: label_into_the_worflow
    #     dobjects_dataset_label: label_into_the_worflow
    def __init__(self, config_file):
        with open(config_file) as cfg:
            conf = yaml.load(cfg)
            if conf.has_key('galaxy'):
                galaxy_conf_values = conf.get('galaxy')
                self.gi = GalaxyInstance(galaxy_conf_values['url'],
                                         galaxy_conf_values['api_key'])
                self.seq_out_workflow_conf = galaxy_conf_values['sequencer_output_importer_workflow']
                self.seq_ds_workflow_conf = galaxy_conf_values['seq_data_sample_importer_workflow']
            else:
                raise RuntimeError('No galaxy configuration in config file')

    def __get_or_create_library(self, name):
        lib_details = self.gi.libraries.get_libraries(name = name)
        if len(lib_details) == 0:
            lib_details = [self.gi.libraries.create_library(name)]
        return lib_details[0]['id']

    def __create_folder(self, folder_name_prefix, library_id):
        folder_details = self.gi.libraries.create_folder(library_id,
                                                         '%s-%r' % (folder_name_prefix,
                                                                    uuid.uuid4().hex))
        return folder_details[0]['id']

    def __drop_library(self, library_id):
        raise NotImplementedError()

    def __upload_to_library(self, data_stream, library_id, folder_id = None):
        dset_details = self.gi.libraries.upload_file_contents(library_id,
                                                              data_stream.getvalue(),
                                                              folder_id = folder_id)
        return dset_details[0]['id']

    def __get_workflow_id(self, workflow_label):
        workflow_mappings = {}
        for wf in self.gi.workflows.get_workflows():
            workflow_mappings.setdefault(wf['name'], []).append(wf['id'])
        if workflow_mappings.has_key(workflow_label):
            if len(workflow_mappings[workflow_label]) == 1:
                return workflow_mappings[workflow_label][0]
            else:
                raise RuntimeError('Multiple workflow with label "%s", unable to resolve ID' % workflow_label)
        else:
            raise ValueError('Unable to retrieve workflow with label "%s"' % workflow_label)

    def __run_workflow(self, workflow_id, dataset_map, history_name_prefix):
        now = datetime.now()
        w_in_mappings = {}
        for k, v in self.gi.workflows.show_workflow(workflow_id)['inputs'].iteritems():
            w_in_mappings[v['label']] = k
        new_dataset_map = {}
        for k, v in dataset_map.iteritems():
            new_dataset_map[w_in_mappings[k]] = v
        history_name = '%s_%s' % (history_name_prefix, now.strftime('%Y-%m-%d_%H:%M:%S'))
        history_details = self.gi.workflows.run_workflow(workflow_id, new_dataset_map,
                                                         history_name = history_name,
                                                         import_inputs_to_history = False)
        return history_details

    def __dump_history_details(self, history):
        tmp = StringIO.StringIO()
        tmp.write(history.json_data)
        tmp.flush()
        return tmp

    def __serialize_options(self, opts_dict):
        if len(opts_dict) == 0:
            return 'None'
        else:
            opts = []
            for k,v in opts_dict.iteritems():
                opts.append('%s=%s' % (k,v))
            return ','.join(opts)

    def __dump_ds_do_datasets(self, items, study):
        ds_csv_header = ['study', 'label', 'source', 'source_type',
                         'seq_dsample_type', 'status', 'device',
                         'options']
        if hasattr(items[0], 'sample_label'):
            ds_csv_header.insert(-1, 'sample')
        do_csv_header = ['study', 'path', 'data_sample', 'mimetype',
                         'size', 'sha1']
        ds_tmp = StringIO.StringIO()
        do_tmp = StringIO.StringIO()
        ds_writer = csv.DictWriter(ds_tmp, ds_csv_header, delimiter = '\t')
        ds_writer.writeheader()
        do_writer = csv.DictWriter(do_tmp, do_csv_header, delimiter = '\t')
        do_writer.writeheader()
        for i in items:
            opts = {}
            if i.tags:
                opts = i.tags
            if i.history_dataset_id:
                opts['history_dataset_id'] = i.history_dataset_id
            ds_record = {'study' : study, 'label' : i.label,
                         'source' : i.source_label,
                         'source_type' : i.source_type,
                         'seq_dsample_type' : i.dataset_type,
                         'status' : i.dataset_status,
                         'device' : i.device_label,
                         'options' : self.__serialize_options(opts)}
            if hasattr(i, 'sample_label'):
                ds_record['sample'] = i.sample_label
            ds_writer.writerow(ds_record)
            for d in i.data_objects:
                do_writer.writerow({'study'       : study,
                                    'path'        : d.path,
                                    'data_sample' : i.label,
                                    'mimetype'    : d.mimetype,
                                    'size'        : d.size,
                                    'sha1'        : d.sha1})
        return ds_tmp, do_tmp

    def __wait(self, history_id, sleep_interval = 5):
        while True:
            status_info = self.gi.histories.get_status(history_id)['state']
            if status_info not in ('queued', 'running'):
                return status_info
            else:
                time.sleep(sleep_interval)
        return status_info

    # Import DataSamples and DataObjects within OMERO.biobank,
    # automatically selects proper workflow by checking object type
    # of 'items' elements
    def run_datasets_import(self, history, items, action_context,
                            async = False):
        history_dataset = self.__dump_history_details(history)
        dsamples_dataset, dobjects_dataset = self.__dump_ds_do_datasets(items,
                                                                        action_context)
        lib_id = self.__get_or_create_library('import_datasets')
        folder_id = self.__create_folder('dataset_import', lib_id)
        hdset_id = self.__upload_to_library(history_dataset, lib_id, folder_id)
        dsset_id = self.__upload_to_library(dsamples_dataset, lib_id, folder_id)
        doset_id = self.__upload_to_library(dobjects_dataset, lib_id, folder_id)
        if type(items[0]) == SequencerOutputItem:
            wf_conf = self.seq_out_workflow_conf
        elif type(items[0]) == SeqDataSampleItem:
            wf_conf = self.seq_ds_workflow_conf
        else:
            raise RuntimeError('Unable to run workflow for type %r' % type(items[0]))
        # Preparing dataset map
        ds_map = {wf_conf['history_dataset_label']: { 'id' :
                      hdset_id, 'src' : 'ld' },
                  wf_conf['dsamples_dataset_label']: { 'id' :
                      dsset_id, 'src' : 'ld' },
                  wf_conf['dobjects_dataset_label']: { 'id' :
                      doset_id, 'src' : 'ld' }
                  }
        hist_details = self.__run_workflow(self.__get_workflow_id(wf_conf['label']),
                                           ds_map, 'seq_datasets_import')
        if async:
            return hist_details
        else:
            status = self.__wait(hist_details['history'])
            if status == 'ok':
                return hist_details
            else:
                raise RuntimeError('Error occurred while processing data')


    def run_flowcells_import(self, samplesheet_data, action_context, namespace = None,
                             async = False):
        pass
