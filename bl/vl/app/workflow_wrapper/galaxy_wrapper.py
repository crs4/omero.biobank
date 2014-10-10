import StringIO, csv, yaml, uuid, time
from datetime import datetime
from bioblend.galaxy import GalaxyInstance

from items import SequencerOutputItem, SeqDataSampleItem

# dataset states corresponding to a 'pending' condition
_PENDING_DS_STATES = set(
    ["new", "upload", "queued", "running", "setting_metadata"]
    )
POLLING_INTERVAL = 10

class GalaxyWrapper(object):

    # In order to work config has to be a dictionary loaded from a YAML
    # configuration file containing the following sections:
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
    #   seq_data_sample_intermediate_importer_workflow:
    #     label: workflow_label
    #     history_dataset_label: label_into_the_worflow
    #     dsamples_dataset_label: label_into_the_worflow
    #   seq_data_sample_importer_workflow:
    #     label: worflow_label
    #     history_dataset_label: label_into_the_worflow
    #     dsamples_dataset_label: label_into_the_worflow
    #     dobjects_dataset_label: label_into_the_worflow
    #  flowcell_from_samplesheet_importer_workflow:
    #    label: workflow_label
    #    samplesheet_dataset_label: label_into_the_workflow
    #    config_parameters_file_label: label_into_the_workflow

    def __init__(self, config, logger):
        self.logger = logger
        if GalaxyWrapper.validate_configuration(config):
            galaxy_conf_values = config['galaxy']
            self.gi = GalaxyInstance(galaxy_conf_values['url'],
                                     galaxy_conf_values['api_key'])
            self.seq_out_workflow_conf = galaxy_conf_values['sequencer_output_importer_workflow']
            self.seq_ds_workflow_conf = galaxy_conf_values['seq_data_sample_importer_workflow']
            self.seq_ds_intermediate_workflow_conf = galaxy_conf_values['seq_data_sample_intermediate_importer_workflow']
            self.smpsh_to_fc_workflow_conf = galaxy_conf_values['flowcell_from_samplesheet_importer_workflow']
        else:
            msg = 'Invalid configuration'
            self.logger.error(msg)
            raise ValueError(msg)

    @staticmethod
    def validate_configuration(config):
        if config.has_key('galaxy'):
            keys = ['api_key',
                    'url',
                    'sequencer_output_importer_workflow',
                    'seq_data_sample_importer_workflow',
                    'flowcell_from_samplesheet_importer_workflow']
            for k in keys:
                if not config['galaxy'].has_key(k):
                    return False
            dsamples_keys = ['label',
                             'history_dataset_label',
                             'dsamples_dataset_label',
                             'dobjects_dataset_label']
            for k in ['sequencer_output_importer_workflow',
                      'seq_data_sample_importer_workflow']:
                for dsk in dsamples_keys:
                    if not config['galaxy'][k].has_key(dsk):
                        return False
            flowcell_keys = ['label', 'samplesheet_dataset_label',
                             'config_parameters_file_label']
            for k in ['flowcell_from_samplesheet_importer_workflow']:
                for fk in flowcell_keys:
                    if not config['galaxy'][k].has_key(fk):
                        return False
        else:
            return False
        return True

    def __get_or_create_library(self, name):
        self.logger.debug('Loading library with name %s', name)
        lib_details = self.gi.libraries.get_libraries(name=name)
        if len(lib_details) == 0:
            self.logger.debug('Unable to load library, creating a new one')
            lib_details = [self.gi.libraries.create_library(name)]
        self.logger.debug('Library ID %s', lib_details[0]['id'])
        return lib_details[0]['id']

    def __create_folder(self, folder_name_prefix, library_id):
        folder_name = '%s-%s' % (folder_name_prefix, uuid.uuid4().hex)
        self.logger.debug('Creating folder %s in library %s', folder_name,
                                                                library_id)
        folder_details = self.gi.libraries.create_folder(library_id,
                                                         folder_name)
        self.logger.debug('Folder created with ID %s', folder_details[0]['id'])
        return folder_details[0]['id']

    def __drop_library(self, library_id):
        raise NotImplementedError()

    def __upload_to_library(self, data_stream, library_id, folder_id=None):
        self.logger.debug('Uploading data to library %s', library_id)
        if type(data_stream) == str:
            data = data_stream
        elif hasattr(data_stream, 'getvalue'):
            data = data_stream.getvalue()
        else:
            msg = 'Unable to upload data_stream of type %r to library' % type(data_stream)
            self.logger.error(msg)
            raise RuntimeError(msg)
        dset_details = self.gi.libraries.upload_file_contents(library_id, data,
                                                              folder_id=folder_id)
        self.logger.debug('Data uploaded, dataset ID is %s', dset_details[0]['id'])
        return dset_details[0]['id']

    def __get_workflow_id(self, workflow_label):
        self.logger.debug('Retrieving workflow %s', workflow_label)
        workflow_mappings = {}
        for wf in self.gi.workflows.get_workflows():
            workflow_mappings.setdefault(wf['name'], []).append(wf['id'])
        if workflow_mappings.has_key(workflow_label):
            if len(workflow_mappings[workflow_label]) == 1:
                self.logger.debug('Workflow details: %r', workflow_mappings[workflow_label][0])
                return workflow_mappings[workflow_label][0]
            else:
                msg = 'Multiple workflow with label "%s", unable to resolve ID' % workflow_label
                self.logger.error(msg)
                raise RuntimeError(msg)
        else:
            msg = 'Unable to retrieve workflow with label "%s"' % workflow_label
            self.logger.error(msg)
            raise ValueError(msg)

    def __run_workflow(self, workflow_id, dataset_map, history_name_prefix):
        self.logger.debug('Running workflow %s', workflow_id)
        now = datetime.now()
        w_in_mappings = {}
        for k, v in self.gi.workflows.show_workflow(workflow_id)['inputs'].iteritems():
            w_in_mappings[v['label']] = k
        new_dataset_map = {}
        for k, v in dataset_map.iteritems():
            new_dataset_map[w_in_mappings[k]] = v
        history_name = '%s_%s' % (history_name_prefix, now.strftime('%Y-%m-%d_%H:%M:%S'))
        history_details = self.gi.workflows.run_workflow(workflow_id, new_dataset_map,
                                                         history_name=history_name,
                                                         import_inputs_to_history=False)
        self.logger.debug('Workflow running on history: %r', history_details)
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
        ds_writer = csv.DictWriter(ds_tmp, ds_csv_header, delimiter='\t')
        do_writer = csv.DictWriter(do_tmp, do_csv_header, delimiter='\t')
        try:
            ds_writer.writeheader()
            do_writer.writeheader()
        except AttributeError:
            # python 2.6 compatibility
            ds_tmp.write('\t'.join(ds_csv_header) + '\n')
            do_tmp.write('\t'.join(do_csv_header) + '\n')
        for i in items:
            opts = {}
            if i.tags:
                opts = i.tags
            if i.hist_src_dataset_id:
                opts['hist_src_dataset_id'] = i.hist_src_dataset_id
            if i.hist_res_dataset_id:
                opts['hist_res_dataset_id'] = i.hist_res_dataset_id
            ds_record = {'study' : study, 'label' : i.label,
                         'source' : i.source_label,
                         'source_type' : i.source_type,
                         'seq_dsample_type' : i.dataset_type,
                         'status' : i.dataset_status,
                         'device' : i.device_label,
                         'options' : self.__serialize_options(opts)}
            if hasattr(i, 'sample_label'):
                if i.sample_label:
                    ds_record['sample'] = i.sample_label
                else:
                    ds_record['sample'] = 'None'
            ds_writer.writerow(ds_record)
            for d in i.data_objects:
                do_writer.writerow({'study'       : study,
                                    'path'        : d.path,
                                    'data_sample' : i.label,
                                    'mimetype'    : d.mimetype,
                                    'size'        : d.size,
                                    'sha1'        : d.sha1})
        return ds_tmp, do_tmp

    def __wait(self, history_id, sleep_interval=POLLING_INTERVAL):
        self.logger.debug('Waiting for history %s', history_id)
        while True:
            state_details = self.gi.histories.get_status(history_id)['state_details']
            non_zero = set(state for state, count in state_details.iteritems() if count > 0)
            # With newer versions of Galayx the history state remains as
            # 'queued' even if individual datasets are in an error state
            if 'error' in non_zero:
                self.logger.error("History %s failed to execute. state_details: %s",
                        history_id, state_details)
                return 'error'
            if len(_PENDING_DS_STATES & non_zero) == 0:
                # no pending datasets
                self.logger.debug('Workflow done with statuses %s', non_zero)
                return 'ok'
            self.logger.debug('Workflow not completed (statuses: %s). Wait %d seconds.',
                    non_zero, sleep_interval)
            time.sleep(sleep_interval)

    def __dump_config_params(self, study_label, namespace=None):
        conf_dict = {'config_parameters': {'study_label' : study_label}}
        if namespace:
            conf_dict['config_parameters']['namespace'] = namespace
        return self.__dump_to_yaml(conf_dict)

    def __dump_to_yaml(self, config_dict):
        return yaml.dump(config_dict, default_flow_style=False)

    def __get_library_name(self, lname_prefix):
        return '%s-%s' % (lname_prefix, datetime.now().strftime('%Y-%m-%d_%H:%M:%S'))

    # Import DataSamples and DataObjects within OMERO.biobank,
    # automatically selects proper workflow by checking object type
    # of 'items' elements
    def run_datasets_import(self, history, items, action_context,
                            no_dataobjects=False, async=False):
        self.logger.info('Running datasets import')
        history_dataset = self.__dump_history_details(history)
        dsamples_dataset, dobjects_dataset = self.__dump_ds_do_datasets(items,
                                                                        action_context)
        lib_id = self.__get_or_create_library(self.__get_library_name('import_datasets'))
        folder_id = self.__create_folder('dataset_import', lib_id)
        hdset_id = self.__upload_to_library(history_dataset, lib_id, folder_id)
        dsset_id = self.__upload_to_library(dsamples_dataset, lib_id, folder_id)
        if not no_dataobjects:
            doset_id = self.__upload_to_library(dobjects_dataset, lib_id, folder_id)
        else:
          doset_id = None
        if type(items[0]) == SequencerOutputItem:
            wf_conf = self.seq_out_workflow_conf
        elif type(items[0]) == SeqDataSampleItem:
            if not no_dataobjects:
                wf_conf = self.seq_ds_workflow_conf
            else:
                wf_conf = self.seq_ds_intermediate_workflow_conf
        else:
            raise RuntimeError('Unable to run workflow for type %r' % type(items[0]))
        # Preparing dataset map
        ds_map = {
            wf_conf['history_dataset_label']: {
                'id': hdset_id,
                'src': 'ld'
            },
            wf_conf['dsamples_dataset_label']: {
                'id': dsset_id,
                'src': 'ld'
            }
        }
        if not no_dataobjects:
            ds_map[wf_conf['dobjects_dataset_label']] = {
                'id': doset_id,
                'src': 'ld'
            }
        hist_details = self.__run_workflow(self.__get_workflow_id(wf_conf['label']),
                                           ds_map, 'seq_datasets_import')
        self.logger.info('Workflow running')
        if async:
            self.logger.info('Enabled async run, returning')
            return hist_details
        else:
            self.logger.info('Waiting for run exit status')
            status = self.__wait(hist_details['history'])
            if status == 'ok':
                self.logger.info('Run completed')
                return hist_details, lib_id
            else:
                msg = 'Error occurred while processing data'
                self.logger.error(msg)
                raise RuntimeError(msg)

    # Import a flowcell samplesheet produced by a Galaxy NGLIMS within OMERO.biobank
    def run_flowcell_from_samplesheet_import(self, samplesheet_data, action_context, namespace=None,
                             async=False):
        self.logger.info('Running flowcell samplesheet import')
        conf_params = self.__dump_config_params(action_context, namespace)
        lib_id = self.__get_or_create_library(self.__get_library_name('import_flowcell'))
        folder_id = self.__create_folder('flowcell_from_samplesheet', lib_id)
        samplesheet_id = self.__upload_to_library(samplesheet_data, lib_id, folder_id)
        conf_file_id = self.__upload_to_library(conf_params, lib_id, folder_id)
        wf_conf = self.smpsh_to_fc_workflow_conf
        ds_map = {wf_conf['samplesheet_dataset_label']: {'id':
                      samplesheet_id, 'src': 'ld'},
                  wf_conf['config_parameters_file_label']: {'id':
                      conf_file_id, 'src': 'ld'}
                  }
        hist_details = self.__run_workflow(self.__get_workflow_id(wf_conf['label']),
                                           ds_map, 'flowcell_samplesheet_import')
        self.logger.info('Workflow running')
        if async:
            self.logger.info('Enabled async run, returning')
            return hist_details
        else:
            self.logger.info('Waiting for run exit status')
            status = self.__wait(hist_details['history'])
            if status == 'ok':
                self.logger.info('Run completed')
                return hist_details, lib_id
            else:
                msg = 'Error occurred while processing data'
                self.logger.error(msg)
                raise RuntimeError(msg)

    def delete_history(self, history_id, purge_history=False):
        self.logger.info('Deleting history with ID %s', history_id)
        self.gi.histories.delete_history(history_id, purge_history)
        self.logger.info('History deleted')

    def delete_library(self, library_id):
        self.logger.info('Deleting library with ID %s', library_id)
        self.gi.libraries.delete_library(library_id)
        self.logger.info('Library deleted')
