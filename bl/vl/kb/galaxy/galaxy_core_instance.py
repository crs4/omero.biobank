from automator.agent.blend_wrapper import BlendWrapper
from bioblend import galaxy as bg

from bl.vl.kb.galaxy.wrappers import Workflow, Library, Folder
from bl.vl.kb.galaxy.wrappers import LibraryDataset
from bl.vl.kb.galaxy.wrappers import LibraryDatasetDatasetAssociation
from bl.vl.kb.galaxy.wrappers import History, HistoryDatasetAssociation
from bl.vl.utils import get_logger

import uuid
import json
import time
import math
from datetime import datetime

def map_name_to_id(object_list):
    id_by_name = {}
    for w in object_list:
        id_by_name.setdefault(w['name'], []).append(w['id'])
    return id_by_name

def create_workflow_name(root_name):
    assert root_name.find(':') == -1    
    return '%s:%s' % (root_name, uuid.uuid1().hex)
    
def parse_workflow_name(name):
    parts = name.split(':', 1)
    return parts[0], None if len(parts) == 1 else parts

def create_history_name(workflow_name):
    assert workflow_name.find('+') == -1
    return '%s+%s' % (workflow_name, uuid.uuid1().hex)    

def parse_history_name(name):
    parts = name.split('+', 1)
    assert len(parts) == 2
    return parts


class GalaxyInstance(object):
    "FIXME "
    
    def __init__(self, url, api_key, logger=None):
        self.blend = BlendWrapper(bg.GalaxyInstance(url, api_key))
        self.logger = logger if logger \
                      else get_logger('galaxy_instance', level='DEBUG')
        self.logger.debug('initialized on %s with api_key: %s' % (url, api_key))

    def _request_workflow(self, workflow_id):
        self.logger.debug('_request_workflow(%s)' % workflow_id)
        wf = self.blend.gi.workflows.export_workflow_json(workflow_id)
        if not wf:
            msg = 'Empty reply when requesting workflow %s' % workflow_id
            self._raise_exception(RuntimeError, msg)
        return wf

    def _raise_exception(self, exception, msg):
        self.logger.error(msg)
        raise exception(msg)

    def _resolve_workflow_name_to_id(self, workflow_name, id_by_name=None):
        if id_by_name is None:
            wf_list = self.blend.gi.workflows.get_workflows()
            id_by_name = map_name_to_id(wf_list)
        if workflow_name not in id_by_name:
            self._raise_exception(RuntimeError,
                                  'Missing workflow %s' % workflow_name)
        if len(id_by_name[workflow_name]) != 1:
            self._raise_exception(RuntimeError,
                                  'Multiple workflows for %s' % workflow_name)
        return id_by_name[workflow_name][0], id_by_name

    def get_workflows(self):
        self.logger.debug('get_workflows()')
        wf_list = self.blend.gi.workflows.get_workflows()
        id_by_name = map_name_to_id(wf_list)
        return [self.get_workflow(w['id'], id_by_name) for w in wf_list]

    def get_workflow_by_name(self, name):
        self.logger.debug('get_workflows_by_name(%s)' % name)        
        wf_id, id_by_name = self._resolve_workflow_name_to_id(name, None)
        return self.get_workflow(wf_id, id_by_name)

    def get_workflow(self, workflow_id, id_by_name=None):
        """
        Retrieve workflow description from the galaxy server.
        """
        self.logger.debug('get_workflow(%s, %s)' % (workflow_id, id_by_name))
        wf = self._request_workflow(workflow_id)
        root, _ = parse_workflow_name(wf['name'])        
        root_id, _ = self._resolve_workflow_name_to_id(root, id_by_name)
        root_wf = wf if workflow_id == root_id \
                     else self._request_workflow(root_id)
        try: 
            wf_ports = json.loads(root_wf['annotation'])
        except ValueError:
            self.logger.error('Root workflow (%s) has a bad annotation' %
                              root_wf['name'])
            wf_ports = None
        wf_links = self.blend.get_workflow_info(workflow_id)['inputs']
        return Workflow(workflow_id, wf, wf_ports, wf_links)

    def get_libraries(self):
        self.logger.debug('get_libraries()')
        lib_list = self.blend.gi.libraries.get_libraries()
        #id_bu_name = map_name_to_id(lib_list)
        return [self.get_library(lib['id']) for lib in lib_list]
        
    def get_library(self, library_id):
        self.logger.debug('get_library(%s)' % library_id)
        ldesc = self.blend.gi.libraries.show_library(library_id)
        if not ldesc:
            msg = 'Empty reply when retrieving library %s' % library_id
            self._raise_exception(RuntimeError, msg)
        return Library(library_id, ldesc)

    def create_library(self, library_name):
        self.logger.debug('create_library(%s)' % library_name)
        res = self.blend.gi.libraries.create_library(library_name)
        if not res:
            msg = 'Empty reply when creating library %s' % library_name
            self._raise_exception(RuntimeError, msg)
        return self.get_library(res['id'])

    def get_library_for_workflow(self, workflow):
        self.logger.debug('get_library_for_workflow(%s)' % workflow)        
        root_name, _ = parse_workflow_name(workflow.name)
        lib_list = self.blend.gi.libraries.get_libraries()
        id_by_name = map_name_to_id(lib_list)
        if root_name not in id_by_name:
            self._raise_exception(RuntimeError,
                                  'Missing root library %s' % root_name)
        return self.get_library(id_by_name[root_name][0])

    def create_folder(self, library, folder_name, description=None):
        self.logger.debug('create_folder(%s, %s, %s)' 
                          % (library.name, folder_name, description))
        folder_desc = self.blend.gi.libraries.create_folder(library.id,
                           folder_name,
                           '' if description is None else description)
        if not folder_desc:
            msg = ('Empty reply when creating folder %s of %s ' 
                   % (folder_name, library.name))
            self._raise_exception(RuntimeError, msg)
        # for unkown reasons, create_folder returns a list
        return Folder(folder_desc[0], library)

    def link_path_to_library(self, destination, path):
        self.logger.debug('link_path_to_library(%s, "%s")'
                          % (destination, path))
        if type(destination) == Library:
            library_id = destination.id
            folder_id = None
        elif type(destination) == Folder:
            library_id = destination.library.id
            folder_id = destination.id
        else:
            self._raise_exception(RuntimeError, 
                 'Destination %s is of the wrong type' % destination)
        res = self.blend.gi.libraries.upload_from_galaxy_filesystem(
                   library_id, path, folder_id, 'auto', '?',
                   link_data_only='link_to_files')
        # FIXME: add 'upload_from_galaxy_filesystem' to our blend_wrapper
        if isinstance(res, list):
            res = res[0]
        if not res or not isinstance(res, dict):
            msg = ('Bad reply when linking path %s into %s: %r'
                   % (path, destination.name, res))
            self._raise_exception(RuntimeError, msg)
        self.logger.debug('\tlinked to:%s)' % res)
        return LibraryDataset(res)

    def get_histories(self, workflow=None):
        self.logger.debug('get_histories()')
        hlist = self.blend.gi.histories.get_histories()
        if workflow is not None:
            if not isinstance(workflow, Workflow):
                self._raise_exception(ValueError,
                                      '%s is not a Workflow' % workflow)
            hlist = [h for h in hlist
                     if workflow.name == (h['name'].split('+', 1))[0]]
            # we are not using parse_history_name because it expect a
            # precise pattern
        return [self.get_history(h['id']) for h in hlist]
                
    def get_history(self, history_id):
        self.logger.debug('get_history(%s)' % history_id)        
        hdesc = self.blend.gi.histories.show_history(history_id)
        details = self.blend.gi.histories.show_history(history_id, True)
        hdas = [self.blend.gi.histories.show_dataset(history_id, f['id'])
                for f in details]
        hdas = map(HistoryDatasetAssociation, hdas)
        return History(hdesc, hdas)

    def update_history(self, history, name=None, annotation=None):
        self.logger.debug('update_history(%s, %s, %s)' % 
                          (history, name, annotation))
        res = self.blend.gi.histories.update_history(history.id, 
                                                     name=name,
                                                     annotation=annotation)
        if res != 200:
            self._raise_exception(RuntimeError, 
                  'failed history update on %s' % history.id)
        return self.get_history(history.id)
        
    # def history_to_workflow(self, history_desc, derive=False):
    #     history_desc = history_desc if type(history_desc) == dict \
    #                    else json.loads(history_desc)
    #     return self.history_to_workflow_helper(history_desc['id'], derive)

    # def history_to_workflow_helper(self, history_id, derive=False):
    #     history = self.get_history(history_id)
    #     workflow = self.get_workflow(history.workflow_id)
    #     if not self.is_consistent(workflow, history):
    #         raise RuntimeError('history %s is not obtained by running %s' 
    #                            % (history.id, workflow.id))
    #     return workflow

    def register(self, obj):
        "Register a workflow object in galaxy"
        if type(obj) == Workflow:
            if obj.id is None:
                root_name, _ = parse_workflow_name(obj.name)
                obj.name = create_workflow_name(root_name)
                wobj = obj.core.wrapped
                res = self.blend.gi.workflows.import_workflow_json(wobj)
                return self.get_workflow(res['id'])
        else:
            raise ValueError('%s is not a Workflow' %  obj)

    def run_workflow(self, workflow, input_paths, wait=True):
        """
        Run is implemented as the sequence of the following steps.

        0. Check if workflow is alive and if the input_paths are what
           workflow expects.

        1. Create a new subfolder, call it folder, in the workflow class
           galaxy library.

        2. Link input_paths into folder.

        3. Launch workflow, save output in a new history.
        """
        # step 0
        self.logger.debug('run_workflow(%s, %s, %s)' % (workflow, input_paths,
                                                        wait))
        if workflow.is_modified:
            self._raise_exception(ValueError, '%s is tainted.' % workflow.name)
        if not set(input_paths.keys()).issuperset(set(workflow.links.keys())):
            self._raise_exception(RuntimeError,
                'input_paths cannot satisfy %s in_port requests' % workflow)
        self.logger.debug('\tpassed validation')
        # step 1
        library = self.get_library_for_workflow(workflow)
        folder = self.create_folder(library, uuid.uuid4().hex, '')
        self.logger.debug('\tcreated folder %s' % folder)
        # step 2
        data_objs = dict([(name, 
                           self.link_path_to_library(folder, path))
                           for name, path in input_paths.iteritems()])
        self.logger.debug('\tuplinked files %s' % data_objs)        
        # step 3
        input_map = dict([(v, {'id': data_objs[k].id, 'src': data_objs[k].src})
                          for k, v in workflow.links.iteritems()])
        res = self.blend.run_workflow(workflow.id, input_map, 
                       history_name=create_history_name(workflow.name))
        self.logger.debug('\tworkflow %s launched' % workflow.name)
        history_id = res['history']
        if wait:
            finish_state = self.wait(history_id)
            if finish_state == 'ok':
                self.logger.info('Workflow %s has been completed.' % workflow)
                # give some time for post-workflow actions
                time.sleep(5)
            else:
                self.logger.error('Workflow %s has failed.' % workflow)
        return self.get_history(history_id)
                
    @staticmethod
    def in_minutes_seconds(delta):
        """
        Calculate the number of minutes and seconds represented by a
        datetime.timedelta object.

        return: a tuple (minutes, seconds)
        """
        seconds = (delta.microseconds + 
                   (delta.seconds + delta.days * 24 * 3600) * 10**6) / 10**6
        return seconds / 60, seconds % 60

    def wait(self, history_id):
        """
        Wait until the history is no longer running.
        Returns the final state string.
        """
        # taken almost verbatim from automator.handler.galaxy_workflow
        # history states:
        #  * queued
        #  * running
        #  * ok
        #  * error
        self.logger.info("Waiting for history %s", history_id)
        sleep_interval_sec = 10 # seconds
        # we'll log no more frequently than log_interval_sec seconds
        log_interval_sec = 60 # seconds
        # calculate the number of iterations suitable for the desired
        # log interval length
        log_interval = int(math.ceil(log_interval_sec / 
                                     float(sleep_interval_sec)))
        iteration_count = 0
        wait_start = datetime.now()
        last_status = None
        while True:
            iteration_count += 1
            status_info = self.blend.get_history_status(history_id)
            # XXX: undoubtedly we'll have to implement retries here to
            # avoid considering a workflow as failed when maybe Galaxy
            # was only temporarily unavailable or some other transient
            # problem kept us from getting the reply as expected.
            if (status_info['state'] not in ('queued', 'running') 
                and last_status == status_info):
                # Since galaxy reports a state of 'ok' before it has
                # finished adding all datasets to the history, we wait
                # until the state is 'ok' AND we haven't seen any
                # changes to the history for the last two queries.
                return status_info['state']
            last_status = status_info
            # log some stuff, then sleep
            if iteration_count % log_interval == 0:
                wait_so_far = datetime.now() - wait_start
                self.logger.debug(
                    "Waiting for history %s since %s (%s). Waiting %d s more",
                     history_id,
                     wait_start.strftime("%Y-%m-%d %H:%M:%S"),
                     "%d'%02d\"" % self.in_minutes_seconds(wait_so_far),
                     sleep_interval_sec)
            time.sleep(sleep_interval_sec)
    
