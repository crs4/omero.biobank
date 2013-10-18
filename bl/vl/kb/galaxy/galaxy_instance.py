from bl.vl.kb.galaxy.galaxy_core_instance import GalaxyInstance\
                                                 as CoreGalaxyInstance
from bl.vl.kb.galaxy.galaxy_core_instance import parse_workflow_name
from bl.vl.kb.galaxy import Workflow, History

import json


def create_object_label(history_name, port_name):
    assert history_name.find('#') == -1
    return '%s#%s' % (history_name, port_name)


def parse_object_label(label):
    parts = label.split('#', 1)
    assert len(parts) == 2
    return parts


class GalaxyInstance(CoreGalaxyInstance):

    def __init__(self, kb, url, api_key, metastore=None):
        self.logger = kb.logger.getChild('galaxy_instance')
        super(GalaxyInstance, self).__init__(url, api_key, logger=self.logger)
        self.kb = kb
        self.metastore = metastore
        self.to_be_killed = []

    def _pack_history_annotation(self, study, workflow, input_object):
        return json.dumps({
            'study' : {'label': study.label, 'id': study.id},
            'workflow': {'name' : workflow.name, 'id' : workflow.id},
            'input_object': {'type' : input_object.get_ome_table(),
                             'id'  : input_object.id,
                             'biobank_id': 'unique-biobank-instance-id',
                             }
            })

    def _unpack_history_annotation(self, annotation):
        try:
            struct = json.loads(annotation)
        except ValueError as e:
            raise RuntimeError('bad annotation: %s' % e)
        workflow = self.get_workflow(struct['workflow']['id'])
        if workflow.name != struct['workflow']['name']:
            self._raise_exception(RuntimeError,
                  'Bad annotation(%s): inconsistent workflow name and id' %
                  annotation)
        try:
            study = self.kb.get_by_vid(self.kb.Study,
                                       struct['study']['id'])
            klass = getattr(self.kb, struct['input_object']['type'])
            vid = struct['input_object']['id']
            input_object = self.kb.get_by_vid(klass, vid)
        except ValueError as e:
            self._raise_exception(RuntimeError,'bad annotation(%s): %s' %
                                  (annotation, e))
        except KeyError as e:
            self._raise_exception(RuntimeError,'bad annotation(%s): %s' %
                                  (annotation, e))
        return study, workflow, input_object

    def is_biobank_compatible(self, obj):
        """
        True if the galaxy object obj could be tracked by omero.biobank.

        FIXME: we are currently using heuristics to speed up things.
        """
        if isinstance(obj, Workflow):
            return obj.ports is not None
        if isinstance(obj, History):
            return obj.annotation.find('biobank_id') > -1
        self._raise_exception(ValueError,
                    'cannot check biobank compatibility of %s' % obj)

    def is_mapped_in_biobank(self, obj):
        """
        True if a representation of obj is stored in omero.biobank.
        """
        if isinstance(obj, Workflow):
            root_name, _ = parse_workflow_name(obj.name)
            device = self.kb.get_device(root_name)
            return device is not None
        if isinstance(obj, History):
            action = self.kb.get_action_setup(obj.name)
            return action is not None
        self._raise_exception(ValueError,
                    'cannot check if %s is mapped to biobank' % obj)

    def get_history(self, history_id=None, action=None):
        if history_id is None and action is None:
            self._raise_exception(
                ValueError, "specify either history_id or action"
                )
        if not history_id:
            history_id = json.loads(action.setup.conf)['id']
        return super(GalaxyInstance, self).get_history(history_id)

    def get_workflows(self, device=None):
        ws = super(GalaxyInstance, self).get_workflows()
        if device is None:
            return ws
        if not isinstance(device, self.kb.Device):
            self._raise_exception(ValueError,
                  '%s is not a kb.Device' % device)
        results = []
        for w in ws:
            root_name, _ = parse_workflow_name(w.name)
            if root_name == device.label:
                results.append(w)
        return results

    def get_initial_conditions(self, history):
        """
        Return study, workflow, input_object used to obtain history.
        """
        if not isinstance(history, History):
            self._raise_exception(ValueError,
                    '%s is not a History' % history)
        if not self.is_biobank_compatible(history):
            self._raise_exception(ValueError,
                    '%s is not biobank compatible' % history)
        return self._unpack_history_annotation(history.annotation)

    # DEPRECATED
    def get_input_object(self, history):
        study, workflow, input_object = self.get_initial_conditions(history)
        return input_object

    def _get_data_object_path(self, sample, mimetype):
        self.logger.debug('_get_data_object_path(%s, %s)' %
                          (sample, mimetype))
        dos = self.kb.get_data_objects(sample)
        if not dos:
            self._raise_exception(RuntimeError,
                  "sample %s has no attached DataObject" % sample)
        for do in dos:
            do.reload()
            self.logger.debug('\tdo.path: <%s>'% do.path)
            self.logger.debug('\tdo.mimetype: <%s>'% do.mimetype)
            if do.mimetype == mimetype:
                return do.path
        else:
            self._raise_exception(RuntimeError,
                  "no DataObject of mimetype %s attached to sample %s" %
                  (mimetype, sample))

    def _get_input_paths(self, input_object, in_port):
        """
        map input_object to in_port.

        For each field of the input port, find a corresponding data
        path. If input_object is a DataCollection, it will map each
        field field to a DataCollectionItem that has role equal to the
        field name. Otherwise, it will interpret the field name as an
        attribute of the input_object.
        """
        input_paths = {}
        if input_object.get_ome_table() != in_port['type']:
            self._raise_exception(ValueError,
                'expected input of type %s got %s.' %
                (in_port['type'], input_object.get_ome_table()))
        if in_port['type'] == 'DataCollection':
            fields = in_port['fields']
            input_paths = dict([
                (k, self._get_data_object_path(ds, fields[k]['mimetype']))
                for k, ds in [(i.role, i.dataSample)
                             for i in self.kb.get_data_collection_items(
                                               input_object)]])
        elif in_port['type'] == 'DataSample':
            fields = in_port['fields']
            if len(fields) != 1:
                self._raise_exception(ValueError, 'wrong number of fields')
            k = fields.iterkeys().next()
            input_paths = {
                k: self._get_data_object_path(
                    input_object, fields[k]['mimetype']
                    )}
        else:
            self._raise_exception(ValueError,
                    'kb type %s not supported' % in_port['type'])
        return input_paths

    def run_workflow(self, study, workflow, input_object, wait=True):
        """
        In the context of study, run 'workflow' with 'input_object'.
        If 'wait' (default behavior) then wait for the results, else
        put request in queue and exit.  It will return the corresponding
        History object.
        """
        # note that we are ignoring any input port past the first.
        in_port = workflow.ports['inputs'].values()[0]
        input_paths = self._get_input_paths(input_object, in_port)
        history = super(GalaxyInstance, self).run_workflow(workflow,
                                                           input_paths, wait)
        annotation = self._pack_history_annotation(study, workflow,
                                                   input_object)
        return self.update_history(history, annotation=annotation)

    def _get_action_klass(self, input_object):
        self.logger.debug('_get_action_klass(%s).' % input_object)
        if isinstance(input_object, self.kb.DataSample):
            return self.kb.ActionOnDataSample
        elif isinstance(input_object, self.kb.DataCollectionItem):
            return self.kb.ActionOnDataCollectionItem
        elif isinstance(input_object, self.kb.VLCollection):
            return self.kb.ActionOnCollection
        else:
            self._raise_exception(RuntimeError,
                    'bad target class: %s' % input_object.get_ome_table())

    def _create_action(self, study, workflow, input_object,
                       history, operator, description):
        self.logger.debug(('_create_action('+ '%s,'*6+')')
                          % (study, workflow, input_object,
                             history, operator, description))
        root_name, _ = parse_workflow_name(workflow.name)
        device = self.kb.get_device(root_name)
        if not device:
            self._raise_exception(RuntimeError,
                                  'missing a device for %s' % workflow.name)
        conf = {'label': history.name, 'conf' : history.to_json()}
        action_setup = self.kb.factory.create(self.kb.ActionSetup, conf)
        self.to_be_killed.append(action_setup.save())
        self.logger.debug('created action_setup %s' % action_setup.label)
        conf = {'setup': action_setup, 'device': device,
                'actionCategory': self.kb.ActionCategory.CREATION,
                'operator': operator, 'context': study,
                'description': description, 'target': input_object}
        action_klass = self._get_action_klass(input_object)
        action = self.kb.factory.create(action_klass, conf)
        self.to_be_killed.append(action.save())
        self.logger.debug('created action %s' % action)
        action.unload() # we do not need the details.
        return action

    # FIXME: refactor to use _create_data_sample
    def _create_data_collection(self, label, action, fields, datasets):
        self.logger.info('creating data_collection %s.' % label)
        conf = {'label': label, 'action': action}
        data_collection = self.kb.factory.create(self.kb.DataCollection, conf)
        self.to_be_killed.append(data_collection.save())
        self.logger.debug('created DataCollection %s' % data_collection)
        for name, desc in fields.iteritems():
            self.logger.debug('Iteration for role %s' % name)
            self.logger.debug('action.is_loaded: %s' % action.is_loaded())
            if action.is_loaded(): action.unload()
            conf = {'label': '%s.%s' % (label, name),
                    'status': self.kb.DataSampleStatus.USABLE,
                    'action': action}
            d_sample = self.kb.factory.create(self.kb.DataSample, conf)
            self.to_be_killed.append(d_sample.save())
            self.logger.debug('created DataSample %s' % d_sample)
            self.logger.debug('action.is_loaded: %s' % action.is_loaded())
            if action.is_loaded(): action.unload()
            conf = {'sample': d_sample,
                    'path': datasets[desc['port']['name']].file_name,
                    'mimetype': desc['mimetype'],
                    'sha1': 'fake-sha1',
                    'size': datasets[desc['port']['name']].file_size}
            d_object = self.kb.factory.create(self.kb.DataObject, conf)
            self.to_be_killed.append(d_object.save())
            self.logger.debug('created DataObject %s' % d_object)
            self.logger.debug('action.is_loaded: %s' % action.is_loaded())
            if action.is_loaded(): action.unload()
            conf = {'dataSample': d_sample,
                    'dataCollection': data_collection,
                    'role': name}
            dci = self.kb.factory.create(self.kb.DataCollectionItem, conf)
            self.to_be_killed.append(dci.save())
            self.logger.debug('created DataCollectionItem: %s' % dci)
        self.logger.debug('filled DataCollection %s' % data_collection)

    def _create_data_sample(self, label, action, fields, datasets):
        self.logger.info('creating data_sample %s.' % label)
        conf = {'label': label, 'action': action}
        name, desc = fields.iteritems().next()
        self.logger.debug('Iteration for role %s' % name)
        self.logger.debug('action.is_loaded: %s' % action.is_loaded())
        if action.is_loaded():
            action.unload()
        conf = {
            'label': '%s.%s' % (label, name),
            'status': self.kb.DataSampleStatus.USABLE,
            'action': action,
            }
        d_sample = self.kb.factory.create(self.kb.DataSample, conf)
        self.to_be_killed.append(d_sample.save())
        self.logger.debug('created DataSample %s' % d_sample)
        self.logger.debug('action.is_loaded: %s' % action.is_loaded())
        if action.is_loaded():
            action.unload()
        conf = {
            'sample': d_sample,
            'path': datasets[desc['port']['name']].file_name,
            'mimetype': desc['mimetype'],
            'sha1': 'fake-sha1',
            'size': datasets[desc['port']['name']].file_size,
            }
        d_object = self.kb.factory.create(self.kb.DataObject, conf)
        self.to_be_killed.append(d_object.save())
        self.logger.debug('created DataObject %s' % d_object)

    def _create_output(self, label, port, datasets, action):
        self.logger.info('creating output object %s.' % label)
        if not set([v['port']['name'] for v in port['fields'].values()])\
                .issubset(set(datasets.keys())):
            self._raise_exception(RuntimeError,
                                  "cannot find data paths for %s" % label)
        klass = getattr(self.kb, port['type'])
        if klass == self.kb.DataCollection:
            self._create_data_collection(label, action, port['fields'],
                                         datasets)
        elif klass == self.kb.DataSample:
            self._create_data_sample(label, action, port['fields'], datasets)
        else:
            self._raise_exception(RuntimeError,
                                  'cannot handle port type %s' % port['type'])

    def save(self, history, operator='galaxy', description=''):
        """
        Save history results in omero.biobank.
        """
        self.logger.info('saving history %s results.' % history.name)
        study, workflow, input_object = self._unpack_history_annotation(
                                              history.annotation)
        self.logger.info('History %s was run' % history.name)
        self.logger.info('\tin the context of %s' % study.label)
        self.logger.info('\twith workflow %s' % workflow.name)
        self.logger.info('\ton input %s(%s)' % (input_object.get_ome_table(),
                                                input_object.id))
        datasets = dict([(d.name, d) for d in history.datasets])
        self.to_be_killed = []
        try:
            action = self._create_action(study, workflow, input_object,
                                         history, operator, description)
            for name, port in workflow.ports['outputs'].iteritems():
                self._create_output(create_object_label(history.name, name),
                                    port, datasets, action)
        except StandardError as e:
            self.logger.error('Got an exception: %s. Started cleanup.' % e)
            while self.to_be_killed:
                o = self.to_be_killed.pop()
                self.logger.debug('deleting object: %s' % o)
                self.kb.delete(o)
            raise e
        finally:
            self.to_be_killed = []
