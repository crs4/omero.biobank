from bl.vl.kb.galaxy.galaxy_core_instance import GalaxyInstance\
                                                 as CoreGalaxyInstance
from bl.vl.kb.galaxy.galaxy_core_instance import parse_workflow_name

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
            return input_paths
        else:
            self._raise_exception(ValueError,
                    'kb type %s not supported' % in_port['type'])


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
                       history, operator, description, saved):
        self.logger.debug(('_create_action('+ '%s,'*7+')')
                          % (study, workflow, input_object,
                             history, operator, description, saved))
        root_name, _ = parse_workflow_name(workflow.name)
        device = self.kb.get_device(root_name)
        if not device:
            self._raise_exception(RuntimeError,
                                  'missing a device for %s' % workflow.name)
        conf = {'label': history.name, 'conf' : history.to_json()}
        action_setup = self.kb.factory.create(self.kb.ActionSetup, conf)
        saved.append(action_setup.save())
        self.logger.info('created action_setup %s' % action_setup.label)        
        conf = {'setup': action_setup, 'device': device,
                'actionCategory': self.kb.ActionCategory.CREATION,
                'operator': operator, 'context': study,
                'description': description, 'target': input_object}
        action_klass = self._get_action_klass(input_object)
        action = self.kb.factory.create(action_klass, conf)
        saved.append(action.save())
        self.logger.info('created action %s' % action)     
        action.unload() # we do not need the details.          
        return action

    def _create_data_collection(self, label, action, fields, datasets, saved):
        data_samples = []
        for name, desc in fields.iteritems():
            conf = {'label': '%s.%s' % (label, name),
                    'status': self.kb.DataSampleStatus.USABLE,
                    'action': action}
            d_sample = self.kb.factory.create(self.kb.DataSample, conf).save()
            saved.append(d_sample)
            self.logger.debug('created DataSample %s' % d_sample)
            conf = {'sample': d_sample,
                    'path': datasets[desc['port']['name']].file_name,
                    'mimetype': desc['mimetype'],
                    'sha1': 'fake-sha1',
                    'size': datasets[desc['port']['name']].file_size}
            d_object = self.kb.factory.create(self.kb.DataObject, conf).save()
            saved.append(d_object)
            self.logger.debug('created DataObject %s' % d_object)
            data_samples.append(d_sample)

        return
        #     conf = {'dataSample': data_sample,
        #             'dataCollection': data_collection, 
        #             'role': name}
        #     self.logger.debug('creating DataCollectionItem conf:%s' 
        #                       % conf)
        #     data_sample.reload() # FIXME this is needed to keep hibernate happy
        #     dci = self.kb.factory.create(self.kb.DataCollectionItem, conf)
        #     data_sample.unload() # FIXME this is needed to keep hibernate happy
        #     saved.append(dci.save())
        #     dci.unload()            
        #     self.logger.debug('created DataCollectionItem %s' % dci)
        # self.logger.info('creating data_collection %s.' % label)        
        # conf = {'label': label, 'action': action}
        # data_collection = self.kb.factory.create(self.kb.DataCollection, conf)
        # data_collection = data_collection.save()
        # saved.append(data_collection)
        # self.logger.debug('created DataCollection %s' % data_collection)        
        # data_collection.unload()
                    
    def _create_output(self, label, port, datasets, action, saved):
        self.logger.info('creating output object %s.' % label)
        if not set([v['port']['name'] for v in port['fields'].values()])\
                .issubset(set(datasets.keys())):
            self._raise_exception(RuntimeError,
                                  "cannot find data paths for %s" % label)
        klass = getattr(self.kb, port['type'])
        if klass == self.kb.DataCollection:
            self._create_data_collection(label, action, port['fields'], 
                                         datasets, saved)
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
        datasets = dict([(d.name, d) for d in history.datasets])
        saved = []
        action = self._create_action(study, workflow, input_object, 
                                     history, operator, description, saved)
        saved.append(action)
        for name, port in workflow.ports['outputs'].iteritems():
            self._create_output(create_object_label(history.name, name),
                                port, datasets, action, saved)
        saved.reverse()
        for o in saved:
            self.logger.debug('deleting object: %s' % o)            
            self.kb.delete(o)
            
        
        # try:
        #     action = self._create_action(study, workflow, input_object, 
        #                                  history, operator, description, saved)
        #     saved.append(action)
        #     for name, port in workflow.ports['outputs']:
        #         self._create_output(create_object_label(history.name, name),
        #                             port, datasets, action, saved)
        # except StandardError as e:
        #     self.logger.error('Got an exception: %s. Started cleanup.' % e)
        #     saved.reverse()
        #     for o in saved:
        #         self.logger.debug('deleting object: %s' % o)            
        #         self.kb.delete(o)
        #     self.logger.error('Done with cleanup. Raising %s again.' % e)
        #     raise e
        

        

def cleanup(kb):
    data_objects = kb.get_objects(kb.DataObject)
    for do in data_objects[3:]:
        kb.delete(do)
    data_samples = kb.get_objects(kb.DataSample)
    for ds in data_samples[3:]:
        kb.delete(ds)
    actions = kb.get_objects(kb.Action)
    for a in actions[1:]:
        kb.delete(a)
    action_setups = kb.get_objects(kb.ActionSetup)
    for a in action_setups[1:]:
        kb.delete(a)
    
