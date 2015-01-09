#!/usr/bin/env python

import argparse, daemon, json, sys, datetime
import daemon.pidlockfile
from functools import wraps
from itertools import izip

from bottle import post, get, run, response, request

from bl.vl.kb import KnowledgeBase as KB

class GalaxyMenusService(object):

    def __init__(self):
        # Web service methods
        post('/galaxy/get/studies')(self.get_studies)
        post('/galaxy/get/map_vid_sources')(self.get_map_vid_sources)
        post('/galaxy/get/snp_marker_sets')(self.get_marker_sets)
        post('/galaxy/get/data_collections')(self.get_data_collections)
        post('/galaxy/get/vessels_collections')(self.get_vessels_collections)
        post('/galaxy/get/titer_plates')(self.get_titer_plates)
        post('/galaxy/get/vessel_status')(self.get_vessel_status)
        post('/galaxy/get/vessel_content')(self.get_vessel_content)
        post('/galaxy/get/data_sample_status')(self.get_data_sample_status)
        post('/galaxy/get/hardware_devices')(self.get_hardware_devices)
        post('/galaxy/get/software_devices')(self.get_software_devices)
        post('/galaxy/get/devices')(self.get_devices)
        post('/galaxy/get/illumina_bead_chip_assay_types')(self.get_illumina_bead_chip_assay_types)
        post('/galaxy/get/illumina_array_of_arrays_types')(self.get_illumina_array_of_arrays_types)
        post('/galaxy/get/illumina_array_of_arrays_classes')(self.get_illumina_array_of_arrays_classes)
        post('/galaxy/get/illumina_array_of_arrays_assay_types')(self.get_illumina_array_of_arrays_assay_types)
        post('/galaxy/get/action_categories')(self.get_action_categories)
        post('/galaxy/get/scanners')(self.get_scanners)
        post('/galaxy/get/container_status')(self.get_container_status)
        post('/galaxy/get/tubes')(self.get_tubes)
        post('/galaxy/get/data_objects')(self.get_data_objects)
        # check status
        post('/check/status')(self.test_server)
        get('/check/status')(self.test_server)

    def _get_knowledge_base(self, params):
        return KB(driver='omero')(params.get('ome_host'), params.get('ome_user'),
                                  params.get('ome_passwd'))

    def _success(self, body, return_code=200):
        response.content_type = 'application/json'
        response.status = return_code
        return json.dumps({'result': body})

    def _build_response_body(self, value_mappings, label_mappings):
        response_body = [
            {
                'value': str.format(*value),
                'label': str.format(*label),
                'selected': False
            } for value, label in izip(value_mappings, label_mappings)
        ]
        return response_body

    def wrap_label(f):
        @wraps(f)
        def wrapper(inst, *args, **kwargs):
            res = f(inst, *args, **kwargs)
            if len(res) == 0:
                return None
            else:
                values = (('{0}', r.label) for r in res)
                labels = (('{0}', r.label) for r in res)
                response_body = inst._build_response_body(values, labels)
                response_body[0]['selected'] = True
                return inst._success(response_body)
        return wrapper

    def wrap_label_with_unique_constraint(f):
        @wraps(f)
        def wrapper(inst, *args, **kwargs):
            res = f(inst, *args, **kwargs)
            if len(res) == 0:
                return None
            else:
                values = (('{0}', r.label.split('_',1)[-1]) for r in res)
                labels = (('{0}', r.label.split('_',1)[-1]) for r in res)
                response_body = inst._build_response_body(values, labels)
                response_body[0]['selected'] = True
                return inst._success(response_body)
        return wrapper

    def wrap_label_and_description(f):
        @wraps(f)
        def wrapper(inst, *args, **kwargs):
            res = f(inst, *args, **kwargs)
            if len(res) == 0:
                return None
            else:
                labels = (('{0} ({1})', r.label,
                          r.description or 'No description') for r in res)
                values = (('{0}', r.label) for r in res)
                response_body = inst._build_response_body(values, labels)
                response_body[0]['selected'] = True
                return inst._success(response_body)
        return wrapper

    def wrap_value(f):
        @wraps(f)
        def wrapper(inst, *args, **kwargs):
            res = f(inst, *args, **kwargs)
            if len(res) == 0:
                return None
            else:
                labels = (('{0}', r) for r in res)
                values = (('{0}', r) for r in res)
                response_body = inst._build_response_body(values, labels)
                response_body[0]['selected'] = True
                return inst._success(response_body)
        return wrapper

    def wrap_record_id(f):
        @wraps(f)
        def wrapper(inst, *args, **kwargs):
            res = f(inst, *args, **kwargs)
            if len(res) == 0:
                return None
            else:
                labels = (('{0}', r.label) for r in res)
                values = (('{0}', r.id) for r in res)
                response_body = inst._build_response_body(values, labels)
                response_body[0]['selected'] = True
                return inst._success(response_body)
        return wrapper

    def wrap_barcode(f):
        @wraps(f)
        def wrapper(inst, *args, **kwargs):
            res = f(inst, *args, **kwargs)
            if len(res) == 0:
                return None
            else:
                labels = (('{0}', r.label) for r in res)
                values = (('{0}', r.barcode) for r in res)
                response_body = inst._build_response_body(values, labels)
                response_body[0]['selected'] = True
                return inst._success(response_body)
        return wrapper

    def wrap_enum(f):
        @wraps(f)
        def wrapper(inst, *args, **kwargs):
            res = f(inst, *args, **kwargs)
            if len(res) == 0:
                return None
            else:
                labels = (('{0}', r.enum_label()) for r in res)
                values = (('{0}', r.omero_id) for r in res)
                response_body = inst._build_response_body(values, labels)
                response_body[0]['selected'] = True
                return inst._success(response_body)
        return wrapper

    def wrap_data_objects(f):
        @wraps(f)
        def wrapper(inst, *args, **kwargs):
            res = f(inst, *args, **kwargs)
            if len(res) == 0:
                return None
            else:
                labels = list()
                values = list()
                for r in res:
                    if hasattr(r.sample,'sample'):
                        if r.sample.sample.label == 'TRAINING_tube_1' : label = 'FATHER'
                        elif r.sample.sample.label == 'TRAINING_tube_2' : label = 'PROBAND'
                        elif r.sample.sample.label == 'TRAINING_tube_3' : label = 'MOTHER'
                        else : continue
                        labels.append(("{0} [{1}] - {2}",label,datetime.datetime.fromtimestamp(int(r.sample.creationDate)).strftime('%Y-%m-%d %H:%M:%S'),r.mimetype))
                        values.append(('{0}',r.omero_id))
                    if hasattr(r.sample,'referenceGenome'):
                        labels.append(("{0} [{1}] - {2}",r.sample.label,datetime.datetime.fromtimestamp(int(r.sample.creationDate)).strftime('%Y-%m-%d %H:%M:%S'),r.mimetype))
                        values.append(('{0}',r.omero_id))
                response_body = inst._build_response_body(values, labels)
                response_body[0]['selected'] = True
                return inst._success(response_body)
        return wrapper

    def test_server(self):
        return 'Server running'

    @wrap_label_and_description
    def get_studies(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        res = kb.get_objects(kb.Study)
        kb.disconnect()
        return res

    @wrap_value
    def get_map_vid_sources(self):
        from bl.vl.app.kb_query.map_vid import MapVIDApp
        sources = MapVIDApp.SUPPORTED_SOURCE_TYPES
        if sources:
            return sources
        else:
            return []

    @wrap_record_id
    def get_marker_sets(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        res = kb.get_objects(kb.SNPMarkersSet)
        kb.disconnect()
        return res

    @wrap_record_id
    def get_data_collections(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        res = kb.get_objects(kb.DataCollection)
        kb.disconnect()
        return res

    @wrap_record_id
    def get_vessels_collections(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        res = kb.get_objects(kb.VesselsCollection)
        kb.disconnect()
        return res

    @wrap_barcode
    def get_titer_plates(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        plates = kb.get_objects(kb.TiterPlate)
        res = [pl for pl in plates if pl.barcode and type(pl) == kb.TiterPlate]
        kb.disconnect()
        return res

    @wrap_value
    def get_vessel_status(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        res = [vs.enum_label() for vs in kb.get_objects(kb.VesselStatus)]
        kb.disconnect()
        return res

    @wrap_enum
    def get_vessel_content(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        res = kb.get_objects(kb.VesselContent)
        kb.disconnect()
        return res

    @wrap_enum
    def get_data_sample_status(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        res = kb.get_objects(kb.DataSampleStatus)
        kb.disconnect()
        return res

    @wrap_record_id
    def get_hardware_devices(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        res = kb.get_objects(kb.HardwareDevice)
        kb.disconnect()
        return res

    @wrap_record_id
    def get_software_devices(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        res = kb.get_objects(kb.SoftwareProgram)
        kb.disconnect()
        return res

    @wrap_record_id
    def get_devices(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        res = kb.get_objects(kb.Device)
        kb.disconnect()
        return res

    @wrap_enum
    def get_illumina_bead_chip_assay_types(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        res = kb.get_objects(kb.IlluminaBeadChipAssayType)
        kb.disconnect()
        return res

    @wrap_enum
    def get_illumina_array_of_arrays_types(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        res = kb.get_objects(kb.IlluminaArrayOfArraysType)
        kb.disconnect()
        return res

    @wrap_enum
    def get_illumina_array_of_arrays_classes(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        res = kb.get_objects(kb.IlluminaArrayOfArraysClass)
        kb.disconnect()
        return res

    @wrap_enum
    def get_illumina_array_of_arrays_assay_types(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        res = kb.get_objects(kb.IlluminaArrayOfArraysAssayType)
        kb.disconnect()
        return res

    @wrap_enum
    def get_action_categories(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        res = kb.get_objects(kb.ActionCategory)
        kb.disconnect()
        return res

    @wrap_record_id
    def get_scanners(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        res = kb.get_objects(kb.Scanner)
        kb.disconnect()
        return res

    @wrap_enum
    def get_container_status(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        res = kb.get_objects(kb.ContainerStatus)
        kb.disconnect()
        return res

    @wrap_label_with_unique_constraint
    def get_tubes(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        res = kb.get_objects(kb.Tube)
        kb.disconnect()
        return res

    @wrap_data_objects
    def get_data_objects(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        result = list()
        datasamples = kb.get_objects(kb.DataSample)
        for ds in datasamples:
            if isinstance(ds, kb.SeqDataSample):
                if isinstance(ds.sample, kb.Tube):
                    data_objects = kb.get_data_objects(ds)
                    for dobj in data_objects:
                        if not dobj.mimetype.endswith('pdf'): result.append(dobj)
            if isinstance(ds, kb.GenomeVariationsDataSample):
                if isinstance(ds.referenceGenome, kb.ReferenceGenome):
                    data_objects = kb.get_data_objects(ds)
                    for dobj in data_objects:
                        result.append(dobj)
            if isinstance(ds, kb.AlignedSeqDataSample):
                if isinstance(ds.referenceGenome, kb.ReferenceGenome) and isinstance(ds.sample, kb.Tube):
                    data_objects = kb.get_data_objects(ds)
                    for dobj in data_objects:
                        result.append(dobj)
            kb.disconnect()
            return result


    def start_service(self, host, port, logfile, pidfile, server, debug=False):
        log = open(logfile, 'a')
        pid = daemon.pidlockfile.PIDLockFile(pidfile)
        with daemon.DaemonContext(stderr=log, pidfile=pid):
            run(host=host, port=port, server=server, debug=debug)


def get_parser():
    parser = argparse.ArgumentParser('Run the Galaxy Menus HTTP server')
    parser.add_argument('--host', type=str, default='127.0.0.1',
                        help='web service binding host')
    parser.add_argument('--port', type=int, default='8080',
                        help='web service binding port')
    parser.add_argument('--server', type=str, default='wsgiref',
                        help='server library (use paste for multi-threaded backend)')
    parser.add_argument('--debug', action='store_true',
                        help='Enable web server DEBUG mode')
    parser.add_argument('--pid-file', type=str, 
                        help='PID file for the service daemon',
                        default='/tmp/galaxy_menus_service.pid')
    parser.add_argument('--log-file', type=str, 
                        help='log file for the service daemon',
                        default='/tmp/galaxy_menus_service.log')
    return parser


def main(argv):
    parser = get_parser()
    args = parser.parse_args(argv)
    gms = GalaxyMenusService()
   
    gms.start_service(args.host, args.port, args.log_file, args.pid_file, 
                      args.server, args.debug)
   

if __name__ == '__main__':
    main(sys.argv[1:])
