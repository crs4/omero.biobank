#!/usr/bin/env python

import argparse, daemon, json, os, sys, datetime
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
                labels = (("{0} # {1} # {2} # {3}", r.sample.sample.label,r.sample.sample.action.context.label,r.mimetype,datetime.datetime.fromtimestamp(int(r.sample.creationDate)).strftime('%Y-%m-%d %H:%M:%S')) for r in res)
                values = (('{0}', r.omero_id) for r in res)
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
        return kb.get_objects(kb.Study)

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
        return kb.get_objects(kb.SNPMarkersSet)

    @wrap_record_id
    def get_data_collections(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        return kb.get_objects(kb.DataCollection)

    @wrap_record_id
    def get_vessels_collections(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        return kb.get_objects(kb.VesselsCollection)

    @wrap_barcode
    def get_titer_plates(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        plates = kb.get_objects(kb.TiterPlate)
        return [pl for pl in plates if pl.barcode and type(pl) == kb.TiterPlate]

    @wrap_enum
    def get_vessel_status(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        return kb.get_objects(kb.VesselStatus)

    @wrap_enum
    def get_vessel_content(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        return kb.get_objects(kb.VesselContent)

    @wrap_enum
    def get_data_sample_status(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        return kb.get_objects(kb.DataSampleStatus)

    @wrap_record_id
    def get_hardware_devices(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        return kb.get_objects(kb.HardwareDevice)

    @wrap_record_id
    def get_software_devices(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        return kb.get_objects(kb.SoftwareProgram)

    @wrap_record_id
    def get_devices(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        return kb.get_objects(kb.Device)

    @wrap_enum
    def get_illumina_bead_chip_assay_types(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        return kb.get_objects(kb.IlluminaBeadChipAssayType)

    @wrap_enum
    def get_illumina_array_of_arrays_types(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        return kb.get_objects(kb.IlluminaArrayOfArraysType)

    @wrap_enum
    def get_illumina_array_of_arrays_classes(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        return kb.get_objects(kb.IlluminaArrayOfArraysClass)

    @wrap_enum
    def get_illumina_array_of_arrays_assay_types(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        return kb.get_objects(kb.IlluminaArrayOfArraysAssayType)

    @wrap_enum
    def get_action_categories(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        return kb.get_objects(kb.ActionCategory)

    @wrap_record_id
    def get_scanners(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        return kb.get_objects(kb.Scanner)

    @wrap_enum
    def get_container_status(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        return kb.get_objects(kb.ContainerStatus)

    @wrap_label_with_unique_constraint
    def get_tubes(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        return kb.get_objects(kb.Tube)

    @wrap_data_objects
    def get_data_objects(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        result = list()
        tubes = kb.get_objects(kb.Tube)
        for tube in tubes:
            try:
                datasamples = kb.get_seq_data_samples_by_tube(tube)
            except:
                datasamples = list()
            for data_sample in datasamples:
                try:
                    data_objects = kb.get_data_objects(data_sample)
                except:
                    data_objects = list()
                for dobj in data_objects:
                    result.append(dobj)
        return result

    def start_service(self, host, port, logfile, debug=False):
        log = open(logfile, 'a')
        with daemon.DaemonContext(stderr=log):
            run(host=host, port=port, debug=debug)


def get_parser():
    parser = argparse.ArgumentParser('Run the Galaxy Menus HTTP server')
    parser.add_argument('--host', type=str, default='127.0.0.1',
                        help='web service binding host')
    parser.add_argument('--port', type=int, default='8080',
                        help='web service binding port')
    parser.add_argument('--debug', action='store_true',
                        help='Enable web server DEBUG mode')
    parser.add_argument('--pid-file', type=str, 
                        help='PID file for the dbservice daemon')
    parser.add_argument('--log-file', type=str, 
                        help='log file for the dbservice daemon',
                        default='/tmp/galaxy_menus_service.log')
    return parser


def check_pid(pid_file):
    if os.path.isfile(pid_file):
        sys.exit(0)


def create_pid(pid_file):
    pid = str(os.getpid())
    with open(pid_file, 'w') as ofile:
        ofile.write(pid)


def destroy_pid(pid_file):
    os.remove(pid_file)


def main(argv):
    parser = get_parser()
    args = parser.parse_args(argv)
    gms = GalaxyMenusService()
    if args.pid_file:
        print "qui"
        check_pid(args.pid_file)
        create_pid(args.pid_file)
    gms.start_service(args.host, args.port, args.log_file, args.debug)
    if args.pid_file:
        destroy_pid(args.pid_file)


if __name__ == '__main__':
    main(sys.argv[1:])
