import sys, argparse, json, os
from functools import wraps

from bottle import post, get, run, response, request

from bl.vl.kb import KnowledgeBase as KB

class GalaxyMenusService(object):

    def __init__(self):
        # Web service methods
        post('/galaxy/get/studies')(self.get_studies)
        # check status
        post('/check/status')(self.test_server)
        get('/check/status')(self.test_server)

    def _get_knowledge_base(self, params):
        return KB(driver='omero')(params.get('ome_host'), params.get('ome_user'),
                                  params.get('ome_passwd'))

    def _success(self, body, return_code=200):
        response.content_type = 'application/json'
        response.status = return_code
        return json.dumps(body)

    def wrap_results_label(f):
        @wraps(f)
        def wrapper(inst, *args, **kwargs):
            res = f(inst, *args, **kwargs)
            response_body = [
                {
                    'value': r.label,
                    'label': r.label,
                    'selected': False
                } for r in res
            ]
            response_body[0]['selected'] = True
            return inst._success(response_body)
        return wrapper

    def test_server(self):
        return 'Server running'

    @wrap_results_label
    def get_studies(self):
        params = request.forms
        kb = self._get_knowledge_base(params)
        return kb.get_objects(kb.Study)

    def start_service(self, host, port, debug=False):
        run(host=host, port=port, debug=debug)


def get_parser():
    parser = argparse.ArgumentParser('Run the Galaxy Menus HTTP server')
    parser.add_argument('--host', type=str, default='127.0.0.1',
                        help='web service binding host')
    parser.add_argument('--port', type=int, default='8080',
                        help='web service binding port')
    parser.add_argument('--debug', action='store_true',
                        help='Enable web server DEBUG mode')
    parser.add_argument('--pid-file', type=str, help='PID file for the dbservice daemon',
                        default='/tmp/galaxy_menus_service.pid')
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
    check_pid(args.pid_file)
    create_pid(args.pid_file)
    gms.start_service(args.host, args.port, args.debug)
    destroy_pid(args.pid_file)


if __name__ == '__main__':
    main(sys.argv[1:])
