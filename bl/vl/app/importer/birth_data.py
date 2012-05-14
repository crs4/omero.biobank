# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
Import birth data
=================

Will read in a tsv file with the following columns::

  study   individual   timestamp      birth_date   birth_place
  ASTUDY  V1234        1310057541608  12/03/1978   006171
  ASTUDY  V14112       1310057541608  25/04/1983   006149
  ASTUDY  V1241        1310057541608  12/03/2001   006172
  .....

where birth_place is a valid ISTAT code for an Italian city or a
foreign Country.

Birth data will be imported as EHR records using the
openEHR-DEMOGRAPHIC-CLUSTER.person_birth_data_iso.v1
 * birth_date will be stored in at0001 field
 * birth_place will be stored in at0002 field
"""

import csv, json, time
import itertools as it
from datetime import datetime

import core
from version import version

class Recorder(core.Core):
    
    def __init__(self, out_stream=None, study_label=None,
                 host=None, user=None, passwd=None,
                 keep_tokens=1, batch_size=1000, logger=None,
                 operator='Alfred E. Neumann', action_setup_conf=None):
        super(Recorder, self).__init__(host, user, passwd, keep_tokens=keep_tokens,
                                       study_label=study_label, logger=logger)
        self.batch_size = batch_size
        self.action_setup_conf = action_setup_conf
        self.operator = operator
        self.preloaded_individuals = {}

    def record(self, records):
        def records_by_chunk(batch_size, records):
            offset = 0
            while len(records[offset:]) > 0:
                yield records[offset:offset + batch_size]
                offset += batch_size
        if not records:
            self.logger.warning('no records')
            return
        self.preload_individuals()
        records = self.do_consistency_checks(records)
        study = self.find_study(records)
        device_label = 'importer.birth_data-%s' % (version)
        device = self.get_device(label = device_label,
                                 maker = 'CRS4', model = 'importer',
                                 release = version)
        asetup = self.get_action_setup('importer.birth_data-%f' % time.time(),
                                       json.dumps(self.action_setup_conf))
        for i, c in enumerate(records_by_chunk(self.batch_size, records)):
            self.logger.info('start processing chunk %d' % i)
            self.process_chunk(c, study, asetup, device)
            self.logger.info('done processing chunk %d' % i)

    def preload_individuals(self):
        self.preload_by_type('individual', self.kb.Individual,
                             self.preloaded_individuals)

    def process_chunk(self, chunk, study, asetup, device):
        actions = []
        for r in chunk:
            target = self.preloaded_individuals[r['individual']]
            conf = {'setup': asetup,
                    'device': device,
                    'actionCategory': self.kb.ActionCategory.IMPORT,
                    'operator': self.operator,
                    'context': study,
                    'target' : target,
                    }
            actions.append(self.kb.factory.create(self.kb.ActionOnIndividual, conf))
        self.kb.save_array(actions)
        for a, r in it.izip(actions, chunk):
            archetype = 'openEHR-DEMOGRAPHIC-CLUSTER.person_birth_data_iso.v1'
            fields = {}
            if r['birth_date']:
                fields['at0001'] = datetime.strptime(r['birth_date'], '%d/%m/%Y')
            if r['birth_place']:
                fields['at0002'] = r['birth_place']
            self.logger.debug('Saving record [%s --- %r]' % (archetype, fields))
            self.kb.add_ehr_record(a, long(r['timestamp']), archetype, fields)

    def do_consistency_checks(self, records):
        self.logger.info('start consistenxy checks')
        good_records = []
        mandatory_fields = ['individual', 'timestamp']
        for i, r in enumerate(records):
            reject = 'Rejecting import %d: ' % i
            if self.missing_fields(mandatory_fields, r):
                f = reject + 'missing mandatory field.'
                self.logger.error(f)
                continue
            if not r['individual'] in self.preloaded_individuals:
                msg = reject + 'unknown individual.'
                self.logger.error(msg)
                continue
            # TODO: check birth plate codes
            try:
                long(r['timestamp'])
            except ValueError, e:
                msg = reject + ('timestamp %r is not a long.' % r['timestamp'])
                self.logger.error(msg)
                continue
            good_records.append(r)
        self.logger.info('done consistenxy checks')
        return good_records


help_doc = """
import birth data (birth date and birth place) into the KB.
"""

def make_parser(parser):
    parser.add_argument('--study', metavar='STRING',
                        help='overrides the study column value')

def implementation(logger, host, user, passwd, args):
    action_setup_conf = Recorder.find_action_setup_conf(args)
    recorder = Recorder(args.study, host = host, user = user,
                        passwd = passwd, operator = args.operator,
                        action_setup_conf = action_setup_conf,
                        logger = logger)
    f = csv.DictReader(args.ifile, delimiter='\t')
    logger.info('start processing file %s' % args.ifile.name)
    records = [r for r in f]
    args.ifile.close()
    canonizer = core.RecordCanonizer(['study'], args)
    canonizer.canonize_list(records)
    recorder.record(records)
    logger.info('done processing file %s' % args.ifile.name)

def do_register(registration_list):
    registration_list.append(('birth_data', help_doc, make_parser,
                              implementation))
