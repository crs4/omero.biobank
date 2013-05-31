# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
Import birth data
=================

Will read in a tsv file with the following columns::

  study   individual   timestamp      birth_date   birth_place   birth_place_district
  ASTUDY  V1234        1310057541608  12/03/1978   006171        AL
  ASTUDY  V14112       1310057541608  25/04/1983   006149        AL
  ASTUDY  V1241        1310057541608  12/03/2001   006172        AL
  .....

where birth_place is a valid ISTAT code for an Italian city or a
foreign Country and birth_date must have the dd/mm/YYYY format.

Birth data will be imported as EHR records using the
openEHR-DEMOGRAPHIC-CLUSTER.person_birth_data_iso.v1
 * birth_date will be stored in at0001 field
 * birth_place will be stored in at0002 field if birth_plate column corresponds to a 
   foreign country (Italy will be stored for Italian cities)
 * birth_place will be stored in at0006.openEHR-DEMOGRAPHIC-CLUSTER.person_other_birth_data_br.v1.at0002
   for Italian cities
 * birth_place_district will be stored in at0006.openEHR-DEMOGRAPHIC-CLUSTER.person_other_birth_data_br.v1.at0001
   for Italian cities
"""

import csv, json, time, sys, copy, os
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
        self.preloaded_birth_records = {}
        self.preloaded_locations = []

    def record(self, records, rtsv):
        def records_by_chunk(batch_size, records):
            offset = 0
            while len(records[offset:]) > 0:
                yield records[offset:offset + batch_size]
                offset += batch_size
        if not records:
            msg = 'No records are going to be imported'
            self.logger.critical(msg)
            raise core.ImporterValidationError(msg)
        self.preload_individuals()
        self.preload_birth_data_records()
        self.preload_locations()
        records, bad_records = self.do_consistency_checks(records)
        for br in bad_records:
            rtsv.writerow(br)
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

    def preload_birth_data_records(self):
        self.logger.info('Start preloading birth data records')
        bd_records = self.kb.get_birth_data()
        for bdr in bd_records:
            self.preloaded_birth_records[bdr['i_id']] = bdr
        self.logger.info('Done preloading birth data records')

    def preload_locations(self):
        self.logger.info('Start preloading locations')
        locs = self.kb.get_objects(self.kb.Location)
        self.preloaded_locations = [l.istatCode for l in locs]
        self.logger.info('Done preloading birth data records')

    def append_birth_place_data(self, atype_fields, record):
        if record['birth_place'].startswith('999'):
            # Foreign country
            atype_fields['at0002'] = record['birth_place']
        else:
            # Italian city
            atype_fields['at0002'] = '999100'
            atype_fields['at0006.openEHR-DEMOGRAPHIC-CLUSTER.person_other_birth_data_br.v1.at0002'] = record['birth_place']
            if record['birth_place_district']:
                atype_fields['at0006.openEHR-DEMOGRAPHIC-CLUSTER.person_other_birth_data_br.v1.at0001'] = record['birth_place_district']

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
                self.append_birth_place_data(fields, r)
            self.logger.debug('Saving record [%s --- %r]' % (archetype, fields))
            self.kb.add_ehr_record(a, long(r['timestamp']), archetype, fields)

    def do_consistency_checks(self, records):
        self.logger.info('start consistenxy checks')
        good_records = []
        bad_records = []
        mandatory_fields = ['individual', 'timestamp']
        for i, r in enumerate(records):
            reject = 'Rejecting record %d: ' % i
            if self.missing_fields(mandatory_fields, r):
                msg = 'missing mandatory field.'
                self.logger.warning(reject + msg)
                bad_rec = copy.deepcopy(r)
                bad_rec['error'] = msg
                bad_records.append(bad_rec)
                continue
            if not r['individual'] in self.preloaded_individuals:
                msg = 'unknown individual.'
                self.logger.warning(reject + msg)
                bad_rec = copy.deepcopy(r)
                bad_rec['error'] = msg
                bad_records.append(bad_rec)
                continue
            if r['individual'] in self.preloaded_birth_records:
                msg = 'birth data already loaded'
                self.logger.error(reject + msg)
                self.logger.debug(self.preloaded_birth_records[r['individual']])
                bad_rec = copy.deepcopy(r)
                bad_rec['error'] = msg
                bad_records.append(bad_rec)
                continue
            try:
                datetime.strptime(r['birth_date'], '%d/%m/%Y')
            except ValueError, e:
                msg = str(e)
                self.logger.error(reject + msg)
                bad_rec = copy.deepcopy(r)
                bad_rec['error'] = msg
                bad_records.append(bad_rec)
                continue
            try:
                long(r['timestamp'])
            except ValueError, e:
                msg = ('timestamp %r is not a long.' % r['timestamp'])
                self.logger.error(reject + msg)
                bad_rec = copy.deepcopy(r)
                bad_rec['error'] = msg
                bad_records.append(bad_rec)
                continue
            if r['birth_place'] != '' and r['birth_place'] not in self.preloaded_locations:
                msg = ('unknown ISTAT code %s' % r['birth_place'])
                self.logger.error(reject + msg)
                bad_rec = copy.deepcopy(r)
                bad_rec['error'] = msg
                bad_records.append(bad_rec)
                continue
            good_records.append(r)
        self.logger.info('done consistenxy checks')
        return good_records, bad_records


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
    report_fnames = copy.deepcopy(f.fieldnames)
    report_fnames.append('error')
    report = csv.DictWriter(args.report_file, report_fnames,
                            delimiter='\t', lineterminator=os.linesep,
                            extrasaction='ignore')
    report.writeheader()
    recorder.record(records, report)
    args.ifile.close()
    args.report_file.close()
    logger.info('done processing file %s' % args.ifile.name)

def do_register(registration_list):
    registration_list.append(('birth_data', help_doc, make_parser,
                              implementation))
