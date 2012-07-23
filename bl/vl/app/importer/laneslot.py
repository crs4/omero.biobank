"""
Import laneslot
===============

TODO: add documentation
"""

import os, csv, json, time, re, copy
import itertools as it

from bl.vl.kb.drivers.omero.utils import make_unique_key
from bl.vl.kb.drivers.omero.vessels import VesselContent

import core
from version import version


class Recorder(core.Core):

    CONTENT_TYPE_CHOICES = ['DNA', 'RNA']
    SOURCE_TYPE_CHOICES = ['Tube', 'PlateWell', 'Individual']

    def __init__(self, out_stream=None, study_label=None,
                 host=None, user=None, passwd=None,
                 keep_tokens=1, batch_size=1000, 
                 action_setup_conf=None, operator=None,
                 logger=None):
        super(Recorder, self).__init__(host, user, passwd, keep_tokens=keep_tokens,
                                       study_label=study_label, logger=logger)
        self.operator = operator
        self.batch_size = batch_size
        self.action_setup_conf = action_setup_conf
        self.batch_size = batch_size
        self.preloaded_sources = {}
        self.preloaded_laneslots = {}
        self.preloaded_lanes = {}

    def record(self, records, otsv, rtsv):
        def records_by_chunk(batch_size, records):
            offset = 0
            while len(records[offset:]) > 0:
                yield records[offset:offset+batch_size]
                offset += batch_size
        if len(records) == 0:
            self.logger.warn('no records')
            return
        study = self.find_study(records)
        self.source_klass = self.find_source_klass(records)
        self.preload_sources()
        self.preload_lanes()
        self.preload_laneslots()
        records, bad_records = self.do_consistency_checks(records)
        for br in bad_records:
            rtsv.writerow(br)
        device = self.get_device('importer-%s.laneslot' % version,
                                 'CRS4', 'IMPORT', version)
        asetup = self.get_action_setup('import-prog-%f' % time.time(),
                                       json.dumps(self.action_setup_conf))
        for i, c in enumerate(records_by_chunk(self.batch_size, records)):
            self.logger.info('start processing chunk %d' % i)
            self.process_chunk(otsv, c, study, asetup, device)
            self.logger.info('done processing chunk %d' % i)

    def find_source_klass(self, records):
        return self.find_klass('source_type', records)

    def preload_sources(self):
        self.preload_by_type('sources', self.source_klass, self.preloaded_sources)

    def preload_lanes(self):
        self.preload_by_type('lanes', self.kb.Lane, self.preloaded_lanes)

    def preload_laneslots(self):
        ls = self.kb.get_objects(self.kb.LaneSlot)
        for x in ls:
            self.preloaded_laneslots[x.laneSlotUK] = x

    def do_consistency_checks(self, records):
        def build_key(r):
            lane = self.preloaded_lanes[r['lane']]
            return make_unique_key(r['tag'], lane.label)
        good_records = []
        bad_records = []
        grecs_keys = {}
        mandatory_fields = ['lane', 'content', 'source']
        for i, r in enumerate(records):
            reject = 'Rejecting import of record %d: ' % i
            if self.missing_fields(mandatory_fields, r):
                m = 'missing mandatory field'
                self.logger.warning(reject + m)
                bad_rec = copy.deepcopy(r)
                bad_rec['error'] = m
                bad_records.append(bad_rec)
                continue
            if r['source'] not in self.preloaded_sources:
                m = 'unknown source'
                self.logger.warning(reject + m)
                bad_rec = copy.deepcopy(r)
                bad_rec['error'] = m
                bad_records.append(bad_rec)
                continue
            if r['lane'] not in self.preloaded_lanes:
                m = 'unknown lane'
                self.logger.warning(reject + m)
                bad_rec = copy.deepcopy(r)
                bad_rec['error'] = m
                bad_records.append(bad_rec)
                continue
            key = build_key(r)
            if 'tag' in r and r['tag'] and key in self.preloaded_laneslots:
                m = 'tag %s alredy used in lane %s' % (r['tag'], r['lane'])
                self.logger.warning(reject + m)
                bad_rec = copy.deepcopy(r)
                bad_rec['error'] = m
                bad_records.append(bad_rec)
                continue
            if 'tag' in r and r['tag'] and key in grecs_keys:
                m = 'another record uses tag %s in lane %s' % (r['tag'],
                                                               r['lane'])
                self.logger.warning(reject + m)
                bad_rec = copy.deepcopy(r)
                bad_rec['error'] = m
                bad_records.append(bad_rec)
                continue
            good_records.append(r)
            grecs_keys[key] = r
        self.logger.info('done consistency checks')
        return good_records, bad_records

    def process_chunk(self, otsv, chunk, study, asetup, device):
        aklass = {
            self.kb.Individual: self.kb.ActionOnIndividual,
            self.kb.Tube: self.kb.ActionOnVessel,
            self.kb.PlateWell: self.kb.ActionOnVessel,
            }
        actions = []
        for r in chunk:
            target = self.preloaded_sources[r['source']]
            conf = {
                'setup': asetup,
                'device': device,
                'actionCategory': self.kb.ActionCategory.IMPORT,
                'operator': self.operator,
                'context': study,
                'target': target,
                }
            actions.append(self.kb.factory.create(aklass[target.__class__], conf))
        assert len(actions) == len(chunk)
        self.kb.save_array(actions)
        laneslots = []
        for a, r in it.izip(actions, chunk):
            a.unload()
            lane = self.preloaded_lanes[r['lane']]
            content = getattr(self.kb.VesselContent, r['content'])
            conf = {
                'lane': lane,
                'content': content,
                'action': a
                }
            if 'tag' in r:
                conf['tag'] = r['tag']
            laneslots.append(self.kb.factory.create(self.kb.LaneSlot, conf))
        assert len(laneslots) == len(chunk)
        self.kb.save_array(laneslots)
        for ls in laneslots:
            otsv.writerow({
                    'study': study.label,
                    'lane': ls.lane.label,
                    'tag': ls.tag if ls.tag else '',
                    'vid': ls.id,
                    })


def make_parser(parser):
    parser.add_argument('--study', metavar="STRING",
                        help='overrides the study column value')
    parser.add_argument('--content', metavar="STRING",
                        help='overrides the content columns value',
                        choices=Recorder.CONTENT_TYPE_CHOICES)
    parser.add_argument('--source_type', metavar="STRING",
                        help='overrides the source_type column value',
                        choices=Recorder.SOURCE_TYPE_CHOICES)

def implementation(logger, host, user, passwd, args):
    fields_to_canonize = [
        'study', 
        'content',
        'source_type'
        ]
    action_setup_conf = Recorder.find_action_setup_conf(args)
    recorder = Recorder(host=host, user=user, passwd=passwd,
                        keep_tokens=args.keep_tokens,
                        operator=args.operator,
                        action_setup_conf=action_setup_conf, logger=logger)
    recorder.logger.info('start processing file %s' % args.ifile.name)
    f = csv.DictReader(args.ifile, delimiter='\t')
    records = [r for r in f]
    canonizer = core.RecordCanonizer(fields_to_canonize, args)
    canonizer.canonize_list(records)
    if len(records) > 0:
        o = csv.DictWriter(args.ofile,
                           fieldnames = ['study', 'lane', 'tag', 'vid'],
                           delimiter='\t', lineterminator = os.linesep)
        o.writeheader()
        report_fnames = copy.deepcopy(f.fieldnames)
        report_fnames.append('error')
        report = csv.DictWriter(args.report_file, report_fnames,
                                delimiter='\t', lineterminator=os.linesep,
                                extrasaction='ignore')
        report.writeheader()
        recorder.record(records, o, report)
    else:
        recorder.logger.info('empty file')
    args.ifile.close()
    args.ofile.close()
    args.report_file.close()
    recorder.logger.info('done processing file %s' % args.ifile.name)

help_doc = """
import ney lane slots.
"""

def do_register(registration_list):
    registration_list.append(('laneslot', help_doc, make_parser,
                              implementation))
