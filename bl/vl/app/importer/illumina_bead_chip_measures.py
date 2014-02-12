"""
Import illumina bead chip measures
==================================

Will read a tsv file with the following columns::

  study   label           red_channel     green_channel     source         source_type
  ASTUDY  CHIP_01_R01C01  V1415151235513  V135135661356161  V351351351551  IlluminaBeadChipArray
  ASTUDY  CHIP_01_R01C02  V2346262462462  V112395151351623  V135113513223  IlluminaBeadChipArray
  ASTUDY  CHIP_01_R02C01  V1351362899135  V913977551235981  V100941215192  IlluminaBeadChipArray

This will create new IlluminaBeadChipMeasures whose labels are defined in the
label column.
"""

import csv, time, os, copy, sys, json
import itertools as it
from bl.vl.kb.drivers.omero.action import ActionCategory

import core
from version import version

SUPPORTED_SOURCE_TYPES=[
    'IlluminaBeadChipArray'
]

SUPPORTED_ACTION_CATEGORIES = [
    e.enum_label() for e in ActionCategory.__enums__
]

class Recorder(core.Core):
    def __init__(self, study_label=None, host=None, user=None, passwd=None,
                 keep_tokens=1, batch_size=1000, operator='Alfred E. Neumann',
                 logger=None, action_setup_conf=None):
        super(Recorder, self).__init__(host, user, passwd, keep_tokens=keep_tokens,
                                       study_label=study_label, logger=logger)
        self.batch_size = batch_size
        self.operator = operator
        self.action_setup_conf = action_setup_conf
        self.preloaded_data_collections = {}

    def preload_data_collections(self):
        self.logger.info('start preloading illumina bead chip measures collections')
        ds = self.kb.get_objects(self.kb.IlluminaBeadChipMeasures)
        for d in ds:
            self.preloaded_data_collections[d.label] = d
        self.logger.info('there are %d IlluminaBeadChipMeasure(s) in the KB',
                         len(self.preloaded_data_collections))

    def do_consistency_checks(self, records):
        self.logger.info('start consistency checks')
        good_records = []
        bad_records = []
        seen = []
        mandatory_fields = ['label', 'source', 'source_type', 'red_channel',
                            'green_channel', 'study', 'action_category']
        for i, r in enumerate(records):
            reject = 'Rejecting import of record %d: ' % i
            if self.missing_fields(mandatory_fields, r):
                f = 'missing mandatory field'
                self.logger.error(reject + f)
                bad_rec = copy.deepcopy(r)
                bad_rec['error'] = f
                bad_records.append(bad_rec)
                continue
            if r['label'] in self.preloaded_data_collections:
                f = 'an IlluminaBeadChipMeasures object with label %s alredy exists' % r['label']
                self.logger.error(reject + f)
                bad_rec = copy.deepcopy(r)
                bad_rec['error'] = f
                bad_records.append(bad_rec)
                continue
            if r['red_channel'] == r['green_channel']:
                f = 'green_channel and red_channel reference the same object'
                self.logger.error(reject + f)
                bad_rec = copy.deepcopy(r)
                bad_rec['error'] = f
                bad_records.append(bad_rec)
                continue
            for channel in ['red_channel', 'green_channel']:
                if not self.is_known_object_id(self.kb.IlluminaBeadChipMeasure,
                                               r[channel]):
                    f = 'unknown data sample with ID %s in column %s' % (r[channel], channel)
                    self.logger.error(reject + f)
                    bad_rec = copy.deepcopy(r)
                    bad_rec['error'] = f
                    bad_records.append(bad_rec)
                    continue
            if not self.is_known_object_id(getattr(self.kb, r['source_type']),
                                           r['source']):
                f = 'unknown %s with ID %s' % (r['source_type'], r['source'])
                self.logger.error(reject + f)
                bad_rec = copy.deepcopy(r)
                bad_rec['error'] = f
                bad_records.append(bad_rec)
                continue
            if r['label'] in seen:
                f = 'multiple copies of bead chip measures %s in this batch' % r['label']
                self.logger.error(reject + f)
                bad_rec = copy.deepcopy(r)
                bad_rec['error'] = f
                bad_records.append(bad_rec)
                continue
            seen.append(r['label'])
            good_records.append(r)
        self.logger.info('done with consistency checks')
        return good_records, bad_records

    def record(self, records, otsv, rtsv, blocking_validation):
        def records_by_chunk(batch_size, records):
            offset = 0
            while len(records[offset:]) > 0:
                yield records[offset:offset+batch_size]
                offset += batch_size

        aklass = {
            'IlluminaBeadChipArray': self.kb.ActionOnVessel,
        }

        if len(records) == 0:
            msg = 'No records are going to be imported'
            self.logger.critical(msg)
            raise core.ImporterValidationError(msg)
        study = self.find_study(records)
        self.preload_data_collections()
        asetup = self.get_action_setup('importer.illumina_bead_chip_measures-%f' % time.time(),
                                       json.dumps(self.action_setup_conf))
        device = self.get_device('importer-%s.illumina_bead_chip_measures' % version,
                                 'CRS4', 'IMPORT', version)
        good_records, bad_records = self.do_consistency_checks(records)
        for br in bad_records:
            rtsv.writerow(br)
        if blocking_validation and len(bad_records) >= 1:
            raise core.ImporterValidationError('%d invalid records' % len(bad_records))
        if len(records) == 0:
            msg = 'No records are going to be imported'
            self.logger.warning(msg)
            sys.exit(0)
        for i, c in enumerate(records_by_chunk(self.batch_size, good_records)):
            self.logger.info('Loading chunk %d', i)
            actions = []
            for rec in c:
                aconf = {
                    'setup': asetup,
                    'device': device,
                    'actionCategory': getattr(self.kb.ActionCategory, rec['action_category']),
                    'operator': self.operator,
                    'context': study,
                    'target': self.kb.get_by_vid(getattr(self.kb, rec['source_type']), rec['source'])
                }
                actions.append(self.kb.factory.create(aklass[rec['source_type']], aconf))
            actions = self.kb.save_array(actions)
            measures = []
            for a, r in it.izip(actions, c):
                self.logger.debug('Dumping record: %r', r)
                a.unload()
                conf = {
                    'label': r['label'],
                    'action': a,
                    'redChannel': self.kb.get_by_vid(self.kb.IlluminaBeadChipMeasure,
                                                     r['red_channel']),
                    'greenChannel': self.kb.get_by_vid(self.kb.IlluminaBeadChipMeasure,
                                                       r['green_channel']),
                }
                measures.append(self.kb.factory.create(self.kb.IlluminaBeadChipMeasures,
                                                       conf))
            try:
                measures = self.kb.save_array(measures)
            except Exception, e:
                self.logger.info('Error! Cleaning saved actions')
                for a in actions:
                    self.kb.delete(a)
                self.logger.info('Done cleaning actions')
                raise core.ImporterValidationError('An error occurred while saving chunk %d: %r' %
                                                   (i, e))
            for m in measures:
                otsv.writerow(
                    {
                        'study': study.label,
                        'label': m.label,
                        'type': m.get_ome_table(),
                        'vid': m.id
                    }
                )


class RecorderCanonizer(core.RecordCanonizer):

    def canonize(self, r):
        super(RecorderCanonizer, self).canonize(r)


def make_parser(parser):
    parser.add_argument('--study', metavar='STRING',
                        help='overrides the study column value')
    parser.add_argument('--action_category', metavar='STRING',
                        help='overrides the action_category column value',
                        choices=SUPPORTED_ACTION_CATEGORIES)
    parser.add_argument('--source_type', metavar='STRING',
                        help='overrides the source_type column value',
                        choices=SUPPORTED_SOURCE_TYPES)


def implementation(logger, host, user, passwd, args, close_handles):
    fields_to_canonize = ['study', 'action_category', 'source_type']
    action_setup_conf = Recorder.find_action_setup_conf(args)
    recorder = Recorder(args.study, host=host, user=user, passwd=passwd,
                        operator=args.operator, action_setup_conf=action_setup_conf,
                        logger=logger)
    f = csv.DictReader(args.ifile, delimiter='\t')
    recorder.logger.info('start processing file %s', args.ifile.name)
    records = [r for r in f]
    canonizer = RecorderCanonizer(fields_to_canonize, args)
    canonizer.canonize_list(records)
    o = csv.DictWriter(args.ofile, fieldnames=['study', 'label', 'type', 'vid'],
                       delimiter='\t')
    o.writeheader()
    report_fnames = copy.deepcopy(f.fieldnames)
    report_fnames.append('error')
    report = csv.DictWriter(args.report_file, report_fnames,
                            delimiter='\t', lineterminator=os.linesep,
                            extrasaction='ignore')
    report.writeheader()
    try:
        recorder.record(records, o, report,
                        args.blocking_validator)
    except core.ImporterValidationError as ve:
        recorder.logger.critical(ve.message)
        raise
    finally:
        close_handles(args)
    recorder.logger.info('done processing file %s', args.ifile.name)
    args.ifile.close()
    args.ofile.close()
    args.report_file.close()

help_doc = """
import ne illumina bead chip measures definition into the KB.
"""


def do_register(registration_list):
    registration_list.append(('illumina_bead_chip_measures', help_doc, make_parser,
                              implementation))
