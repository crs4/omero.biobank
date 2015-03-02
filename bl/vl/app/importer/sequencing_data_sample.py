"""
Import Sequencing related datasample
===============

Will read a tsv file with the following columns::

 study   label      source   source_type  seq_dsample_type  status  device
 FOOBAR  seq_out_1  V012141  FlowCell     SequencerOutput   USABLE  V123141
 FOOBAR  seq_out_2  V012141  FlowCell     SequencerOutput   USABLE  V123141
 FOOBAR  seq_out_3  V1AD124  FlowCell     SequencerOutput   USABLE  V123141
 ...

where
  * seq_dsample_type can assume one of the following values: SequencerOutput, RawSeqDataSample, SeqDataSample
  * source_type can assume one of the following values: FlowCell, SequencerOutput, RawSeqDataSample
 
study, source_type, seq_dsample_type, status and device columns can be
overwritten by using command line options.

A special case of the previous file is when seq_dsample_type is
SeqDataSample, in this case a mandatory sample column is required,
this column has to contain IDs of Tube objects.
The file will look like this

 study   label          source   source_type      seq_dsample_type  status  device   sample
 FOOBAR  seq_dsample_1  V041241  SequencerOutput  SeqDataSample     USABLE  VBB2351  V124AA41
 FOOBAR  seq_dsample_2  V051561  SequencerOutput  SeqDataSample     USABLE  VBB2351  V4151AAE
 FOOBAR  seq_dsample_3  V151561  SequencerOutput  SeqDataSample     USABLE  VBB2351  V15199CD
 ...

A file containing ax export of the Galaxy history that produced the
data that are going to be imported can be passed as input parameter,
history details must represented as a string serialized in JSON
format.

"""

import os, csv, json, time, copy, sys

import core


SUPPORTED_SOURCES = [
    'FlowCell',
    'SequencerOutput',
    'RawSeqDataSample',
    'Tube',
    ]

SUPPORTED_DATA_SAMPLE_TYPES = [
    'SequencerOutput',
    'RawSeqDataSample',
    'SeqDataSample',
    'AlignedSeqDataSample'
    ]

class Recorder(core.Core):
    def __init__(self, study_label = None,
                 host = None, user = None, passwd = None,
                 keep_tokens = 1, batch_size = 1000,
                 operator = 'Alfred E. Neumann',
                 action_setup_conf = None, 
                 history = None, logger = None):
        super(Recorder, self).__init__(host, user, passwd, keep_tokens = keep_tokens,
                                       study_label = study_label, logger = logger)
        self.batch_size = batch_size
        self.action_setup_conf = action_setup_conf
        self.operator = operator
        self.preloaded_data_samples = {}
        self.preloaded_lanes = {}
        self.preloaded_tubes = {}
        self.preloaded_references = {}
        self.history = history
        self.status_map = dict((st.enum_label(), st) for st in
                               self.kb.get_objects(self.kb.DataSampleStatus))

    def record(self, records, otsv, rtsv, blocking_validation):
        def records_by_chunk(batch_size, records):
            offset = 0
            while len(records[offset:]) > 0:
                yield records[offset : offset + batch_size]
                offset += batch_size
        if len(records) == 0:
          msg = 'No records are going to be imported'
          self.logger.critical(msg)
          raise core.ImporterValidationError(msg)
        study = self.find_study(records)
        self.source_klass = self.find_source_klass(records)
        self.seq_sample_klass = self.find_seq_sample_klass(records)
        if self.seq_sample_klass == self.kb.RawSeqDataSample:
            self.preload_lanes()
        if self.seq_sample_klass == self.kb.SeqDataSample:
            self.preload_tubes()
        if self.seq_sample_klass == self.kb.AlignedSeqDataSample:
            self.preload_tubes()
            self.preload_references()

        records, bad_records = self.do_consistency_checks(records)
        for br in bad_records:
            rtsv.writerow(br)
        if blocking_validation and len(bad_records) >= 1:
            raise core.ImporterValidationError('%d invalid records' % len(bad_records))
        act_setups = set((r['source'], r.get('device', None),
                          Recorder.get_action_setup_options(r, self.action_setup_conf,
                                                            self.history))
                         for r in records)
        self.logger.debug('Action setups:\n%r' % act_setups)
        actions = {}
        for acts in act_setups:
            # TODO: if a history has been passed, add this to the options
            act_label = 'importer.seq_data_sample.%f' % time.time()
            act_setup_conf = {'label' : act_label,
                              'conf' : acts[2]}
            act_setup = self.kb.save(self.kb.factory.create(self.kb.ActionSetup, 
                                                            act_setup_conf))
            act_setup.unload()
            if issubclass(self.source_klass, self.kb.FlowCell):
                act_klass = self.kb.ActionOnCollection
                act_category = self.kb.ActionCategory.MEASUREMENT
            elif issubclass(self.source_klass, self.kb.DataSample):
                act_klass = self.kb.ActionOnDataSample
                act_category = self.kb.ActionCategory.PROCESSING
            else:
                self.logger.error('Unmanaged source type %r' % self.source_klass)
                sys.exit('Unmanaged source type %r' % self.source_klass)
            act_conf = {'setup' : act_setup,
                        'actionCategory' : act_category,
                        'operator' : self.operator,
                        'context' : study,
                        'target' : self.kb.get_by_vid(self.source_klass, acts[0])}
            if acts[1]:
                act_conf['device'] = self.kb.get_by_vid(self.kb.Device, acts[1])
            action = self.kb.factory.create(act_klass, act_conf)
            action = self.kb.save(action)
            # Unload the action object or it will cause a bug when
            # saving objects that references to ActionOnDataSample
            # records, too many inheritance steps
            action.unload()
            actions[acts] = action
        self.logger.debug('Actions are:\n%r' % actions)
        for i,c in enumerate(records_by_chunk(self.batch_size, records)):
            self.logger.info('start processing chunk %d' % i)
            self.process_chunk(otsv, c, actions, study)
            self.logger.info('done processing chunk %d' % i)

    def find_source_klass(self, records):
        return self.find_klass('source_type', records)

    def find_seq_sample_klass(self, records):
        return self.find_klass('seq_dsample_type', records)

    def preload_lanes(self):
        self.preload_by_type('lanes', self.kb.Lane,
                             self.preloaded_lanes)

    def preload_tubes(self):
        self.preload_by_type('tubes', self.kb.Tube,
                             self.preloaded_tubes)

    def preload_references(self):
        self.preload_by_type('tubes', self.kb.ReferenceGenome,
                             self.preloaded_references)

    def do_consistency_checks(self, records):
        self.logger.info('start consistency checks')
        good_recs, bad_recs = self.do_consistency_checks_common_fields(records)
        if self.seq_sample_klass == self.kb.RawSeqDataSample:
            good_recs, brecs = self.do_consistency_checks_raw_seq_data_sample(good_recs)
            bad_recs.extend(brecs)
        if self.seq_sample_klass == self.kb.SeqDataSample:
            good_recs, brecs = self.do_consistency_checks_seq_data_sample(good_recs)
            bad_recs.extend(brecs)
        if self.seq_sample_klass == self.kb.AlignedSeqDataSample:
            good_recs, brecs = self.do_consistency_checks_aligned_seq_data_sample(good_recs)
            bad_recs.extend(brecs)
        self.logger.info('done consistency checks')
        return good_recs, bad_recs

    def do_consistency_checks_raw_seq_data_sample(self, records):
        pass

    def do_consistency_checks_common_fields(self, records):
        def preload_data_samples():
            self.logger.info('start preloading data samples')
            objs = self.kb.get_objects(self.kb.DataSample)
            for o in objs:
                assert not o.label in self.preloaded_data_samples
                self.preloaded_data_samples[o.label] = o
            self.logger.info('done preloading data samples')
        preload_data_samples()
        good_records = []
        bad_records = []
        grecs_labels = {}
        mandatory_fields = ['status', 'label', 'source']
        for i, r in enumerate(records):
            reject = 'Rejecting import of line %d.' % i
            if self.missing_fields(mandatory_fields, r):
                m = 'missing mandatory field. '
                self.logger.warning(m + reject)
                bad_rec = copy.deepcopy(r)
                bad_rec['error'] = m
                bad_records.append(bad_rec)
                continue
            if r['label'] in self.preloaded_data_samples:
                m = 'label %s already in use. ' % r['label']
                self.logger.warning(m + reject)
                bad_rec = copy.deepcopy(r)
                bad_rec['error'] = m
                bad_records.append(bad_rec)
                continue
            if r['label'] in grecs_labels:
                m = 'label %s already used in record %d. ' % (r['label'],
                                                              grecs_labels[r['label']])
                self.logger.warning(m + reject)
                bad_rec = copy.deepcopy(r)
                bad_rec['error'] = m
                bad_records.append(bad_rec)
                continue
            if r['source'] and not self.is_known_object_id(r['source'],
                                                           self.source_klass):
                m = 'unknown source with ID %s. ' % r['source']
                self.logger.warning(m + reject)
                bad_rec = copy.deepcopy(r)
                bad_rec['error'] = m
                bad_records.append(bad_rec)
                continue
            if 'device' in r and r['device'] and not self.is_known_object_id(
                r['device'], self.kb.Device):
                m = 'unknown device with ID %s. ' % r['device']
                self.logger.warning(m + reject)
                bad_rec = copy.deepcopy(r)
                bad_rec['error'] = m
                bad_records.append(bad_rec)
                continue
            if r['status'] not in self.status_map:
                m = 'unkown status %s. ' % r['status']
                self.logger.warning(m + reject)
                bad_rec = copy.deepcopy(r)
                bad_rec['error'] = m
                bad_records.append(bad_rec)
                continue
            good_records.append(r)
            grecs_labels[r['label']] = i
        self.logger.info('done consistency checks')
        return good_records, bad_records

    def do_consistency_checks_seq_data_sample(self, records):
        good_records = []
        bad_records = []
        for i, r in enumerate(records):
            reject = 'Rejecting import of line %d.' % i
            if r['sample'] and r['sample'] not in self.preloaded_tubes:
                m = 'unknown sample with ID %s. ' % r['sample']
                self.logger.warning(m + reject)
                bad_rec = copy.deepcopy(r)
                bad_rec['error'] = m
                bad_records.append(bad_rec)
                continue
            good_records.append(r)
        self.logger.info('done SeqDataSample specific consistency checks')
        return good_records, bad_records

    def do_consistency_checks_aligned_seq_data_sample(self, records):
        good_records = []
        bad_records = []
        for i, r in enumerate(records):
            reject = 'Rejecting import of line %d.' % i
            if r['sample'] and r['sample'] not in self.preloaded_tubes:
                m = 'unknown sample with ID %s. ' % r['sample']
                self.logger.warning(m + reject)
                bad_rec = copy.deepcopy(r)
                bad_rec['error'] = m
                bad_records.append(bad_rec)
                continue
            if r['genome_reference'] and r['genome_reference'] not in self.preloaded_references:
                m = 'unknown reference genome with ID %s. ' % r['genome_reference']
                self.logger.warning(m + reject)
                bad_rec = copy.deepcopy(r)
                bad_rec['error'] = m
                bad_records.append(bad_rec)
                continue
            good_records.append(r)
        self.logger.info('done AlignedSeqDataSample specific consistency checks')
        return good_records, bad_records

    def process_chunk(self, otsv, chunk, actions, study):
        seq_data_samples = []
        for r in chunk:
            a = actions[(r['source'], r.get('device', None),
                         Recorder.get_action_setup_options(r, self.action_setup_conf,
                                                           self.history))]
            if self.seq_sample_klass == self.kb.SequencerOutput:
                seq_data_samples.append(self.conf_sequencer_output_data_sample(r, a))
            elif self.seq_sample_klass == self.kb.RawSeqDataSample:
                seq_data_samples.append(self.conf_raw_seq_data_sample(r, a))
            elif self.seq_sample_klass == self.kb.SeqDataSample:
                seq_data_samples.append(self.conf_seq_data_sample(r, a))
            elif self.seq_sample_klass == self.kb.AlignedSeqDataSample:
                seq_data_samples.append(self.conf_aligned_seq_data_sample(r, a))
            else:
                self.logger.error('Unmanaged data sample type %r' % self.seq_sample_klass)
                sys.exit('Unmanaged data sample type %r' % self.seq_sample_klass)
        assert len(seq_data_samples) == len(chunk)
        self.kb.save_array(seq_data_samples)
        for d in seq_data_samples:
            otsv.writerow({'study' : study.label,
                           'label' : d.label,
                           'type'  : d.get_ome_table(),
                           'vid'   : d.id })

    def conf_sequencer_output_data_sample(self, r, a):
        conf = {'label' : r['label'],
                'status' : self.status_map[r['status']],
                'action' : a}
        return self.kb.factory.create(self.kb.SequencerOutput, conf)

    def conf_raw_seq_data_sample(self, r, a):
        conf = {'label' : r['label'],
                'status' : self.status_map[r['status']],
                'action' : a}
        if r['lane']:
            conf['lane'] = self.preloaded_lanes[r['lane']]
        return self.kb.factory.create(self.kb.RawSeqDataSample, conf)

    def conf_seq_data_sample(self, r, a):
        conf = {'label' : r['label'],
                'status' : self.status_map[r['status']],
                'action' : a}
        if r['sample']:
            conf['sample'] = self.preloaded_tubes[r['sample']]

        return self.kb.factory.create(self.kb.SeqDataSample, conf)

    def conf_aligned_seq_data_sample(self, r, a):
        conf = {'label' : r['label'],
                'status' : self.status_map[r['status']],
                'action' : a}
        if r['sample']:
            conf['sample'] = self.preloaded_tubes[r['sample']]
        if r['genome_reference']:
            conf['referenceGenome'] = self.preloaded_references[r['genome_reference']]
        return self.kb.factory.create(self.kb.AlignedSeqDataSample, conf)
            
            
class RecordCanonizer(core.RecordCanonizer):

    def canonize(self, r):
        super(RecordCanonizer, self).canonize(r)
        for f in 'device', 'sample', 'lane', 'options':
            if f in r:
                if r[f].upper() == 'NONE':
                    r[f] = None


def make_parser(parser):
    parser.add_argument('--study', metavar='STRING',
                        help='overrides the study column value')
    parser.add_argument('--source-type', metavar='STRING',
                        choices = SUPPORTED_SOURCES,
                        help='overrides the source_type column value')
    parser.add_argument('--seq-dsample-type', metavar='STRING',
                        choices = SUPPORTED_DATA_SAMPLE_TYPES,
                        help='overrides the seq_dsample_type column')
    parser.add_argument('--status', metavar='STRING',
                        choices = ['UNKNOWN', 'DESTROYED', 'CORRUPTED', 'USABLE'],
                        help='overrides the status column')
    parser.add_argument('--device', metavar='STRING',
                        help='overrides the device column')
    parser.add_argument('--history', metavar='STRING',
                        help='galaxy history in JSON format, all the objects in the input file will share this history')

def implementation(logger, host, user, passwd, args, close_handles):
    fields_to_canonize = [
        'study',
        'source_type',
        'seq_dsample_type',
        'status',
        'device'
        ]
    if args.history:
        with open(args.history) as hf:
            history = json.loads(hf.read().strip())
    else:
        history = None
    action_setup_conf = Recorder.find_action_setup_conf(args)
    recorder = Recorder(args.study, host = host, user = user,
                        passwd = passwd, operator = args.operator,
                        action_setup_conf = action_setup_conf,
                        history = history, logger = logger)
    f = csv.DictReader(args.ifile, delimiter='\t')
    recorder.logger.info('start processing file %s' % args.ifile.name)
    records = [r for r in f]
    canonizer = RecordCanonizer(fields_to_canonize, args)
    canonizer.canonize_list(records)
    o = csv.DictWriter(args.ofile,
                       fieldnames = ['study', 'label', 'type', 'vid'],
                       delimiter = '\t', lineterminator = os.linesep)
    o.writeheader()
    report_fnames = copy.deepcopy(f.fieldnames)
    report_fnames.append('error')
    report = csv.DictWriter(args.report_file, report_fnames,
                            delimiter = '\t', lineterminator = os.linesep,
                            extrasaction = 'ignore')
    report.writeheader()
    try:
        recorder.record(records, o, report,
                        args.blocking_validator)
    except core.ImporterValidationError as ve:
        recorder.logger.critical(ve.message)
        raise
    close_handles(args)
    recorder.logger.info('done processing file %s' % args.ifile.name)


help_doc = """
import new sequencing related data samples into the knowledhge base
"""

def do_register(registration_list):
    registration_list.append(('seq_data_sample', help_doc, make_parser,
                              implementation))
