import csv, sys, json

from bl.vl.app.importer.core import Core


class BuildSeqRunResultsReport(Core):
    OUT_FILE_HEADER = ['FILE_PATH', 'FILE_MIMETYPE',
                       'PROJECT', 'SAMPLE_ID', 'READ']

    def __init__(self, host=None, user=None, passwd=None,
                 keep_tokens=1, logger=None, study_label=None,
                 operator='Alfred E. Neumann'):
        super(BuildSeqRunResultsReport, self).__init__(host, user, passwd,
                                                       keep_tokens=keep_tokens,
                                                       study_label=study_label,
                                                       logger=logger)

    def __get_sequencer_output__(self, run_id):
        seq_out = self.kb.get_data_sample(run_id)
        if seq_out and type(seq_out) == self.kb.SequencerOutput:
            return seq_out
        else:
            return None

    def __get_children_seq_dsamples__(self, obj):
        # TODO: replace this with the Neo4j graph API when we'll merge develop into master
        query = 'SELECT dsample FROM SeqDataSample dsample JOIN dsample.action act WHERE act.id IN ' \
                '(SELECT a.id FROM ActionOnDataSample a JOIN a.target t WHERE t.label = :target_label)'
        results = self.kb.find_all_by_query(query, {'target_label': obj.label})
        return results

    def __get_results__(self, sequencer_output):
        # Retrieve the first data samples connected to the sequencer output
        results = []
        stage1 = self.__get_children_seq_dsamples__(sequencer_output)
        results.extend(stage1)
        for ds in stage1:
            # For each data sample in stage1, retrieve the produced results
            results.extend(self.__get_children_seq_dsamples__(ds))
        return results

    def __dump_records__(self, csv_writer, seq_data_sample):
        dobjs = self.kb.get_data_objects(seq_data_sample)
        if len(dobjs) == 0:
            self.logger.info('No files related to Data Sample %s' % seq_data_sample.label)
        # Retrieve seq_data_sample action's setup, we need this to retrieve read and project
        act_setup = json.loads(seq_data_sample.action.setup.conf)
        for d in dobjs:
            record = {
                'FILE_PATH': d.path,
                'FILE_MIMETYPE': d.mimetype,
                'PROJECT': str(act_setup[u'project']),
                'SAMPLE_ID': seq_data_sample.sample.label,
                }
            try:
                record['READ'] = str(act_setup[u'read'])
            except KeyError:
                # No read information
                pass
            self.logger.debug('Dumping record %r' % record)
            csv_writer.writerow(record)

    def dump(self, run_id, out_file):
        self.logger.info('Starting job')
        run_obj = self.__get_sequencer_output__(run_id)
        if not run_obj:
            self.logger.info('No run found with ID %s, nothing to do' % run_id)
            sys.exit(0)
        dsamples = self.__get_results__(run_obj)
        if len(dsamples) == 0:
            self.logger.info('No results for run ID %s' % run_id)
            sys.exit(0)
        self.logger.info('Writing report file')
        out_writer = csv.DictWriter(out_file, self.OUT_FILE_HEADER,
                                    delimiter='\t')
        out_writer.writeheader()
        for ds in dsamples:
            self.__dump_records__(out_writer, ds)
        out_file.close()
        self.logger.info('Job completed')


def make_parser(parser):
    parser.add_argument('--run_id', type=str, required=True,
                        help='Run ID')


def implementation(logger, host, user, passwd, args):
    app = BuildSeqRunResultsReport(host=host, user=user, passwd=passwd,
                                   keep_tokens=args.keep_tokens,
                                   logger=logger)
    app.dump(args.run_id, args.ofile)


def do_register(registration_list):
    help_doc = 'Build a report for the results obtained for the given run ID'
    registration_list.append(('seq_results_report', help_doc,
                              make_parser, implementation))