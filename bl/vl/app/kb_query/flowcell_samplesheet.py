# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
DOC here..
"""

import argparse, csv, sys, json

from bl.vl.app.importer.core import Core

class IdentifierError(Exception):
    def __init__(self, identifier):
        self.identifier = identifier

    def __str__(self):
        return '%s can\'t be used as a unique identifier, add a namespace' % self.identifier

class BuildFlowCellSamplesheetApp(Core):

    OUT_FILE_HEADER = ['FCID', 'Lane', 'SampleID', 'SampleRef', 'Index',
                       'Description', 'Control', 'Recipe', 'Operator']
    NAMESPACE_DELIMITER = '|'

    def __init__(self, host=None, user=None, passwd=None,
                 keep_tokens=1, logger=None, study_label=None,
                 operator='Alfred E. Neumann'):
        super(BuildFlowCellSamplesheetApp, self).__init__(host, user, passwd,
                                                          keep_tokens=keep_tokens,
                                                          study_label=study_label,
                                                          logger=logger)

    def __get_flowcell(self, flowcell_id, ignore_namespace):
        if ignore_namespace:
            query = "SELECT fc FROM FlowCell fc WHERE fc.label LIKE '%%|%s'" % flowcell_id
        else:
            query = 'SELECT fc FROM FlowCell fc WHERE fc.label = :flowcell_id'
        self.logger.debug('Using query: %r' % query)
        flowcells = self.kb.find_all_by_query(query, {'flowcell_id' : flowcell_id})
        self.logger.debug('Query returned %d items' % len(flowcells))
        if len(flowcells) > 1:
            raise IdentifierError(flowcell_id)
        try:
            return flowcells[0]
        except IndexError:
            msg = 'No FlowCell found using pattern "%s"'
            if ignore_namespace:
                msg = msg % ('%%|%s' % flowcell_id)
            else:
                msg = msg % flowcell_id
            self.logger.info(msg)
            return None

    def __sort_laneslots(self, laneslot_a, laneslot_b):
        if laneslot_a.lane.slot > laneslot_b.lane.slot:
            return 1
        elif laneslot_a.lane.slot < laneslot_b.lane.slot:
            return -1
        else:
            return 0

    def __get_flowcell_details(self, flowcell):
        self.logger.info('Loading lanes')
        lanes = list(self.kb.get_lanes_by_flowcell(flowcell))
        self.logger.info('Loaded %d lanes' % len(lanes))
        if len(lanes) == 0:
            return []
        laneslots = []
        self.logger.info('Loading laneslots')
        for l in lanes:
            laneslots.extend(list(self.kb.get_laneslots_by_lane(l)))
        self.logger.info('Loaded %d laneslots' % len(laneslots))
        laneslots.sort(cmp=self.__sort_laneslots)
        return laneslots
            

    def __get_label(self, obj, remove_namespaces):
        if not remove_namespaces:
            return obj.label
        else:
            return self.NAMESPACE_DELIMITER.join(obj.label.split(self.NAMESPACE_DELIMITER)[1:])

    def __dump_record(self, csv_writer, laneslot, remove_namespaces, add_sample_label):
        record = {'FCID'     : self.__get_label(laneslot.lane.flowCell, remove_namespaces),
                  'Lane'     : laneslot.lane.slot,
                  'SampleID' : laneslot.action.target.id,
                  'Index'    : laneslot.tag or '',
                  'Recipe'   : json.loads(laneslot.action.setup.conf)['protocol'],
                  'Operator' : json.loads(laneslot.action.setup.conf)['operator']
                  }
        if add_sample_label:
            record['SampleLabel'] = self.__get_label(laneslot.action.target, remove_namespaces) # FIX: crashes if source is not a sample
        self.logger.debug('Dumping record %r' % record)
        csv_writer.writerow(record)

    def dump(self, flowcell_id, out_file, fields_separator,
             ignore_namespace, remove_namespaces, add_sample_label):
        self.logger.info('Starting job')
        flowcell = self.__get_flowcell(flowcell_id, ignore_namespace)
        if not flowcell:
            self.logger.info('Nothing to do, exit.')
            sys.exit(0)
        fc_elements = self.__get_flowcell_details(flowcell)
        if len(fc_elements) == 0:
            self.logger.info('There aren\'t slots associated to this FlowCell, nothing to do.')
            sys.exit(0)
        self.logger.info('Writing samplesheet')
        if add_sample_label:
            self.OUT_FILE_HEADER.append('SampleLabel')
        out_writer = csv.DictWriter(out_file, self.OUT_FILE_HEADER,
                                    delimiter=fields_separator)
        out_writer.writeheader()
        for element in fc_elements:
            self.__dump_record(out_writer, element, remove_namespaces, add_sample_label)
        out_file.close()
        self.logger.info('Job completed')


def make_parser(parser):
    parser.add_argument('--flowcell', type=str, required=True,
                        help='FlowCell ID')
    parser.add_argument('--ignore_namespace', action='store_true',
                        help='ignore namespace when looking for the given FlowCell ID')
    parser.add_argument('--remove_namespaces', action='store_true',
                        help='remove namespaces from IDs in output file')
    parser.add_argument('--sample_label', action='store_true',
                       help='add the SampleLabel column to the output file')
    parser.add_argument('-s', '--separator', type=str, default=',',
                        help='delimiter character for the output file')

def implementation(logger, host, user, passwd, args):
    app = BuildFlowCellSamplesheetApp(host=host, user=user, passwd=passwd,
                                      keep_tokens=args.keep_tokens, logger=logger)
    app.dump(args.flowcell, args.ofile, args.separator,
             args.ignore_namespace, args.remove_namespaces,
             args.sample_label)

def do_register(registration_list):
    help_doc = 'Extract samplesheet for the given FlowCell'
    registration_list.append(('flowcell_samplesheet', help_doc,
                              make_parser, implementation))
