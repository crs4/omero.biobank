import csv, argparse, sys

from bl.vl.app.importer.core import Core

class MapToCollection(Core):

    def __init__(self, host = None, user = None, passwd = None, keep_tokens = 1,
                 logger = None, study_label = None, operator = 'Alfred E. Neumann'):
        super(MapToCollection, self).__init__(host, user, passwd,
                                              keep_tokens = keep_tokens,
                                              study_label = study_label,
                                              logger = logger)

    def load_collection(self, coll_type, coll_label):
        query = 'SELECT coll FROM %s coll WHERE coll.label  = :coll_label' % coll_type
        coll = self.kb.find_all_by_query(query, {'coll_label' : coll_label})
        return coll[0] if len(coll) > 0 else None

    def get_collection_items(self, collection, coll_type):
        if coll_type == 'VesselsCollection':
            citems = self.kb.get_vessels_collection_items(collection)
            ci_map = {}
            for ci in citems:
                # PlateWells are labelled using the pattern PLATE_LABEL:WELL_LABEL
                if ci.vessel.OME_TABLE == 'PlateWell':
                    ci_map['%s:%s' % (ci.vessel.container.label, ci.vessel.label)] = ci
                else:
                    ci_map[ci.vessel.label] = ci
        elif coll_type == 'DataCollection':
            citems = self.kb.get_data_collection_items(collection)
            ci_map = {}
            for ci in citems:
                ci_map[ci.dataSample.label] = ci
        else:
            raise ValueError('Unknown data collection type %s' % coll_type)
        return ci_map

    def dump(self, input_list, field_label, collection_label, collection_type, out_file):
        self.logger.info('Loading %s with label %s' % (collection_type,
                                                       collection_label))
        collection = self.load_collection(collection_type, collection_label)
        if collection is None:
            msg = 'No %s found with label %s' % (collection_type, collection_label)
            self.logger.error(msg)
            sys.exit(0)
        self.logger.info('Loaded collectio with ID %s' % collection.id)

        self.logger.info('Loading collection items')
        coll_items = self.get_collection_items(collection, collection_type)
        self.logger.info('Loaded %d items from collection' % len(coll_items))
        if len(coll_items) == 0:
            msg = 'No items fetched from the collection %s, nothing to do' % collection_label
            self.logger.warning(msg)
            sys.exit(0)
        try:
            with open(input_list) as infile:
                reader = csv.DictReader(infile, delimiter='\t')
                match_list = [row[field_label] for row in reader]
            self.logger.info('%s items are going to be mapped' % len(match_list))
        except KeyError, ke:
            msg = '%s is not a valid field label for file %s' % (ke, input_list)
            self.logger.critical(msg)
            sys.exit(msg)

        writer = csv.DictWriter(out_file, ['collection_label', 'item_id', 'item_type'],
                                delimiter='\t')
        writer.writeheader()
        for mi in match_list:
            try:
                record = {'collection_label' : collection_label,
                          'item_id' : coll_items[mi].id}
                if collection_type == 'DataCollection':
                    record['item_type'] = coll_items[mi].dataSample.OME_TABLE
                elif collection_type == 'VesselsCollection':
                    record['item_type'] = coll_items[mi].vessel.OME_TABLE
                writer.writerow(record)
                self.logger.debug('Done matching element %s' % mi)
            except KeyError, ke:
                msg = 'No match found for element %s' % ke
                self.logger.warning(msg)

        out_file.close()
        self.logger.info('Job completed')
        

help_doc = """
Check that the items in the input list belong to the specified
collection and map them to the collection item ID
"""

def make_parser(parser):
    parser.add_argument('-i', '--ifile', type=str, required=True,
                        help = 'list of the labels of the elements to be checked')
    parser.add_argument('--field_label', type=str, default='item_label',
                        help = 'label of the field (from input list) that will be mapped')
    parser.add_argument('--collection_type', type=str, required=True,
                        choices = ['DataCollection', 'VesselsCollection'],
                        help = 'type of the collection')
    parser.add_argument('--collection_label', type=str, required=True,
                        help = 'label of the collection')

def implementation(logger, host, user, passwd, args):
    app = MapToCollection(host = host, user = user, passwd = passwd,
                          keep_tokens = args.keep_tokens, logger = logger,
                          study_label = None)
    app.dump(args.ifile, args.field_label,
             args.collection_label, args.collection_type, args.ofile)

def do_register(registration_list):
    registration_list.append(('map_to_collection', help_doc, make_parser,
                              implementation))
