import csv, argparse, sys

from bl.vl.app.importer.core import Core

class RetrieveVesselsByIndividual(Core):

    VESSEL_TYPES = ['Vessel', 'Tube', 'PlateWell']

    def __init__(self, host = None, user = None, passwd = None,
                 keep_tokens = 1, logger = None, study_label = None,
                 operator = 'Alfred E. Neumann'):
        super(RetrieveVesselsByIndividual, self).__init__(host, user, passwd,
                                                          keep_tokens = keep_tokens,
                                                          study_label = study_label,
                                                          logger = logger)

    def get_inds_selection_list(self, ind_objs, ind_vids):
        sel_list = []
        inds_lookup = {}
        for ind in ind_objs:
            inds_lookup[ind.id] = ind
        for ivid in ind_vids:
            try:
                sel_list.append(inds_lookup[ivid])
            except KeyError, ke:
                self.logger.warning('ID %s is not a valid Individual ID' % ke)
        return sel_list

    def load_vcoll_filters(self, vessels_collection_label, vessel_type):
        vcoll = self.logger.info('Loading VesselsCollection %s' % \
                                     vessels_collection_label)
        if vcoll is None:
            msg = '%s is not a valid VesselsCollection label, aborting' % \
                vessels_collection_label
            self.logger.critical(msg)
            raise ValueError(msg)
        self.logger.info('Loading VesselsCollection items')
        vcitems = self.kb.get_vessels_collection_items(vcoll)
        if vessel_type:
            self.logger.info('Keep only item of type: %s' % vessel_type)
            vcitems = [vci for vci in vcitems
                       if type(vci.vessel) == getattr(self.kb, vessel_type)]
        return [vci.vessel.id for vci in vcitems]
                    
    def load_vessel_ids(self, vessels_collection, vessel_type):
        if vessels_collection:
            vessel_ids = self.load_vcoll_filters(vessels_collection, vessel_type)
        else:
            if vessel_type:
                vessel_ids = [v.id for v in self.kb.get_objects(getattr(self.kb, vessel_type))]
            else:
                vessel_ids = [v.id for v in self.kb.get_objects(self.kb.Vessel)]
        return vessel_ids

    def load_vessels_by_ind(self, individuals, vessel_ids, vessel_type):
        vessels = {}
        for ind in individuals:
            self.logger.debug('Fetching vessels for %s' % ind.id)
            if vessel_type:
                self.logger.debug('Fetching %s' % vessel_type)
                indv = list(self.kb.get_vessels_by_individual(ind, vessel_type))
            else:
                indv = []
                for vtype in self.VESSEL_TYPES:
                    self.logger.debug('Fetching %s' % vtype)
                    indv.extend(list(self.kb.get_vessels_by_individual(ind, vtype)))
            indv = [iv for iv in indv if iv.id in vessel_ids]
            self.logger.info('Fetched %d vessel(s) for %s' % (len(indv), ind.id))
            vessels[ind] = indv
        return vessels

    def dump(self, in_file, out_file, vessels_collection,
             vessel_type):
        self.logger.info('Loading individuals')
        inds = self.kb.get_objects(self.kb.Individual)
        self.logger.info('Loaded %d individuals' % len(inds))

        with open(in_file) as ifile:
            reader = csv.DictReader(ifile, delimiter='\t')
            ind_vids = [row['individual'] for row in reader]
        self.logger.info('Fetching data for %d individuals' % len(ind_vids))

        selection_list = self.get_inds_selection_list(inds, ind_vids)

        vessel_ids = self.load_vessel_ids(vessels_collection, vessel_type)
        if len(vessel_ids) == 0:
            self.logger.warning('No vessels will be selected, nothing to do')
            sys.exit(0)

        selected_vessels = self.load_vessels_by_ind(selection_list, vessel_ids,
                                                    vessel_type)

        writer = csv.DictWriter(out_file, ['individual', 'vessel_label',
                                           'vessel_type', 'vessel_status'],
                                delimiter='\t')
        writer.writeheader()
        for i, vs in selected_vessels.iteritems():
            for v in vs:
                record = {'individual'    : i.id,
                          'vessel_type'   : v.OME_TABLE,
                          'vessel_status' : v.status.enum_label()}
                if v.OME_TABLE == 'PlateWell':
                    record['vessel_label'] = '%s:%s' % (v.container.label, v.label)
                else:
                    record['vessel_label'] = v.label
                writer.writerow(record)

        out_file.close()
        self.logger.info('Job completed')


help_doc = """
Write a list of Vessels matching vessel_type parameter connected to
the individuals passed with the input_file. If a vessels_collection
has been specified, only vessels in the collection will be considered.
"""

def make_parser(parser):
    parser.add_argument('-i', '--ifile', type=str, required=True,
                        help='list of Individuals IDs used to fetch data')
    parser.add_argument('--vessels_collection', type=str,
                        help='label of the VesselsCollection that will be used as filter')
    parser.add_argument('--vessel_type', type=str,
                        choices = RetrieveVesselsByIndividual.VESSEL_TYPES,
                        help='type of the Vessels that will be fetched')

def implementation(logger, host, user, passwd, args):
    app = RetrieveVesselsByIndividual(host = host, user = user, passwd = passwd,
                                      keep_tokens = args.keep_tokens, logger = logger,
                                      study_label = None)
    app.dump(args.ifile, args.ofile, args.vessels_collection,
             args.vessel_type)
    
def do_register(registration_list):
    registration_list.append(('vessels_by_individual', help_doc, make_parser,
                              implementation))
