"""
Prepare all the files to create the omero objects to collect sequencing data
The objects chain is:
tube->laneslot->lane->flowcell->seqoutput->seqdatasample->dataobjects

Information are retrieved from Omero and iRODS
This script relies into PyRods library

Will be used a generic_BGI_scanner device.

All the configuration details for the script are into a YAML file
with the following structure:

 config_parameters:
   icollection: collection
   ome_host: hostname
   ome_user: username
   ome_passwd: password
   ome_study_label: study_label
   tubes_ofile: tubes_file
   flowcells_ofile: flowcells_file
   lanes_ofile: lanes_file
   laneslots_ofile: laneslots_file

"""

import argparse
import csv
import sys
import yaml

from bl.vl.kb import KnowledgeBase as KB
from bl.vl.utils import LOG_LEVELS, get_logger
from irods import *


def make_parser():
    parser = argparse.ArgumentParser(description='Prepare import files')
    parser.add_argument('--logfile', type=str, help='log file (default=stderr)')
    parser.add_argument('--loglevel', type=str, help='logger level',
                        choices=LOG_LEVELS, default='INFO')
    parser.add_argument('--config_file', type=str, help='configuration file')
    return parser


def get_inds_enrolled_into_a_study(kb, study):
    enrolls = kb.get_enrolled(kb.get_by_label(kb.Study, study))
    return [{'studyCode': x.studyCode, 'individual_id': x.individual.id}
            for x in enrolls]


def write_csv(logger, filename, csv_header, records_map):
    logger.info('Writing CSV file %s' % filename)    
    with open(filename, 'w') as f:
        writer = csv.DictWriter(f, csv_header,
                                delimiter='\t', quotechar='"',
                                restval='None')
        writer.writeheader()
        for k, v in records_map.iteritems():
            writer.writerow(v)
    logger.info('Wrote {} records'.format(len(records_map)))
    return


def get_last_id_tube_imported(kb):
    tubes = kb.get_objects(kb.Tube)
    highest_id = 0
    for t in tubes:
        _ = t.label.split('_')[2]
        if _ > highest_id:
            highest_id = _
    return highest_id


def get_next_tube_to_import(kb, study_label):
    tubes = kb.get_objects(kb.Tube)
    enrolls = kb.get_enrolled(kb.get_by_label(kb.Study, study_label))
    inds_and_tubes = {}
    for t in tubes:
        inds_and_tubes[t.action.target.id] = {'tube_label': t.label}
    return [{'studyCode': e.studyCode, 'individual_id': e.individual.id}
            for e in enrolls if e.individual.id not in inds_and_tubes]


def generate_tubes_map(study_label, inds_map, next_tube_id):
    prefix = "_".join([study_label, 'tube'])
    i = int(next_tube_id)
    tubes_map = {}
    for _  in inds_map:
        i += 1
        tubes_map[i] = {'study'         : study_label,
                        'label'         : "_".join([prefix, "{0:04d}".format(i)]),
                        'vessel_type'   : 'Tube',
                        'vessel_content': 'DNA',
                        'vessel_status' : 'UNKNOWN',
                        'source'        : _['individual_id'],
                        'source_type'   : 'Individual'}
    tubes_header = ['study', 'label', 'vessel_type', 'vessel_content',
                    'vessel_status', 'source', 'source_type']
    return tubes_header, tubes_map


def get_irods_metadata(logger, conn, collection):
    c = irodsCollection(conn)
    c.openCollection(collection)
    logger.info("Number of iRODS data objects: {}".format(c.getLenObjects()))
    metadata = []
    for dataObj in c.getObjects():
        md = {}
        data_name = dataObj[0]
        f = c.open(data_name, "r")
        for _ in f.getUserMetadata():
            md[_[0]] = _[1]
        metadata.append(md)
    return metadata


def generates_flowcells_map(study_label, metadata):
    flowcells_map = {}
    for _ in metadata:
        flowcells_map[_['fcid']] = {'study': study_label,
                                    'label': _['fcid'],
                                    'barcode': _['fcid'],
                                    'container_status':  'INSTOCK',
                                    'number_of_slots': 8,
                                    'options': 'operator={}'.format('BGI_operator')}
    flowcells_header = ['study', 'label', 'barcode', 'container_status',
                        'number_of_slots', 'options']
    return flowcells_header, flowcells_map


def generate_lanes_dict(study_label, metadata):
    i = ""
    lanes_dict = {}
    for _ in metadata:
        i = "_".join([ _['fcid'], _['lanes'] ])
        lanes_dict[i] = {'study': study_label,
                        'flow_cell': _['fcid'],
                        'slot': _['lanes'],
                        'container_status': 'INSTOCK',
                        'options': 'operator={}'.format('BGI_operator')}
    lanes_header = ['study','flow_cell', 'slot', 'container_status', 'options' ]
    return lanes_header, lanes_dict


def get_tube_label_from_studycode(kb, study_label):
    inds_and_tubes = {}
    enrolls = kb.get_enrolled(kb.get_by_label(kb.Study, study_label))
    tubes = kb.get_objects(kb.Tube)
    for e in enrolls:
        if e.individual.id not in inds_and_tubes:
            inds_and_tubes[e.individual.id] = {'studyCode': e.studyCode}
    for t in tubes:
        inds_and_tubes[t.action.target.id]['tube_label'] = t.label
    codes_dict = {}
    for k,v in inds_and_tubes.iteritems():
        codes_dict[v['studyCode']] = v['tube_label']
    return codes_dict


def generate_laneslots_dict(study_label, metadata, e_codes_map,
                            codes_dict, ext_map=False):
    i = ""
    laneslot_dict = {}
    for _ in metadata:
        if _['id'] in codes_dict:
            if ext_map:
                source = codes_dict[e_codes_map[_['id']]]
            else:
                source = codes_dict[_['id']]
            i = "_".join([_['fcid'], _['lanes'], _['barcode']])
            laneslot_dict[i] = {'study': study_label,
                                'lane': ":".join([_['fcid'], _['lanes']]),
                                'tag': _['barcode'],
                                'content': 'DNA',
                                'source': source,
                                'source_type': 'Tube',
                                'options': 'operator={}'.format('BGI_operator')}
    laneslot_header = ['study', 'lane', 'tag', 'content', 'source',
                       'source_type', 'options']
    return laneslot_header, laneslot_dict

def main(argv):
    parser = make_parser()
    args = parser.parse_args(argv)
    logger = get_logger('prepare_import_files', level=args.loglevel,
                        filename=args.logfile)

    required_parameters = ['ome_study_label', 'tubes_ofile']
    with open(args.config_file) as cfg:
        conf = yaml.load(cfg)
        for rp in required_parameters:
            if not (conf['config_parameters'].has_key(rp) and
                    conf['config_parameters'][rp] != None):
                raise RuntimeError("No {} provided".format(rp))

    ome_host = conf['config_parameters']['ome_host']
    ome_user = conf['config_parameters']['ome_user']
    ome_passwd = conf['config_parameters']['ome_passwd']
    kb = KB(driver='omero')(ome_host, ome_user, ome_passwd)

    status, myEnv = getRodsEnv()
    conn, errMsg = rcConnect(myEnv.rodsHost, myEnv.rodsPort,
                                   myEnv.rodsUserName, myEnv.rodsZone)
    status = clientLogin(conn)
    collection =  conf['config_parameters']['icollection']
    metadata = get_irods_metadata(logger, conn, collection)

    study_label = conf['config_parameters']['ome_study_label']

    # create tubes import files
    tubes_file =  conf['config_parameters']['tubes_ofile']
    tubes_next_id = get_last_id_tube_imported(kb)
    tubes_header, tubes_map = generate_tubes_map(study_label, get_next_tube_to_import(kb, study_label), tubes_next_id)
    write_csv(logger, tubes_file, tubes_header, tubes_map)

    #create flowcells import files
    flowcells_file = conf['config_parameters']['flowcells_ofile']
    flowcells_header, flowcells_map = generates_flowcells_map(study_label,
                                                              metadata)
    write_csv(logger, flowcells_file, flowcells_header, flowcells_map)

    # create lanes import files
    lanes_file = conf['config_parameters']['lanes_ofile']
    lanes_header, lanes_map = generate_lanes_dict(study_label, metadata)
    write_csv(logger, lanes_file, lanes_header, lanes_map)

    # create laneslot import file
    laneslots_file = conf['config_parameters']['laneslots_ofile']
    codes_map_file = '/home/gmauro/wip/OPBG/mappa'
    codes_map = {}
    with open(codes_map_file, 'r') as sfile:
        reader = csv.reader(sfile, delimiter='\t')
        for row in reader:
            codes_map[row[0]] = row[1]
    laneslots_header, laneslots_dict = generate_laneslots_dict(study_label,
                                                               metadata,
                                                               codes_map,
                                                               get_tube_label_from_studycode(kb, study_label))
    write_csv(logger, laneslots_file, laneslots_header, laneslots_dict)
   

if __name__ == '__main__':
    main(sys.argv[1:])
