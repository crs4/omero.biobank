"""
This tool produce files to be used as in input to import
 * SequencerOutput data samples
 * SequencerOutput data objects
within OMERO.biobank using import apps.

All the configuration details for the script are into a YAML file
with the following structure:

 config_parameters:
   icollection: collection
   ome_host: hostname
   ome_user: username
   ome_passwd: password
   ome_study_label: study_label


"""
import argparse
import csv
import sys
import yaml

from bl.vl.kb import KnowledgeBase as KB
from bl.vl.utils import LOG_LEVELS, get_logger

def make_parser():
    parser = argparse.ArgumentParser(description="Prepare seqout import files")
    parser.add_argument('--logfile', type=str, help='log file (default=stderr)')
    parser.add_argument('--loglevel', type=str, help='logger level',
                        choices=LOG_LEVELS, default='INFO')
    parser.add_argument('--config_file', type=str, help='configuration file')
    return parser


def write_csv(logger, filename, csv_header, records):
    logger.info('Writing CSV file %s' % filename)
    with open(filename, 'w') as f:
        writer = csv.DictWriter(f, csv_header,
                                delimiter='\t', quotechar='"',
                                restval='None')
        writer.writeheader()
        for k, v in records.iteritems():
            writer.writerow(v)
    logger.info('Wrote {} records'.format(len(records)))
    return



def get_BGI_device(kb):
    BGI_device = kb.get_by_label(kb.Device, 'generic_BGI_scanner')
    return BGI_device.id

def get_flowcells(kb):
    return [x.label for x in kb.get_objects(kb.FlowCell)]


def generate_seqout(study_label, device_id, fcs_list):
    '''produce a SequencerOutput for each flowcell'''
    seqout_dict = {}
    for f in fcs_list:
        run_dir = '_'.join(['BGI_000', f])
        seqout_dict[f] = {'study': study_label,
                              'label': run_dir,
                              'source': f,
                              'source_type': 'FlowCell',
                              'seq_dsample_type': 'SequencerOutput',
                              'status': 'USABLE',
                              'device': device_id}
    seqout_header = ['study', 'label', 'source', 'source_type', 
                     'seq_dsample_type', 'status', 'device']
    return seqout_header, seqout_dict



def generate_dataobjects(study_label, fcs_list):
    dobjs_dict = {}
    for f in fcs_list:
        run_dir = '_'.join(['BGI_000', f])
        path = "file:///SHARE/USERFS/els7/users/sequencing_data/completed/{}/raw".format(run_dir)
        dobjs_dict[f] = {'study': study_label,
                         'path': path,
                         'data_sample': run_dir,
                         'mimetype': 'x-vl/pathset',
                         'size': '-1',
                         'sha1': 'N.A.'}
    dobjs_header = ['study', 'path', 'data_sample', 'mimetype', 'size', 
                    'sha1']
    return dobjs_header, dobjs_dict


def main(argv):
    parser = make_parser()
    args = parser.parse_args(argv)
    logger = get_logger('prepare_import_files', level=args.loglevel,
                        filename=args.logfile)

    required_parameters = ['ome_study_label', 'seqout_ofile']
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

    study_label = conf['config_parameters']['ome_study_label']

    # create SequencerOutput import file
    seqout_file = conf['config_parameters']['seqout_ofile']
    seqout_header, seqout_dict = generate_seqout(study_label,
                                                 get_BGI_device(kb),
                                                 get_flowcells(kb))
    write_csv(logger, seqout_file, seqout_header, seqout_dict)

    # create dataobjects import file
    dobjects_file = conf['config_parameters']['dobjects_ofile']
    dobjs_header, dobjs_dict = generate_dataobjects(study_label, 
                                                    get_flowcells(kb))
    write_csv(logger, dobjects_file, dobjs_header, dobjs_dict)




if __name__ == '__main__':
    main(sys.argv[1:])
